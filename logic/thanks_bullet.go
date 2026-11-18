package logic

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xbclub/BilibiliDanmuRobot-Core/entity"
	"github.com/xbclub/BilibiliDanmuRobot-Core/svc"
	"github.com/zeromicro/go-zero/core/logx"
)

// 检测到礼物，push [uname]->[giftName]->[cost]，number+1
// 每3s统计一次礼物，并进行感谢，礼物价值高于x元加一句大气

var thanksGiver *GiftThanksGiver

type GiftThanksGiver struct {
	giftNameUidTable     map[string]int
	giftNotBlindBoxTable map[string]map[string]map[string]int
	giftBlindBoxTable    map[string]map[string]map[string]int
	giftBlindBoxTimer    map[int]*time.Timer
	locked               *sync.Mutex
	tableMu              sync.RWMutex
	giftChan             chan *entity.SendGiftText
}

func PushToGiftChan(g *entity.SendGiftText) {
	thanksGiver.giftChan <- g
}

func PushToGuardChan(svcCtx *svc.ServiceContext, g *entity.GuardBuyText, reply ...*entity.DanmuMsgTextReplyInfo) {
	user, gift := g.Data.Username, g.Data.GiftName
	if len(svcCtx.Config.ThanksGuardDanmu) > 0 {
		tpl := svcCtx.Config.ThanksGuardDanmu[rand.Intn(len(svcCtx.Config.ThanksGuardDanmu))]
		text := renderThanks(tpl, user, gift, 1, 0)
		if reply != nil {
			PushToBulletSender(text, reply...)
		} else {
			PushToBulletSender(text)
		}
		return
	}
	if reply != nil {
		msg := "感谢" + gift
		PushToBulletSender(msg, reply...)
	} else {
		msg := "感谢 " + user + " 的 " + gift
		PushToBulletSender(msg)
	}
}

func PushThanksBlindBox(svcCtx *svc.ServiceContext, user, gift string) {
	if len(svcCtx.Config.ThanksBlindBoxDanmu) > 0 {
		tpl := svcCtx.Config.ThanksBlindBoxDanmu[rand.Intn(len(svcCtx.Config.ThanksBlindBoxDanmu))]
		PushToBulletSender(renderThanks(tpl, user, gift, 1, 0))
		return
	}
	PushToBulletSender("感谢 " + user + " 的 " + gift)
}

func ThanksGift(ctx context.Context, svcCtx *svc.ServiceContext) {

	thanksGiver = &GiftThanksGiver{
		giftNameUidTable:     make(map[string]int),
		giftNotBlindBoxTable: make(map[string]map[string]map[string]int),
		giftBlindBoxTable:    make(map[string]map[string]map[string]int),
		giftBlindBoxTimer:    make(map[int]*time.Timer),
		locked:               new(sync.Mutex),
		tableMu:              sync.RWMutex{},
		giftChan:             make(chan *entity.SendGiftText, 1000),
	}

	var g *entity.SendGiftText
	var w = time.Duration(svcCtx.Config.ThanksGiftTimeout) * time.Second
	var t = time.NewTimer(w)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			goto END
		case <-t.C:
			thanksGiver.locked.Lock()
			summarizeGift(svcCtx.Config.DanmuLen, svcCtx.Config.ThanksMinCost, svcCtx)
			thanksGiver.locked.Unlock()
			t.Reset(w)
		case g = <-thanksGiver.giftChan:
			thanksGiver.locked.Lock()

			if svcCtx.Config.ThanksGiftUseAt {
				thanksGiver.giftNameUidTable[g.Data.Uname] = g.Data.UID
			}

			giftName := g.Data.GiftName
			if g.Data.BlindGift.OriginalGiftName != "" {
				giftName = giftName + "(" + strings.ReplaceAll(g.Data.BlindGift.OriginalGiftName, "盲盒", "") + ")"
			}
			if _, ok := thanksGiver.giftNotBlindBoxTable[g.Data.Uname]; !ok {
				thanksGiver.giftNotBlindBoxTable[g.Data.Uname] = make(map[string]map[string]int)
			}
			if _, ok := thanksGiver.giftNotBlindBoxTable[g.Data.Uname][giftName]; !ok {
				thanksGiver.giftNotBlindBoxTable[g.Data.Uname][giftName] = make(map[string]int)
			}
			thanksGiver.giftNotBlindBoxTable[g.Data.Uname][giftName]["cost"] += g.Data.Price
			thanksGiver.giftNotBlindBoxTable[g.Data.Uname][giftName]["count"] += g.Data.Num

			t.Reset(w)

			if svcCtx.Config.BlindBoxProfitLossStat && g.Data.BlindGift.OriginalGiftName != "" {
				//fmt.Printf("盲盒: ")
				if t, ok := thanksGiver.giftBlindBoxTimer[g.Data.UID]; !ok || t == nil {
					thanksGiver.giftBlindBoxTimer[g.Data.UID] = time.NewTimer(time.Duration(svcCtx.Config.ThanksGiftTimeout) * time.Second)
					go func(t *time.Timer) {
						for {
							<-t.C
							thanksGiver.locked.Lock()
							summarizeBlindGift(svcCtx.Config.DanmuLen, svcCtx)
							thanksGiver.locked.Unlock()
							t.Stop()
							thanksGiver.giftBlindBoxTimer[g.Data.UID] = nil
						}
					}(thanksGiver.giftBlindBoxTimer[g.Data.UID])
				}

				if thanksGiver.giftBlindBoxTimer[g.Data.UID] != nil {
					thanksGiver.giftBlindBoxTimer[g.Data.UID].Reset(time.Duration(svcCtx.Config.ThanksGiftTimeout) * time.Second)
				}

				if _, ok := thanksGiver.giftBlindBoxTable[g.Data.Uname]; !ok {
					thanksGiver.giftBlindBoxTable[g.Data.Uname] = make(map[string]map[string]int)
				}
				if _, ok := thanksGiver.giftBlindBoxTable[g.Data.Uname][g.Data.BlindGift.OriginalGiftName]; !ok {
					thanksGiver.giftBlindBoxTable[g.Data.Uname][g.Data.BlindGift.OriginalGiftName] = make(map[string]int)
				}
				thanksGiver.giftBlindBoxTable[g.Data.Uname][g.Data.BlindGift.OriginalGiftName]["count"] += g.Data.Num
				thanksGiver.giftBlindBoxTable[g.Data.Uname][g.Data.BlindGift.OriginalGiftName]["profit_and_loss"] += (g.Data.Price - g.Data.BlindGift.OriginalGiftPrice) * g.Data.Num
			}
			thanksGiver.locked.Unlock()
		}
	}
END:
}

func renderThanks(tpl, user, gift string, num, cost int) string {
	return strings.NewReplacer(
		"{user}", user,
		"{gift}", gift,
		"{num}", strconv.Itoa(num),
		"{cost}", fmt.Sprintf("%.2f", float64(cost)/1000),
	).Replace(tpl)
}

func summarizeBlindGift(danmuLen int, svcCtx *svc.ServiceContext) {
	// 盲盒礼物
	for name, m := range thanksGiver.giftBlindBoxTable {
		giftstring := []string{}
		msg := ""
		sumCount := 0
		for blindBoxName, blindBoxMap := range m {
			//giftstring = append(giftstring, fmt.Sprintf("%d个%s盈亏%+.2f元", blindBoxMap["count"], blindBoxName, float64(blindBoxMap["profit_and_loss"])/1000))
			if blindBoxMap["profit_and_loss"] > 0 {
				giftstring = append(giftstring, fmt.Sprintf("%d个%s赚了＋%.2f元", blindBoxMap["count"], blindBoxName, float64(blindBoxMap["profit_and_loss"])/1000))
			} else {
				giftstring = append(giftstring, fmt.Sprintf("%d个%s亏了－%.2f元", blindBoxMap["count"], blindBoxName, math.Abs(float64(blindBoxMap["profit_and_loss"])/1000)))
			}
			sumCount += blindBoxMap["count"]
			// 感谢完后立刻清空map
			delete(m, blindBoxName)
		}

		msgShort := ""

		if !svcCtx.Config.ThanksGiftUseAt {
			msg = name + "的"
		}
		for k, v := range giftstring {
			if k == 0 {
				msg += v
				msgShort = v
			} else {
				msg += "，" + v
				msgShort += "，" + v
			}
		}

		giftText := strings.Join(giftstring, "，")
		if len(svcCtx.Config.ThanksBlindBoxDanmu) > 0 {
			tpl := svcCtx.Config.ThanksBlindBoxDanmu[rand.Intn(len(svcCtx.Config.ThanksBlindBoxDanmu))]
			text := renderThanks(tpl, name, giftText, sumCount, 0)
			if svcCtx.Config.ThanksGiftUseAt {
				PushToBulletSender(text, &entity.DanmuMsgTextReplyInfo{
					ReplyUid: strconv.Itoa(thanksGiver.giftNameUidTable[name]),
				})
			} else {
				PushToBulletSender(text)
			}
			delete(thanksGiver.giftBlindBoxTable, name)
			continue
		}

		ms := []rune(msg)

		if len(ms) > danmuLen {
			if !svcCtx.Config.ThanksGiftUseAt {
				PushToBulletSender(name + "的")
				PushToBulletSender(msgShort)
			} else {
				PushToBulletSender(msgShort, &entity.DanmuMsgTextReplyInfo{
					ReplyUid:   strconv.Itoa(thanksGiver.giftNameUidTable[name]),
					ReplyMsgId: "",
				})
			}
		} else {
			if !svcCtx.Config.ThanksGiftUseAt {
				PushToBulletSender(msg)
			} else {
				PushToBulletSender(msg, &entity.DanmuMsgTextReplyInfo{
					ReplyUid:   strconv.Itoa(thanksGiver.giftNameUidTable[name]),
					ReplyMsgId: "",
				})
			}
		}
		delete(thanksGiver.giftBlindBoxTable, name)
	}
}

func summarizeGift(danmuLen int, minCost int, svcCtx *svc.ServiceContext) {
	for name, m := range thanksGiver.giftNotBlindBoxTable {
		sumCost := 0
		sumCount := 0
		giftstring := []string{}
		msg := ""
		for gift, cost := range m {
			giftstring = append(giftstring, fmt.Sprintf("%d个%s", cost["count"], gift))
			// 计算打赏金额
			sumCost += cost["cost"]
			sumCount += cost["count"]

			// 感谢完后立刻清空map
			delete(m, gift)
		}

		msgShort := ""

		if !svcCtx.Config.ThanksGiftUseAt {
			msg = "感谢" + name + "的"
		} else {
			msg = "感谢"
		}
		for k, v := range giftstring {
			if k == 0 {
				msg += v
				msgShort = v
			} else {
				msg += "，" + v
				msgShort += "，" + v
			}
		}

		giftText := strings.Join(giftstring, "，")
		if sumCost < minCost {
			logx.Infof("礼物感谢跳过: uname=%s gifts=%v cost=%d minCost=%d", name, giftstring, sumCost, minCost)
			delete(thanksGiver.giftNotBlindBoxTable, name)
			continue
		}
		if len(svcCtx.Config.ThanksGiftDanmu) > 0 {
			tpl := svcCtx.Config.ThanksGiftDanmu[rand.Intn(len(svcCtx.Config.ThanksGiftDanmu))]
			text := renderThanks(tpl, name, giftText, sumCount, sumCost)
			if svcCtx.Config.ThanksGiftUseAt {
				PushToBulletSender(text, &entity.DanmuMsgTextReplyInfo{
					ReplyUid: strconv.Itoa(thanksGiver.giftNameUidTable[name]),
				})
			} else {
				PushToBulletSender(text)
			}
			if sumCost >= 50000 {
				PushToBulletSender(name + "老板大气大气")
			}
			delete(thanksGiver.giftNotBlindBoxTable, name)
			continue
		}

		ms := []rune(msg)
		if len(ms) > danmuLen {
			if !svcCtx.Config.ThanksGiftUseAt {
				PushToBulletSender("感谢 " + name + " 的")
				PushToBulletSender(msgShort)
			} else {
				PushToBulletSender(msgShort, &entity.DanmuMsgTextReplyInfo{
					ReplyUid:   strconv.Itoa(thanksGiver.giftNameUidTable[name]),
					ReplyMsgId: "",
				})
			}
		} else {
			if !svcCtx.Config.ThanksGiftUseAt {
				PushToBulletSender(msg)
			} else {
				PushToBulletSender(msg, &entity.DanmuMsgTextReplyInfo{
					ReplyUid:   strconv.Itoa(thanksGiver.giftNameUidTable[name]),
					ReplyMsgId: "",
				})
			}
		}

		//fmt.Println("礼物-----", name, giftstring)
		// 总打赏高于x元，加一句大气
		if sumCost >= 50000 { // 50元
			PushToBulletSender(name + "老板大气大气")
		}
		delete(thanksGiver.giftNotBlindBoxTable, name)
	}
}
