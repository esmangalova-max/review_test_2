package handlers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"

	"ParseReestrLZP/cnfg"
	"ParseReestrLZP/dbase"
	"ParseReestrLZP/sqlScripts"
	"ParseReestrLZP/utils"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func handleZlistol(data []byte, zf *dbase.ZFiles) error {
	tableSuff := strings.Replace(fmt.Sprintf("%v", zf.RId), "-", "_", -1)
	ctx := context.Background()
	// ctx, done := context.WithTimeout(ctx, 300*time.Second)
	// defer done()
	avaPref := []string{
		"401", "402", "404", "407",
	}

	ch := *cnfg.Cnfg.CH
	chBatch, err := ch.PrepareBatch(ctx, fmt.Sprintf(sqlScripts.ZlistolInsert, tableSuff))
	if err != nil {
		return err
	}
	defer chBatch.Close()
	zlistol, err := dbase.ReadZlistol(&data, zf)
	if err != nil {
		return err
	}

	noPref := make(map[uuid.UUID]struct{})
	for _, row := range zlistol {
		pref := slices.Contains(avaPref, row.CKat)
		if !pref {
			noPref[row.Id] = struct{}{}

			// err = errors.Join(err, (*cnfg.Cnfg.CH).Exec(ctx, fmt.Sprintf(sqlScripts.InsertError, tableSuff), row.ReestrId, row.CaseId, "(003) Нарушено условие заполнения поля C_KAT", 2))
			err = errors.Join(err, utils.AddReestrError(row.ReestrId, row.CaseId, fmt.Sprintf("(003 ZLISTOL) Нарушено условие заполнения поля C_KAT (case_id:%v, c_kat:%v)", row.CaseId, row.CKat), 2, "zlistol"))
		}
		ok := slices.Contains(cnfg.Cnfg.CodeUsl, row.Codusl)
		var crit uint8 // критичность

		if !ok {

			if slices.Contains(cnfg.Cnfg.DisCheckS, 1) {
				crit = 3
			} else {
				crit = 2
			}
			err = errors.Join(err, utils.AddReestrError(row.ReestrId, row.CaseId, fmt.Sprintf("(002 ZLISTOL) Значение поля CODUSL (Код услуги) не соответсвует списку (case_id:%v, codusl:%v)", row.CaseId, row.Codusl), crit, "zlistol"))
		}
		if row.Mkb == "" {
			err = errors.Join(err, utils.AddReestrError(row.ReestrId, row.CaseId, fmt.Sprintf("(001 ZLISTOL) Отсутвует код МКБ диагноза (case_id:%v)", row.CaseId), 2, "zlistol"))
		}
		k, _ := row.Kol.Float64()
		if k == 0 {
			err = errors.Join(err, utils.AddReestrError(row.ReestrId, row.CaseId, fmt.Sprintf("(001 ZLISTOL) Отсутвует поле KOL (кол-во услуг) (case_id:%v)", row.CaseId), 2, "zlistol"))
		}
		if row.Podr == 0 {
			err = errors.Join(err, utils.AddReestrError(row.ReestrId, row.CaseId, fmt.Sprintf("(001 ZLISTOL) Отсутвует поле PODR (код подразделения оказавшего услугу) (case_id:%v)", row.CaseId), 2, "zlistol"))
		}
		if row.DateU.Before(zf.Mt.AddDate(0, -3, 0)) {
			err = errors.Join(err, utils.AddReestrError(row.ReestrId, row.CaseId, fmt.Sprintf("(001 ZLISTOL) Ошибка в поле DATE_U (дата услуги) (case_id:%v, date_u:%v)", row.CaseId, row.DateU), 2, "zlistol"))
		}
		err = chBatch.AppendStruct(&row)
		if err != nil {
			return err
		}

	}
	if len(noPref) > 0 {
		log.Println("noPref:", noPref)
	}
	err = chk513(&ctx, tableSuff)
	if err != nil {
		err = errors.Join()
	}
	err = CompareZZol(zf)
	if err != nil {
		err = errors.Join()
	}
	err = chBatch.Send()
	if err != nil {
		return err
	}
	return nil
}

func chk513(ctx *context.Context, tsuff string) error {
	found := false
	rc, err := (*cnfg.Cnfg.CH).Query(*ctx, fmt.Sprintf(sqlScripts.Chk513, tsuff))
	if err != nil {
		return err
	}
	defer rc.Close()
	for rc.Next() {
		found = true
		// var caseId uint64
		var caseId uint8
		var s, u, m decimal.Decimal
		var ree uuid.UUID
		if err = rc.Scan(&s, &u, &m, &ree, &caseId); err != nil {
			log.Println(err)
		}
		err = errors.Join(err, utils.AddReestrError(ree, uint64(caseId), fmt.Sprintf("(513 ZLISTOL) Превышено количество слепков (case_id:%v)", caseId), 2, "zlistol"))

	}
	rcAB, err := (*cnfg.Cnfg.CH).Query(*ctx, fmt.Sprintf(sqlScripts.Check513AB, tsuff))
	if err != nil {
		return err
	}
	defer rcAB.Close()
	for rcAB.Next() {
		found = true
		// var caseId uint64
		var caseId uint8
		var ka71, kb71 decimal.Decimal
		var ree uuid.UUID
		if err = rcAB.Scan(&ka71, &kb71, &ree, &caseId); err != nil {
			log.Println(err)
		}
		err = errors.Join(err, utils.AddReestrError(ree, uint64(caseId), fmt.Sprintf("(513 ZLISTOL) Превышено количество слепков (case_id:%v)", caseId), 2, "zlistol"))

	}

	if !found {
		return nil
	}

	return errors.Join(errors.New("(513 ZLISTOL) Превышено максимальное число слепков"), err)
}
