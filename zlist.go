package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"log"
	"strings"
	"time"

	"ParseReestrLZP/cnfg"
	"ParseReestrLZP/dbase"
	"ParseReestrLZP/sqlScripts"
	"ParseReestrLZP/tempHandlers"
	"ParseReestrLZP/utils"

	// cthash "github.com/ClickHouse/clickhouse-go/v2/lib/cityhash102"
	"github.com/go-faster/city"
)

func handleZlist(data []byte, zf *dbase.ZFiles) error {
	tableSuff := strings.Replace(fmt.Sprintf("%v", zf.RId), "-", "_", -1)
	ctx := context.Background()
	// ctx, _ = context.WithTimeout(ctx, 120*time.Second)

	ch := *cnfg.Cnfg.CH
	chBatch, err := ch.PrepareBatch(ctx, fmt.Sprintf(sqlScripts.ZlistInsert, tableSuff))
	if err != nil {
		return err
	}
	defer chBatch.Close()

	pacs := make(ZPacs)
	hashes := make([]uint64, 0)
	zlist, err := dbase.ReadZlist(&data, zf)
	if err != nil {
		return err
	}
	for _, row := range zlist {
		// fmt.Println(i)
		dr := row.Dr.Format("2006-01-02")
		s := fmt.Sprintf("%s%s%s%s", strings.ToUpper(row.Fam), strings.ToUpper(row.Im), strings.ToUpper(row.Ot), dr)
		s = strings.Replace(s, "Ё", "Е", -1)

		// chash := cthash.CityHash64([]byte(s), uint32(len(s)))
		chash := city.CH64([]byte(s))
		// fmt.Println(chashF, chash)
		hashes = append(hashes, chash)
		pacs[chash] = OnePac{
			CaseId: row.CaseId,
			Fam:    row.Fam,
			Im:     row.Im,
			Ot:     row.Ot,
			Dr:     dr,
			Snilst: row.Snils,
		}
		err = chBatch.AppendStruct(&row)
		if err != nil {
			return err
		}

	}
	err_ := FormalCheck(&zlist, zf)
	if err != nil {
		err = errors.Join(err_, err)
		return err
	}
	err = CheckAllIn(zf, &hashes)
	if err != nil && err != cnfg.ErrNotInRegistr {
		return err
	} else {
		if err == cnfg.ErrNotInRegistr {
			CheckPacByOne(&pacs, zf)
		}
	}
	err = CheckPacPeriod(&pacs, zf)
	if err != nil {
		return err
	}
	err = chBatch.Send()
	if err != nil {
		return err
	}

	return nil
}

func CheckAllIn(zf *dbase.ZFiles, chs *[]uint64) error {
	// tableSuff := strings.Replace(fmt.Sprintf("%v", zf.RId), "-", "_", -1)
	ctx := context.Background()
	ctx, done := context.WithTimeout(ctx, time.Second*120)
	defer done()
	var result string
	// log.Println(sqlScripts.ChkAllIn)
	err := (*cnfg.Cnfg.CH).QueryRow(ctx, fmt.Sprintf(sqlScripts.ChkAllIn, cnfg.Cnfg.CDb), chs).Scan(&result)
	if err != nil {
		return err
	}
	if result == "Ok!" {
		return nil
	}
	if result == "No all in!" {
		return cnfg.ErrNotInRegistr
	}
	return nil
}

func CheckPacByOne(pacs *ZPacs, zf *dbase.ZFiles) error {
	ctx := context.Background()
	ctx, done := context.WithTimeout(ctx, time.Second*120)
	defer done()
	for k, v := range *pacs {
		// fmt.Printf("k:%v, v:%v\n", k, v)
		isOk := ""
		(*cnfg.Cnfg.CH).QueryRow(ctx, fmt.Sprintf(sqlScripts.ChkOneIn, cnfg.Cnfg.CDb), k).Scan(&isOk)
		if isOk == "Ok!" {
			continue
		}
		log.Println(v.Fam, v.Im, v.Ot, v.Dr, v.Snilst)
		var countBySnils uint64
		if len(v.Snilst) > 3 {
			err := (*cnfg.Cnfg.CH).QueryRow(ctx, fmt.Sprintf(sqlScripts.ChkOneInBySnils, cnfg.Cnfg.CDb, v.Snilst), k).Scan(&countBySnils)
			if err != nil {
				log.Println("QueryRow ChkOneInBySnils err:", err)
				return err
			}
			fmt.Println("Pacs to check pers data:", countBySnils)
			if countBySnils > 0 {
				err_ := utils.AddReestrError(zf.RId, v.CaseId, fmt.Sprintf("(000) Личные данные пациента [(%s) %s %s %s %v] не совпадаю с данными в регистре ЛП", v.Snilst, v.Fam, v.Im, v.Ot, v.Dr), 3, "zlist")
				if err_ != nil {
					log.Println("AddReestrError err:", err_)
				}
				continue
			}
		} else {

			messErr := fmt.Sprintf("(503) Пациент %s %s %s  %v отсутствует в регистре региональных льготников или не имеет льготы по ЛЗП", v.Fam, v.Im, v.Ot, v.Dr)
			utils.AddReestrError(zf.RId, v.CaseId, messErr, 2, "zlist")
		}
	}
	return nil
}

func CheckPacPeriod(pacs *ZPacs, zf *dbase.ZFiles) error {
	ctx := context.Background()
	ctx, done := context.WithTimeout(ctx, time.Second*120)
	ch := *cnfg.Cnfg.CH
	defer done()
	var err error
	for k, v := range *pacs {
		sqline := fmt.Sprintf("select date_u from %s.pac_date_usl where chash = ?", cnfg.Cnfg.CDb)
		var dateU time.Time

		err_ := ch.QueryRow(ctx, sqline, k).Scan(&dateU)
		if err_ != nil && err_ != sql.ErrNoRows {
			err = errors.Join(err, err_)
		}
		if err_ == sql.ErrNoRows {
			continue
		}
		if dateU.AddDate(3, 0, 0).Before(time.Now()) {
			continue
		}
		err = ChkVKR(zf, k, 1)
		if err != nil {
			err = errors.Join(err, err_)
			err = errors.Join(err, errors.New("(511 ZLIST) Возможно данные об оказанных услугах совпадают с данными услуг, принятыми ранее"))
			utils.AddReestrError(zf.RId, v.CaseId, err.Error(), 2, "zlist")
		}

	}
	return err
}

func ChkVKR(zf *dbase.ZFiles, k uint64, vtype int) error {

	var idList uint64
	ch := *cnfg.Cnfg.CH
	sqline := fmt.Sprintf("select id_list from %s.p_chash  where chash = ?", cnfg.Cnfg.CDb)
	ctx := context.Background()
	ctx, done := context.WithTimeout(ctx, time.Second*500)
	defer done()
	err := ch.QueryRow(ctx, sqline, k).Scan(&idList)
	if err != nil {
		return err
	}
	var vkrId int32
	chkVkrLine := `
	select vkr_id from vkr v 
	left join vkr_link vl on vl.vkr_id = v.vkr_id
	where vl.reestr_id = toUUIDOrZero('') and v.id_list = ? and v.type_id = ?
	`
	err = ch.QueryRow(ctx, chkVkrLine, idList, vtype).Scan(&vkrId)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == sql.ErrNoRows {
		return cnfg.ErrNoVKR
	}
	err = tempHandlers.CreateTempVkrLink(zf)
	if err != nil {
		return err
	}
	err = LinkVKR(zf, vkrId, idList)
	if err != nil {
		return err
	}
	return nil
}

func LinkVKR(zf *dbase.ZFiles, vkrId int32, idList uint64) error {
	tableSuff := strings.Replace(fmt.Sprintf("%v", zf.RId), "-", "_", -1)
	ctx := context.Background()
	ctx, done := context.WithTimeout(ctx, time.Second*120)
	defer done()
	ch := *cnfg.Cnfg.CH
	sqline := fmt.Sprintf(`
	insert into tmp_lzp.vkr_link_%s (reestr_id_ch, vkr_id) values (?,?)
	`, cnfg.Cnfg.CDb, tableSuff)
	err := ch.Exec(ctx, sqline, vkrId, idList)
	if err != nil {
		return err
	}
	return nil
}

func FormalCheck(zlist *[]dbase.Zlist, zf *dbase.ZFiles) error {

	var err error
	haveErrors := false
	for _, row := range *zlist {
		if row.Gender == "" {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле  W (пол пациента) (case_id:%v)", row.CaseId), 2, "zlist")
			// err = errors.Join(errors.New(fmt.Sprintf("Отстутвует поле W (пол пациента) (case_id:%v)", row.CaseId)), err)
			haveErrors = true
		}
		if row.CDoc == 0 {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле C_DOC (тип документа удостоверяющего личность) (case_id:%v)", row.CaseId), 2, "zlist")
			// err = errors.Join(errors.New("Отстутвует поле C_DOC"), err)
			haveErrors = true
		}
		if row.SDoc == "" {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле S_DOC (серия документа удостоверяющего личность) (case_id:%v)", row.CaseId), 2, "zlist")
			// err = errors.Join(errors.New("Отстутвует поле S_DOC"), err)
			haveErrors = true
		}
		if row.NDoc == "" {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле N_DOC (номер документа удостоверяющего личность) (case_id:%v)", row.CaseId), 2, "zlist")
			// err = errors.Join(errors.New("Отстутвует поле N_DOC"), err)
			haveErrors = true
		}
		if row.Vpolis == 0 {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле VPOLIS (тип полиса) (case_id:%v)", row.CaseId), 2, "zlist")
			// err = errors.Join(errors.New("Отстутвует поле VPOLIS"), err)
			haveErrors = true
		}
		if row.Number == "" {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле NUMBER (номер амбулаторной карты) (case_id:%v)", row.CaseId), 2, "zlist")
			// err = errors.Join(errors.New("Отстутвует поле NUMBER"), err)
			haveErrors = true
		}
		if row.NPol == "" {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле N_POL (номер полиса) (case_id:%v)", row.CaseId), 2, "zlist")
			// err = errors.Join(errors.New("Отстутвует поле N_POL"), err)
			haveErrors = true
		}
		// if !cnfg.Cnfg.DisCheck["3"] {
		// 	if row.SPol == "" {
		// 		err = errors.Join(errors.New("Отстутвует поле S_POL"), err)
		// 		AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001) Отсутствует значение в поле S_POL (номер полиса) (case_id:%v)", row.CaseId), 3)
		// 	}
		// }
		if !slices.Contains(cnfg.Cnfg.DisCheckS, 3) {
			if row.SPol == "" {
				utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле S_POL (номер полиса) (case_id:%v)", row.CaseId), 2, "zlist")
				// err = errors.Join(errors.New("Отстутвует поле S_POL"), err)
				haveErrors = true
			}
		}
		if row.Fam == "" {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле FAM (Фамилия) (case_id:%v)", row.CaseId), 2, "zlist")
			// err = errors.Join(errors.New("Отстутвует поле FAM"), err)
			haveErrors = true
		}
		if row.Im == "" {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле IM (Имя) (case_d:%v)", row.CaseId), 2, "zlist")
			// err = errors.Join(errors.New("Отстутвует поле IM"), err)

		}
		if row.Dr.Year() < 1900 {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(003 ZLIST) Нарушено условие заполнения поля DR (Дата Рождения) (case_id:%v)", row.CaseId), 2, "zlist")
			// err = errors.Join(errors.New("Ошибка в поле DR"), err)
			haveErrors = true
		}
		// if row.Kont == 0 {
		//
		// 	err = errors.Join(errors.New("Ошибка в поле KONT"), err)
		// 	log.Println(row.Fam, row.CaseId, row.Kont)
		// 	AddReestrError(zf.RId, row.CaseId, "(001) отсутствие поля KONT (социальный статус)", 2)
		// }
		if row.Idmu == 0 {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(003 ZLIST) Нарушено условие заполнения поля IDMU (case_id:%v, idmu:%v)", row.CaseId, row.Idmu), 2, "zlist")
			// err = errors.Join(errors.New("Ошибка в поле IDMU"), err)
			haveErrors = true
		}
		if row.PCity == "" {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле P_CITY (case_id:%v)", row.CaseId, row.Idmu), 2, "zlist")
			// err = errors.Join(errors.New("Ошибка в поле IDMU"), err)
			haveErrors = true

		}
		if row.PRgn == 0 {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле P_RGN (case_id:%v)", row.CaseId), 2, "zlist")
			// err = errors.Join(errors.New("Ошибка в поле P_RGN"), err)
			haveErrors = true
		}
		if row.FRgn == 0 {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле F_RGN (case_id:%v)", row.CaseId), 2, "zlist")
			// err = errors.Join(errors.New("Ошибка в поле F_RGN"), err)
			haveErrors = true
		}
		if row.CodeP == 0 {
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001 ZLIST) Отсутствует значение в поле CODE_P (код СМО) (case_id:%v)", row.CaseId), 2, "zlist")
			// err = errors.Join(errors.New("Ошибка в поле CODE_P"), err)
			haveErrors = true
		}
		if row.IdCity == 0 {
			var crit uint8
			crit = 2
			if slices.Contains(cnfg.Cnfg.DisCheckS, 3) {
				crit = 3
			}
			utils.AddReestrError(zf.RId, row.CaseId, fmt.Sprintf("(001) Отсутствует значение в поле ID_CITY (код населенного пункта) (case_id:%v)", row.CaseId), crit, "zlist")
			if crit == 2 {
				haveErrors = true
			}

		}

	}
	if haveErrors {
		err = errors.New("Критические ошибки формального контроля")
	}
	return err
}
