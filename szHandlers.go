package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/LindsayBradford/go-dbf/godbf"
	"github.com/go-faster/errors"
	"github.com/google/uuid"

	"ParseReestrLZP/cnfg"
	"ParseReestrLZP/dbase"
	"ParseReestrLZP/sqlScripts"
	"ParseReestrLZP/unarch"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/schollz/progressbar/v3"
)

func HandleSz(arcname string, zf *dbase.ZFiles) error {
	data, err := unarch.Decopress(arcname, zf)
	if err != nil {
		return errors.Errorf("unarch.UnarcSZ(%q): %w", arcname, err)
	}
	szlData := (*data)["sz_l.dbf"]
	err = SaveSzL(&szlData, zf)
	if err != nil {
		return errors.Errorf("SaveSzL(%q): %w", arcname, err)
	}
	szpData := (*data)["sz_p.dbf"]
	err = SaveSzP(&szpData, zf)
	if err != nil {
		return errors.Errorf("SaveSzP(%q): %w", arcname, err)
	}
	err = MoveSZToArchWOTran()
	if err != nil {
		return errors.Errorf("MoveSZToArch(%q): %w", arcname, err)
	}
	err = TmpToCurrent()
	if err != nil {
		return errors.Errorf("TmpToCurrent(): %w", err)
	}

	err = RefreshPersonList()
	if err != nil {
		return errors.Errorf("RefreshPersonList(): %w", err)
	}

	err = AddFilesRecord(zf, 0)
	if err != nil {
		return errors.Errorf("AddFilesRecord(%q): %w", arcname, err)
	}
	return nil
}

func MoveSZToArchWOTran() error {
	sqlSelectRegId := fmt.Sprintf("select distinct registr_id from  %s.szl_curent order by  UUIDv7ToDateTime(registr_id) desc", cnfg.Cnfg.CDb)
	chd := *cnfg.Cnfg.CH
	var regId uuid.UUID
	ctx := context.Background()
	err := chd.QueryRow(ctx, sqlSelectRegId).Scan(&regId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return errors.Errorf("QueryRow failed: %w", err)
	}

	sqlChkExists := fmt.Sprintf(`select exists(select toUInt8(1) from %s.szl_arch where registr_id = toUUID('%v')) ex`, cnfg.Cnfg.CDb, regId)
	var regExistsInArch uint8
	err = chd.QueryRow(ctx, sqlChkExists).Scan(&regExistsInArch)
	if err != nil {
		return errors.Errorf("QueryRow (check exists registr_id) failed: %w", err)
	}
	if regExistsInArch == 1 {
		fmt.Println("Already moved to arch")
		return nil
	}

	err = chd.Exec(ctx, fmt.Sprintf(sqlScripts.CopySZLToArch, cnfg.Cnfg.CDb, cnfg.Cnfg.CDb))

	if err != nil {
		return errors.Errorf("CopySZLToArch : %w", err)
	}
	err = chd.Exec(ctx, fmt.Sprintf(sqlScripts.CopySZPToArch, cnfg.Cnfg.CDb, cnfg.Cnfg.CDb))
	if err != nil {
		return errors.Errorf("CopySZPToArch : %w", err)
	}
	err = chd.Exec(ctx, fmt.Sprintf(sqlScripts.TruncateSZL, cnfg.Cnfg.CDb))
	if err != nil {
		return errors.Errorf("TruncateSZL : %w", err)
	}
	err = chd.Exec(ctx, fmt.Sprintf(sqlScripts.TruncateSZP, cnfg.Cnfg.CDb))
	if err != nil {
		return errors.Errorf("TruncateSZP : %w", err)
	}

	return nil
}

func MoveSZToArch() error {
	ch, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%s@%s:%s?database=%s", cnfg.Cnfg.CUser, cnfg.Cnfg.CPass, cnfg.Cnfg.CHost, cnfg.Cnfg.CPort, cnfg.Cnfg.CDb))
	if err != nil {
		return errors.Errorf("Clickhouse driver failed: %w", err)
	}

	defer ch.Close()
	err = ch.Ping()
	if err != nil {
		return errors.Errorf("Ping failed: %w", err)
	}
	ch.SetConnMaxIdleTime(time.Minute * 5) // Should be less than server's idle timeout
	ch.SetMaxIdleConns(5)
	ch.SetMaxOpenConns(10)
	// fmt.Printf("%#v", ch)
	// check exists register
	sqlSelectRegId := fmt.Sprintf("select distinct registr_id from  %s.szl_curent order by  UUIDv7ToDateTime(registr_id) desc", cnfg.Cnfg.CDb)
	chd := *cnfg.Cnfg.CH
	var regId uuid.UUID
	ctx := context.Background()
	err = chd.QueryRow(ctx, sqlSelectRegId).Scan(&regId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return errors.Errorf("QueryRow failed: %w", err)
	}

	sqlChkExists := fmt.Sprintf(`select exists(select toUInt8(1) from %s.szl_arch where registr_id = toUUID('%v')) ex`, cnfg.Cnfg.CDb, regId)
	var regExistsInArch uint8
	err = chd.QueryRow(ctx, sqlChkExists).Scan(&regExistsInArch)
	if err != nil {
		return errors.Errorf("QueryRow (check exists registr_id) failed: %w", err)
	}
	if regExistsInArch == 1 {
		fmt.Println("Already moved to arch")
		return nil
	}
	tx, err := ch.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	// 2. Prepare and execute statements within the transaction
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(sqlScripts.CopySZLToArch, cnfg.Cnfg.CDb, cnfg.Cnfg.CDb))
	if err != nil {
		return errors.Errorf("CopySZLToArch prepare context: %w", err)

	}

	if _, err := stmt.ExecContext(ctx); err != nil {
		return errors.Errorf("CopySZLToArch : %w", err)
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	stmt, err = tx.PrepareContext(ctx, fmt.Sprintf(sqlScripts.CopySZPToArch, cnfg.Cnfg.CDb, cnfg.Cnfg.CDb))
	if err != nil {
	}
	if _, err := stmt.ExecContext(ctx); err != nil {
		return errors.Errorf("CopySZPToArch : %w", err)
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	stmt, err = tx.PrepareContext(ctx, fmt.Sprintf(sqlScripts.TruncateSZL, cnfg.Cnfg.CDb))
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(ctx); err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	stmt, err = tx.PrepareContext(ctx, fmt.Sprintf(sqlScripts.TruncateSZP, cnfg.Cnfg.CDb))
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(ctx); err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}

	// 3. Commit the transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func SaveSzL(dbfdata *[]byte, zf *dbase.ZFiles) error {
	chcon := *cnfg.Cnfg.CH
	sqlDropTemp := fmt.Sprintf(`DROP TABLE IF EXISTS %s.szl`, "tmp_lzp")
	sqlCrateTemp := fmt.Sprintf("%s %s", fmt.Sprintf(sqlScripts.NewSZL, "tmp_lzp"), "engine = Memory")
	sqlInsert := fmt.Sprintf(sqlScripts.InserSZL, "tmp_lzp")
	dbf, err := godbf.NewFromByteArray(*dbfdata, "cp866")
	if err != nil {
		return err
	}
	ctx := context.Background()
	err = chcon.Exec(ctx, sqlDropTemp)
	if err != nil {
		return errors.Errorf("Error drop temp szl: %v", err)
	}
	err = chcon.Exec(ctx, sqlCrateTemp)
	if err != nil {
		return errors.Errorf("Error create temp szl: %v", err)
	}

	chBatch, err := chcon.PrepareBatch(ctx, sqlInsert)
	if err != nil {
		log.Fatal(err)
	}
	// fmt.Println("dbf.NumberOfRecords():", dbf.NumberOfRecords())
	// fmt.Println("SaveSzL:")
	bar := progressbar.Default(int64(dbf.NumberOfRecords()), "SaveSzL:")
	step := cnfg.Cnfg.SzBatchSize
	for i := range dbf.NumberOfRecords() {

		if i == 0 {

			ctx1, done := context.WithTimeout(ctx, 10*time.Second)
			defer done()
			chBatch, err = chcon.PrepareBatch(ctx1, sqlInsert)
			if err != nil {
				done()
			}

		}

		if i%step == 0 && i != 0 {
			err = chBatch.Send()
			if err != nil {
				log.Fatal(err)
			}
			chBatch.Close()
			ctx1, done := context.WithTimeout(ctx, 10*time.Second)
			defer done()
			chBatch, err = chcon.PrepareBatch(ctx1, sqlInsert)
		}
		if i%step == 0 && i != 0 {
			// fmt.Println(i, "SZL batch send")
			bar.Add(step)
		}
		var row dbase.SzL
		row.Id, _ = uuid.NewV7()
		row.RId = zf.RId
		IdlistStr, _ := dbf.FieldValueByName(i, "ID_LIST")
		row.Idlist, _ = strconv.ParseUint(IdlistStr, 10, 64)
		row.Ss, _ = dbf.FieldValueByName(i, "SS")
		row.CKat, _ = dbf.FieldValueByName(i, "C_KAT")
		row.KatF, _ = dbf.FieldValueByName(i, "KAT_F")
		row.NameDL, _ = dbf.FieldValueByName(i, "NAME_DL")
		row.SnDl, _ = dbf.FieldValueByName(i, "SN_DL")
		dbl, _ := dbf.FieldValueByName(i, "DATE_BL")
		if dbl == "" {
			row.DateBl = nil
		} else {
			tmp, _ := time.Parse("20060102", dbl)
			row.DateBl = &tmp
		}
		ddel, _ := dbf.FieldValueByName(i, "DATE_EL")
		if ddel == "" {
			row.DateEl = nil
		} else {
			tmp, _ := time.Parse("20060102", ddel)
			row.DateEl = &tmp
		}
		row.Comment, _ = dbf.FieldValueByName(i, "COMENT")
		uslp, _ := dbf.FieldValueByName(i, "USLP")
		if uslp == "" || uslp == " " {
			uslp = "0"
		}
		row.Uslp, err = strconv.ParseUint(uslp, 10, 64)
		if err != nil {
			// fmt.Println("===>", uslp, "<===>", err)
			row.Uslp = uint64(0)
		}

		err = chBatch.AppendStruct(&row)
		if err != nil {
			return fmt.Errorf("Append to batch: %w", err)
		}

	}
	bar.Finish()
	err = chBatch.Send()
	if err != nil {
		return fmt.Errorf("Send batch: %w", err)
	}
	chBatch.Close()

	return nil
}
func SaveSzP(dbfdata *[]byte, zf *dbase.ZFiles) error {
	chcon := *cnfg.Cnfg.CH
	sqlDropTemp := fmt.Sprintf(`DROP TABLE IF EXISTS %s.szp`, "tmp_lzp")
	sqlCrateTemp := fmt.Sprintf("%s %s", fmt.Sprintf(sqlScripts.NewSZP, "tmp_lzp"), "engine = Memory")
	sqlInsert := fmt.Sprintf(sqlScripts.InsertSZP, "tmp_lzp")
	ctx := context.Background()
	err := chcon.Exec(ctx, sqlDropTemp)
	if err != nil {
		return errors.Errorf("Error drop temp szp: %v", err)
	}
	err = chcon.Exec(ctx, sqlCrateTemp)
	if err != nil {
		return errors.Errorf("Error create temp szp: %v", err)
	}
	chBatch, err := chcon.PrepareBatch(ctx, sqlInsert)
	if err != nil {
		log.Fatal(err)
	}
	dbfTable, err := godbf.NewFromByteArray(*dbfdata, "CP866")
	bar := progressbar.Default(int64(dbfTable.NumberOfRecords()), "SaveSzP:")
	step := cnfg.Cnfg.SzBatchSize
	for i := 0; i < dbfTable.NumberOfRecords(); i++ {
		// if i > 10 {
		//	break
		// }

		if i%step == 0 && i != 0 {

			// defer cancel()

			err = chBatch.Send()
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Println(i, "batch send ")
			chBatch.Close()
			ctx, _ := context.WithTimeout(ctx, 20*time.Second)
			chBatch, err = chcon.PrepareBatch(ctx, sqlInsert)
		}
		if i%step == 0 && i != 0 {
			bar.Add(step)
		}
		var row dbase.SzP
		row.Id, _ = uuid.NewV7()
		row.RId = zf.RId
		idListStr, _ := dbfTable.FieldValueByName(i, "ID_LIST")
		row.Idlist, _ = strconv.ParseUint(idListStr, 10, 64)
		row.Ss, _ = dbfTable.FieldValueByName(i, "SS")
		row.SPol, _ = dbfTable.FieldValueByName(i, "S_POL")
		row.NPol, _ = dbfTable.FieldValueByName(i, "N_POL")
		row.Fam, _ = dbfTable.FieldValueByName(i, "FAM")
		row.Im, _ = dbfTable.FieldValueByName(i, "IM")
		row.Ot, _ = dbfTable.FieldValueByName(i, "OT")
		row.Gender, _ = dbfTable.FieldValueByName(i, "W")
		dr, _ := dbfTable.FieldValueByName(i, "DR")
		if dr == "" {
			dr = "18990101"
		}
		row.Dr, _ = time.Parse("20060102", dr)

		// row.Dr = fmt.Sprintf("%s-%s-%s 00:00:00", dr[:4], dr[4:6], dr[6:8])
		// fmt.Println(dr, row.Dr)
		row.SDoc, _ = dbfTable.FieldValueByName(i, "S_DOC")
		row.NDoc, _ = dbfTable.FieldValueByName(i, "N_DOC")
		cDoc, _ := dbfTable.FieldValueByName(i, "C_DOC")
		row.CDoc, _ = strconv.ParseUint(cDoc, 10, 64)
		row.PCity, _ = dbfTable.FieldValueByName(i, "P_CITY")
		row.Ul, _ = dbfTable.FieldValueByName(i, "UL")
		row.Dom = uint64(0)
		row.Bdom = ""
		row.Kor = uint64(0)
		row.Kv = uint64(0)
		row.Bkv = ""
		drb, _ := dbfTable.FieldValueByName(i, "DATE_RB")
		dre, _ := dbfTable.FieldValueByName(i, "DATE_RE")
		if drb == "" {
			row.DateRb = nil
		} else {
			drbp, _ := time.Parse("20060102", drb)
			row.DateRb = &drbp
		}
		if dre == "" {
			row.DateRe = nil
		} else {

			drep, _ := time.Parse("20060102", dre)
			row.DateRe = &drep
		}
		okato, _ := dbfTable.FieldValueByName(i, "OKATO_OMS")
		row.OkatoOms, _ = strconv.ParseUint(okato, 10, 64)
		row.QmOgrn, _ = dbfTable.FieldValueByName(i, "QM_OGRN")
		utyp, _ := dbfTable.FieldValueByName(i, "UT_TYPE")
		row.UType, _ = strconv.ParseUint(utyp, 10, 64)
		row.DType, _ = dbfTable.FieldValueByName(i, "D_TYPE")
		row.Comment, _ = dbfTable.FieldValueByName(i, "COMENT")
		row.SsF, _ = dbfTable.FieldValueByName(i, "SS_F")
		dd, _ := dbfTable.FieldValueByName(i, "DATEDEATH")
		if dd == "" {
			row.Datedeath = nil
		} else {

			ddp, _ := time.Parse("20060102", dd)
			row.Datedeath = &ddp
		}
		err = chBatch.AppendStruct(&row)
		if err != nil {
			return err
		}
	}
	bar.Finish()
	err = chBatch.Send()
	if err != nil {
		return err
	}
	chBatch.Close()

	return nil
}

func TmpToCurrent() error {
	ch := *cnfg.Cnfg.CH
	ctx := context.Background()
	ctx, done := context.WithTimeout(ctx, time.Second*3600)
	defer done()
	err := ch.Exec(ctx, fmt.Sprintf(sqlScripts.TmpToCurrentSZL, cnfg.Cnfg.CDb))
	if err != nil {
		return errors.Errorf("TmpToCurrentSZL error: %s", err.Error())
	}
	err = ch.Exec(ctx, fmt.Sprintf(sqlScripts.TmpToCurrentSZP, cnfg.Cnfg.CDb))
	if err != nil {
		return errors.Errorf("TmpToCurrentSZP error: %s", err.Error())
	}
	return nil
}

func AddFilesRecord(zf *dbase.ZFiles, status int) error {
	sqlInsert := fmt.Sprintf("insert into %s.files (id, filename, status) values (toUUID('%v'), '%s', %d)", cnfg.Cnfg.CDb, zf.RId, zf.FName, status)
	ch := *cnfg.Cnfg.CH
	err := ch.Exec(context.Background(), sqlInsert)
	if err != nil {
		return errors.Errorf("AddFilesRecord error: %s", err.Error())
	}
	return nil
}

func RefreshPersonList() error {
	ch := *cnfg.Cnfg.CH
	ctx := context.Background()
	ctx, done := context.WithTimeout(ctx, time.Second*6000)
	defer done()
	sqlDropPChash := fmt.Sprintf("drop table if exists %s.p_chash;", cnfg.Cnfg.CDb)
	sqlCreatePChash := fmt.Sprintf(`
CREATE MATERIALIZED VIEW %s.p_chash
            (
             chash Nullable(UInt64),
             ss String,
             id_list UInt64
                )
            ENGINE = Memory populate
AS
SELECT chash,
       ss,
       id_list
FROM (
         SELECT DISTINCT cityHash64(replaceAll(
                 concat(upperUTF8(fam), upperUTF8(im), upperUTF8(ot), substringUTF8(toString(dr), 1, 10)), 'Ё',
                 'Е'))               AS chash,
                         szp.ss      AS ss,
                         szp.id_list AS id_list
         FROM %s.szp_curent szp
                  INNER JOIN %s.szl_curent szl ON (szp.registr_id = szl.registr_id) AND (szp.id_list = szl.id_list)
                  INNER JOIN %s.c_kat AS ck ON ck.c_kat = szl.c_kat
         UNION ALL
         SELECT cityHash64(replaceAll(
                 concat(upperUTF8(fam), upperUTF8(im), upperUTF8(ot), substringUTF8(toString(dr), 1, 10)), 'Ё',
                 'Е'))      AS chash,
                szp.ss      AS ss,
                szp.id_list AS id_list
         FROM %s.szp_curent szp
                  INNER JOIN %s.szl_curent szl ON (szp.registr_id = szl.registr_id) AND (szp.id_list = szl.id_list)
           where (szl.c_kat = '05')
         )
`, cnfg.Cnfg.CDb, cnfg.Cnfg.CDb, cnfg.Cnfg.CDb, cnfg.Cnfg.CDb, cnfg.Cnfg.CDb, cnfg.Cnfg.CDb)
	sqlDropPChashArray := fmt.Sprintf("drop table if exists %s.p_chash_array;", cnfg.Cnfg.CDb)
	sqlCreatePChashArray := fmt.Sprintf(`
		CREATE MATERIALIZED VIEW %s.p_chash_array
		(
			chash_array Array(UInt64)
		)
		ENGINE = Memory populate
		AS
		SELECT groupArray(chash) AS chash_array
		FROM %s.p_chash;
`, cnfg.Cnfg.CDb, cnfg.Cnfg.CDb)
	err := ch.Exec(ctx, sqlDropPChash)
	if err != nil {
		return errors.Errorf("DropPCHash error: %s", err.Error())
	}
	err = ch.Exec(ctx, sqlCreatePChash)
	if err != nil {
		return errors.Errorf("CreatePCHash error: %s", err.Error())
	}
	err = ch.Exec(ctx, sqlDropPChashArray)
	if err != nil {
		return errors.Errorf("DropPCHashArray error: %s", err.Error())
	}
	err = ch.Exec(ctx, sqlCreatePChashArray)
	if err != nil {
		return errors.Errorf("CreatePCHashArray error: %s", err.Error())
	}
	return nil
}
