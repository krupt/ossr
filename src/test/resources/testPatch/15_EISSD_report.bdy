CREATE OR REPLACE PACKAGE BODY REPORT is
  c_package constant varchar2(30) := 'REPORT.';
  ex_org_id_is_null exception;
  ex_tmc_id_is_null exception;
  ex_tmc_type_is_null exception;
  ex_dog_id_is_null exception;
  --------------------------------------------------------
  -- попытка оптимизации
  --------------------------------------------------------
  function Get_Whole_Report(pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                            pi_tmc_type  in T_TMC.TMC_TYPE%type,
                            pi_op_type   in T_TMC_OPERATIONS.OP_TYPE%type,
                            pi_date_0    in date,
                            pi_date_1    in date,
                            pi_worker_id in T_USERS.USR_ID%type,
                            po_err_num   out pls_integer,
                            po_err_msg   out t_Err_Msg,
                            pi_gen_num   in number := 0)
    return sys_refcursor as
    res            sys_refcursor;
    l_date_beg     date;
    l_date_end     date;
    l_user_org_tab num_tab := num_tab();
    l_count        number;
  begin
    if ((pi_org_id is null) or (pi_org_id <= -1)) then
      raise ex_org_id_is_null;
    end if;
    -- checking access for operation for specified user
    if (not Security_pkg.Check_Rights_str('EISSD.TMC_REPORTS.WHOLE',
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg,
                                          null,
                                          null)) then
    
      return null;
    end if;
  
    l_user_org_tab := get_user_orgs_tab(pi_worker_id, 0);
    if (pi_date_0 is null) then
      l_date_beg := sysdate;
    else
      l_date_beg := pi_date_0;
    end if;
    if (pi_date_1 is null) then
      l_date_end := sysdate;
    else
      l_date_end := pi_date_1;
    end if;
    -- заполним temp таблицу
    begin
      REPORT_TEMP_OPERATIONS(pi_org_id,
                             pi_tmc_type,
                             pi_date_0 - 1,
                             pi_date_1 + 1);
    end;
  
    select count(*) into l_count from TEMP_OPERATIONS;
  
    open res for
      select distinct temp_tbl.op_date - 2 / 24 OP_DATE, -- дата операции
                      dv.dv_name OP_NAME,
                      coalesce(tm.modem_ser,
                               tum.usb_ser,
                               tc.card_number,
                               ts.sim_imsi,
                               ta.imsi,
                               ont.ont_ser,
                               iptv.serial_number,
                               tp.serial_number) IMSI,
                      coalesce(tc.card_ser, to_char(ts.sim_callsign), ta.nsd) CALLSIGN,
                      temp_tbl.tmc_id TMC_ID,
                      coalesce(mp.name_model,
                               mmu.usb_model,
                               tt1.title,
                               smod.stb_model,
                               NVL2(tt1.title, tt1.title || ' / ', '') ||
                               r1.kl_name) TARIF_OLD, -- назвние + регион
                      coalesce(mp.name_model,
                               mmu.usb_model,
                               smod.stb_model,
                               NVL2(tt2.title, tt2.title || ' / ', '') ||
                               r2.kl_name,
                               tt2.title) TARIF_NEW, -- название + регион
                      l_org1.org_name ORG_NAME_OLD, -- название старой организации
                      l_org2.org_name ORG_NAME_NEW, --название новой организации
                      org.org_name user_org, -- подключившая организация
                      decode(temp_tbl.tmc_type,
                             c_tmc_payd_card_id,
                             tc.card_full_cost,
                             /*7002,
                             nvl(osmi.retail_price, temp_tbl.TMC_TMP_COST),*/
                             temp_tbl.TMC_TMP_COST) TMC_TMP_COST,
                      tp1.person_lastname || ' ' || tp1.person_firstname || ' ' ||
                      tp1.person_middlename user_name, --юзер
                      tp2.person_lastname || ' ' || tp2.person_firstname || ' ' ||
                      tp2.person_middlename abonent, --абонент
                      ts.sim_iccid ICCID,
                      ts.sim_type sim_type,
                      dic_st.name sim_type_name,
                      iptv.mac_address,
                      iptv.is_second_hand,
                      pi_tmc_type tmc_type,
                      temp_tbl.unit_id,
                      dim.description sim_imaging_name
        from TEMP_OPERATIONS temp_tbl
        left join t_tarif_by_at_id tt1
          on temp_tbl.tar_id_0 = tt1.at_id
        left join t_tarif_by_at_id tt2
          on temp_tbl.tar_id_1 = tt2.at_id
        left join t_organizations l_org1
          on temp_tbl.owner_id_0 = l_org1.org_id
        left join t_organizations l_org2
          on temp_tbl.owner_id_1 = l_org2.org_id
        left join t_dic_mvno_region dmr1
          on tt1.at_region_id = dmr1.id
         and dmr1.reg_id = l_org1.region_id
        left join t_dic_mvno_region dmr2
          on tt2.at_region_id = dmr2.id
         and dmr2.reg_id = l_org2.region_id
        left join t_dic_region r1
          on dmr1.reg_id = r1.reg_id
        left join t_dic_region r2
          on dmr2.reg_id = r2.reg_id
        left join (select /*+ precompute_subquery */
                    column_value as id_org
                     from table(l_user_org_tab)) q
          on q.id_org = temp_tbl.org_id
        left join t_organizations org
          on temp_tbl.org_id = org.org_id
        left join t_users tu
          on temp_tbl.user_id = tu.usr_id
        left join t_person tp1
          on tu.usr_person_id = tp1.person_id
        left join t_dic_values dv
          on temp_tbl.op_type = dv.dv_id
        left join t_Tmc_Sim ts
          on temp_tbl.tmc_id = ts.tmc_id
        left join t_dic_sim_type dic_st
          on dic_st.id = nvl(temp_tbl.sim_type, ts.sim_type)
        left join t_Tmc_Adsl_Card ta
          on temp_tbl.tmc_id = ta.tmc_id
        left join t_tmc_pay_card tc
          on temp_tbl.tmc_id = tc.tmc_id
        left join t_tmc_modem_adsl tm
          on temp_tbl.tmc_id = tm.tmc_id
        left join t_tmc_modem_usb tum
          on temp_tbl.tmc_id = tum.tmc_id
        left join t_tmc_ont ont
          on temp_tbl.tmc_id = ont.tmc_id
        left join t_tmc_iptv iptv
          on temp_tbl.tmc_id = iptv.tmc_id
        left join t_ott_stb_model_info osmi
          on osmi.id = iptv.stb_model_id
        left join t_stb_model smod
          on smod.id = osmi.model_stb_id
        left join t_abonent ab
          on temp_tbl.tmc_id = ab.ab_tmc_id
        left join t_clients tcl
          on ab.client_id = tcl.client_id
        left join t_person tp2
          on tcl.fullinfo_id = tp2.person_id
        left join t_tmc_phone tp
          on tp.tmc_id = temp_tbl.tmc_id
        left join t_model_phone mp
          on mp.model_id = tp.model_id
        left join t_modem_model_usb mmu
          on mmu.id = tum.usb_model
        LEFT JOIN t_sim_by_imaging_type img ON (TO_NUMBER(ts.sim_imsi) BETWEEN img.imsi_range_start AND img.imsi_range_end)
        LEFT JOIN t_dic_sim_imaging dim ON dim.id = img.sim_imaging_type
       where (pi_op_type is null or
             (pi_op_type is not null and temp_tbl.op_type = pi_op_type))
            --     and temp_tbl.op_date >= date_beg - 1   -- расширенный диапазон для использ. индекса по дате
            --      and temp_tbl.op_date <= date_end + 1   --
         and trunc(temp_tbl.op_date - 2 / 24) >= l_date_beg
         and trunc(temp_tbl.op_date - 2 / 24) <= l_date_end
         and NVL(temp_tbl.error_id, 0) = 0
         and (pi_tmc_type is null or temp_tbl.tmc_type = pi_tmc_type or
             (pi_tmc_type > 7003 and temp_tbl.tmc_type = 7002))
       order by temp_tbl.unit_id,
                temp_tbl.op_date - 2 / 24,
                l_org1.org_name,
                l_org2.org_name;
    return res;
  exception
    when ex_org_id_is_null then
      po_err_num := 1001;
      po_err_msg := 'ID организации не указан.';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      logging_pkg.error('pi_org_id=' || pi_org_id || ',pi_tmc_type=' ||
                        pi_tmc_type || ',pi_op_type=' || pi_op_type ||
                        ',pi_date_0=' || pi_date_0 || ',pi_date_1=' ||
                        pi_date_1 || ',pi_worker_id=' || pi_worker_id ||
                        ',pi_gen_num=' || pi_gen_num,
                        'err:Get_Whole_Report');
      return null;
  end Get_Whole_Report;
  -----------------------------------------------------------
  function Get_Whole_By_TMC_Id_Report(pi_tmc_id    in T_ORGANIZATIONS.ORG_ID%type,
                                      pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                                      pi_worker_id in T_USERS.USR_ID%type,
                                      po_err_num   out pls_integer,
                                      po_err_msg   out t_Err_Msg,
                                      pi_gen_num   in number := 0)
    return sys_refcursor is
    res              sys_refcursor;
    l_user_org_tab   num_tab := num_tab();
    l_gen_doc_number t_Err_Msg := '';
  begin
    -- checking access for operation for specified user
    if (not Security_pkg.Check_User_Right_str('EISSD.TMC_REPORTS.WHOLE_BY_TMC',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;
    l_user_org_tab := get_user_orgs_tab(pi_worker_id, 0);
    if ((pi_tmc_id is null) or (pi_tmc_id <= -1)) then
      raise ex_tmc_id_is_null;
    end if;
    if (pi_gen_num = 1) then
      l_gen_doc_number := Get_Dogovor_Number(pi_org_id, 8004);
    end if;
  
    open res for
      select coalesce(TS.SIM_IMSI,
                      TR.RUIM_IMSI,
                      TA.NSD,
                      tm.modem_ser,
                      tc.CARD_NUMBER,
                      ont.ont_ser,
                      iptv.serial_number,
                      tp.serial_number,
                      tmu.usb_ser) IMSI,
             decode(op.op_type, 530, opu.callsign_0, OPU.CALLSIGN_1) CS, -- patrick 22405: при удалении тмц нужно брать callsign_0
             coalesce(mp.name_model,
                      mmu.usb_model,
                      smod.stb_model,
                      NVL2(R.KL_NAME,
                           CONCAT(TAR.TITLE, CONCAT(' / ', R.KL_NAME)),
                           TAR.TITLE)) TARIFF_NAME,
             OP.OP_ID OPERATION_ID,
             (case
                when (DV2.DV_ID >= 11 and DV2.DV_ID <= 15) then
                 CONCAT(DV.DV_NAME, CONCAT(' (', CONCAT(DV2.DV_NAME, ')')))
                else
                 DV.DV_NAME
              end)
             -- 44348 Вывод комментария
             -- 45265 Берем коммментарий по конкретной карте, а не по всей операции
             /*|| decode(op.op_type,
             19,
             ' (' || sct.comment_for_report || ')',
             '')*/ OPERATION_NAME,
             OP.OP_DATE - 2 / 24 DATE_OF_OP,
             NVL(O1.ORG_NAME, '') || (case
                                        when (rt1.is_org_rtm = 0 and
                                             nvl(r1.reg_id, ri1.reg_id) is not null) or
                                             rt1.org_id = 2001825 then
                                         '(РТК)'
                                        when rt1.is_org_rtm = 1 and
                                             nvl(r1.reg_id, ri1.reg_id) is not null then
                                         '(РТМ)'
                                        else
                                         ''
                                      end) ORG_NAME_0,
             NVL(O2.ORG_NAME, '') || (case
                                        when (rt2.is_org_rtm = 0 and
                                             nvl(r2.reg_id, ri2.reg_id) is not null) or
                                             rt2.org_id = 2001825 then
                                         '(РТК)'
                                        when rt2.is_org_rtm = 1 and
                                             nvl(r2.reg_id, ri2.reg_id) is not null then
                                         '(РТМ)'
                                        else
                                         ''
                                      end) ORG_NAME_1,
             OP.OP_TYPE,
             OP.ORG_ID OP_ORG_ID,
             OPORG.ORG_NAME OP_ORG_NAME,
             OP.USER_ID OP_USER_ID,
             CONCAT(OPPERS.PERSON_LASTNAME,
                    CONCAT(' ',
                           CONCAT(OPPERS.PERSON_FIRSTNAME,
                                  CONCAT(' ', OPPERS.PERSON_MIDDLENAME)))) OP_USER_NAME,
             TAR.TARIFF_TYPE,
             OPU.TAR_ID_0 TAR_ID_0,
             TAR.AT_ID TAR_ID_1,
             l_gen_doc_number DOG_NUMBER,
             TS.SIM_CALLSIGN_CITY CITY_NUM,
             TS.SIM_COLOR SIM_COLOR,
             decode(T.TMC_TYPE,
                    c_tmc_payd_card_id,
                    tc.card_full_cost,
                    nvl(opu.tmc_op_cost, t.tmc_tmp_cost)) TMC_TMP_COST, --T.TMC_TMP_COST
             Decode(TS.SIM_CALLSIGN_CITY,
                    null,
                    'Федеральный',
                    TS.SIM_CALLSIGN_CITY) CITY_NUM_TX,
             Decode(T.TMC_TYPE,
                    c_tmc_sim_id,
                    (select dv3.name dv_name
                       from t_dic_sim_color dv3
                      where dv3.id = TS.SIM_COLOR),
                    null) SIM_COLOR_TX,
             (case
               when t.tmc_type in (4, 8, 7003) and op.op_type = 22 and
                    nvl(ab.is_deleted, 0) = 0 then
                ab.ab_id
               when t.tmc_type not in (4, 8, 7003) and op.op_type = 22 then
                dt.id_dog
               else
                null
             end) link,
             (case
               when t.tmc_type in (4, 8, 7003) and op.op_type = 22 and
                    nvl(ab.is_deleted, 0) = 0 then
                c.client_type
               else
                null
             end) client_type,
             coalesce(iptv.priznak, t.tmc_type) tmc_type,
             opu.unit_id,
             ts.sim_type sim_type,
             dic_st.name sim_type_name,
             p.prm_name prm_name,
             decode(op.op_type, 22, ts_call.sim_callsign, null) callsign,
             ab.transfer_phone,
             iptv.mac_address,
             iptv.is_second_hand,
             opu.state_id,
             /*decode(op.op_type,
                    22,
                    (select distinct rd.id
                       from t_request_delivery rd
                       join t_req_deliv_equip rde
                         on rd.id = rde.req_id
                      where rde.tmc_id = iptv.tmc_id
                        and rd.state_id <> 10212),
                    null) req_id,*/
             rd.id req_id,       
             op.channel_id,
             dic.name channel_name,
             dim.description sim_imaging_name
        from T_TMC_OPERATIONS OP
        join T_TMC_OPERATION_UNITS OPU
          on OP.OP_ID = OPU.OP_ID
        join T_TMC T
          on OPU.TMC_ID = T.TMC_ID
        left join T_TMC_SIM TS
          on T.tmc_id = TS.TMC_ID
        left join t_dic_sim_type dic_st
          on nvl(opu.sim_type, 1) = dic_st.id
        left join T_TMC_RUIM TR
          on T.tmc_id = TR.tmc_id
        left join T_TMC_ADSL_CARD TA
          on T.tmc_id = TA.tmc_id
        left join t_tmc_pay_card TC
          on T.tmc_id = Tc.tmc_id
        left join t_tmc_modem_adsl TM
          on T.tmc_id = Tm.tmc_id
        left join T_ABSTRACT_TAR AT
          on OPU.TAR_ID_1 = AT.AT_ID
        left join T_ORGANIZATIONS O1
          on O1.ORG_ID = OPU.OWNER_ID_0
        left join T_ORG_IS_RTMOB rt1
          on o1.org_id = rt1.org_id
        left join t_dic_region r1
          on r1.org_id = o1.org_id
        left join t_dic_region_info ri1
          on ri1.rtmob_org_id = o1.org_id
        left join T_ORGANIZATIONS O2
          on O2.ORG_ID = OPU.OWNER_ID_1
        left join T_ORG_IS_RTMOB rt2
          on o2.org_id = rt2.org_id
        left join t_dic_region r2
          on r2.org_id = o2.org_id
        left join t_dic_region_info ri2
          on ri2.rtmob_org_id = o2.org_id
        join T_DIC_VALUES DV
          on OP.OP_TYPE = DV.DV_ID
        left join t_tarif_by_at_id TAR
          on TAR.ID = get_Last_tar_id_by_at_id(TAR.AT_ID)
         and AT.AT_ID = TAR.AT_ID
        left join T_DIC_VALUES DV2
          on DV2.DV_ID = OPU.ST_SKLAD_1
        left join t_dic_mvno_region dmr
          on dmr.id = AT.AT_REGION_ID
         and dmr.reg_id = o1.region_id
        left join t_dic_region R
          on dmr.REG_ID = R.REG_ID
        left join T_ORGANIZATIONS OPORG
          on OP.ORG_ID = OPORG.ORG_ID
        left join T_USERS OPUSER
          on OP.USER_ID = OPUSER.USR_ID
        left join T_PERSON OPPERS
          on OPUSER.USR_PERSON_ID = OPPERS.PERSON_ID
        left join t_tmc_ont ont
          on t.tmc_id = ont.tmc_id
        left join t_tmc_iptv iptv
          on t.tmc_id = iptv.tmc_id
        left join t_ott_stb_model_info osmi
          on osmi.id = iptv.stb_model_id
        left join t_stb_model smod
          on smod.id = osmi.model_stb_id
        left join t_abonent ab
          on op.op_id = ab.id_op
        left join t_dog_tmc dt
          on t.tmc_id = dt.id_tmc
      -- 30057
        left join t_clients c
          on ab.client_id = c.client_id
        left join t_tmc_phone tp
          on t.tmc_id = tp.tmc_id
        left join t_model_phone mp
          on tp.model_id = mp.model_id
      -- 45265
        left join t_sim_change_tar sct
          on sct.unit_id = opu.unit_id
        left join t_perm p
          on p.prm_id = opu.sim_perm
        left join t_tmc_modem_usb tmu
          on tmu.tmc_id = t.tmc_id
        left join t_modem_model_usb mmu
          on tmu.usb_model = mmu.id
        left join t_tmc_sim ts_call
          on ab.ab_tmc_id = ts_call.tmc_id
        left join t_dic_channels dic
          on dic.channel_id = op.channel_id
        left join t_tmc_operation_request tor
          on tor.oper_id=op.op_id
         and op.op_type=22 
        left join t_request_delivery rd
          on rd.id=tor.request_id
        LEFT JOIN t_sim_by_imaging_type img ON (TO_NUMBER(ts.sim_imsi) BETWEEN img.imsi_range_start AND img.imsi_range_end)
        LEFT JOIN t_dic_sim_imaging dim ON dim.id = img.sim_imaging_type
        -- and rd.state_id<>10212
       where ((pi_org_id is null) or (pi_org_id <= -1) or
             ((pi_org_id > -1) and
             (OPU.OWNER_ID_0 = pi_org_id or OPU.OWNER_ID_1 = pi_org_id)))
         and T.TMC_ID = pi_tmc_id
         and (NVL(OPU.ERROR_ID, 0) = 0)
         and ((OPU.TAR_ID_1 is null) or
             (TAR.ID = get_Last_tar_id_by_at_id(TAR.AT_ID)))
         and (OPU.OWNER_ID_0 in (select * from TABLE(l_user_org_tab)) or
             OPU.OWNER_ID_1 in (select * from TABLE(l_user_org_tab)))
         and (pi_org_id is null or pi_org_id = -1 or
             pi_org_id in (select * from TABLE(l_user_org_tab)))
       order by /*DATE_OF_OP*/ /*opu.unit_id asc, OPERATION_ID asc*/
                op.op_date,
                opu.unit_id;
    return res;
  exception
    when ex_tmc_id_is_null then
      po_err_num := 1001;
      po_err_msg := 'ID ТМЦ не указан.';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
  end Get_Whole_By_TMC_Id_Report;
  -----------------------------------------------------------
  function Get_Operation_Types(pi_worker_id in T_USERS.USR_ID%type,
                               po_err_num   out pls_integer,
                               po_err_msg   out t_Err_Msg)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select DV.DV_ID ID0, DV.DV_NAME NAME0
        from T_DIC_VALUES DV
       where DV.DV_TYPE_ID = 5
         --and not DV.DV_ID = 1901
       order by DV.DV_NAME asc;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
  end Get_Operation_Types;
  -----------------------------------------------------------
  function Get_Report_Akt_Sverki(pi_org_id    in t_organizations.org_id%type,
                                 pi_org_pid   in t_organizations.org_id%type,
                                 pi_dog_id    in t_dogovor.dog_id%type,
                                 pi_tmc_type  in t_tmc.tmc_type%type,
                                 pi_date      in t_tmc_operations.op_date%type,
                                 pi_worker_id in t_users.usr_id%type,
                                 po_err_num   out pls_integer,
                                 po_err_msg   out t_err_msg,
                                 pi_gen_num   in number := 0)
    return sys_refcursor is
    res              sys_refcursor;
    date1            date;
    for_dealer       pls_integer;
    l_org_list       num_tab := num_tab();
    l_gen_doc_number t_err_msg := '';
  begin
    if (orgs.is_have_usi_job(pi_worker_id) +
       orgs.is_have_sp_job(pi_worker_id) = 0) then
      for_dealer := 1;
    else
      for_dealer := 0;
    end if;
    if (pi_org_id is not null) then
      if (not Security_pkg.check_rights_str('EISSD.TMC_REPORTS.AKT_SVERKI',
                                            pi_org_id,
                                            pi_worker_id,
                                            po_err_num,
                                            po_err_msg,
                                            true,
                                            false)) then
        return null;
      end if;
      if (for_dealer = 1) then
        l_org_list := num_tab(pi_org_id);
      else
        l_org_list := orgs.get_orgs(pi_worker_id, pi_org_id, 0, 1, 1, 0, 0);
      end if;
    else
      -- checking access for operation for specified user
      if (not Security_pkg.check_user_right_str('EISSD.TMC_REPORTS.AKT_SVERKI',
                                                pi_worker_id,
                                                po_err_num,
                                                po_err_msg)) then
        return null;
      end if;
    
      l_org_list := get_user_orgs_tab(pi_worker_id, 0);
    end if;
  
    if (pi_date is null) then
      date1 := sysdate;
    else
      date1 := pi_date;
    end if;
  
    if (pi_gen_num = 1) then
      l_gen_doc_number := report.get_dogovor_number(pi_org_id, 8006);
    end if;
  
    open res for
      select distinct coalesce(sim.sim_imsi,
                               ruim.ruim_imsi,
                               adsl.imsi,
                               mdm.modem_ser,
                               card.ser_number,
                               ont.ont_ser,
                               tp.serial_number,
                               tmu.usb_ser,
                               iptv.serial_number) imsi,
                      coalesce(sim.sim_callsign,
                               ruim.ruim_callsign,
                               to_number(adsl.nsd)) callsign,
                      pi_tmc_type tmc_type,
                      tar.title op_tar_name,
                      nvl(val.dv_name,
                          decode(iptv.state_id,
                                 1,
                                 'Принято',
                                 2,
                                 'Продано',
                                 3,
                                 'Б/У',
                                 4,
                                 'Брак',
                                 5,
                                 'Утеряно',
                                 6,
                                 'Удалено',
                                 null)) op_sklad_st_name,
                      tmc.tmc_id,
                      decode(tmc.tmc_type,
                             c_tmc_payd_card_id,
                             payd_status.card_active_state,
                             c_tmc_sim_id,
                             status.crd_status,
                             c_tmc_ruim_id,
                             status.crd_status,
                             null) active_status,
                      last_oper.tar_id_1,
                      l_gen_doc_number dog_number,
                      org.org_id,
                      org.org_name,
                      org.region_id,
                      coalesce(card.card_full_cost,
                               pmc.cost_with_nds,
                               tmu.usb_full_cost,
                               get_tmc_cost_by_color(last_oper.tar_id_1,
                                                     CONSTANTS_PKG.c_tmc_sim_cost,
                                                     date1,
                                                     1,
                                                     nvl2(sim.sim_callsign_city,
                                                          9002,
                                                          9001),
                                                     nvl(sim.sim_color, tmc_sim.get_color_default),
                                                     null)) tmc_tmp_cost, -- номинальная стоимость
                      sim.sim_callsign_city,
                      /*(case
                        when tmc.tmc_type = c_tmc_sim_id and
                             sim.sim_color in (9101, 9102, 9103, 9104) then
                         val_color.dv_name
                        else
                         null
                      end)*/val_color.name dv_name,
                      last_oper.dog_number dog_id,
                      decode(tmc.tmc_type,
                             c_tmc_sim_id,
                             decode(status.crd_status,
                                    1501,
                                    status.crd_ch_date - 2 / 24,
                                    null),
                             c_tmc_ruim_id,
                             decode(status.crd_status,
                                    1501,
                                    status.crd_ch_date - 2 / 24,
                                    null),
                             c_tmc_payd_card_id,
                             decode(status.crd_status,
                                    1601,
                                    payd_status.card_change_date - 2 / 24,
                                    null),
                             null) change_status_date, -- дата активации
                      decode(tmc.tmc_type,
                             c_tmc_modem_id,
                             pmca.name_mark ||
                             decode(pt.name_type,
                                    null,
                                    null,
                                    '(' || pt.name_type || ')'),
                             7001,
                             om.ont_model,
                             7002,
                             nvl(sm.stb_model, smod.stb_model),
                             7003,
                             mp.name_model,
                             c_tmc_usb_modem_id,
                             mmu.usb_model,
                             null) modem_model,
                      tmc.tmc_tmp_cost zalog_st, -- залоговая стоимость
                      sim.sim_type,
                      dic_st.name sim_type_name,
                      iptv.mac_address,
                      /*osmi.*/
                      tmc.tmc_tmp_cost retail_price
        from (select u.tmc_id,
                     op.op_date,
                     u.owner_id_1,
                     u.st_sklad_1,
                     u.tar_id_1,
                     td.dog_number dog_number,
                     u.sim_type
                from t_tmc_operations op
                join t_tmc_operation_units u
                  on op.op_id = u.op_id
                left join t_dogovor td
                  on td.dog_id = nvl(op.op_dog_id, 0)
               where u.unit_id in
                     (select max(u.unit_id) keep(dense_rank last order by u.unit_id, op.op_date) u_id
                        from t_tmc_operations op, t_tmc_operation_units u
                       where op.op_id = u.op_id
                         and op.op_date - 2 / 24 <= date1
                         and NVL(u.error_id, 0) = 0
                         and (pi_dog_id is null or op.op_dog_id = pi_dog_id or
                             op.op_dog_id is null)
                         and u.tmc_id in
                             (select tmc_id
                                from t_tmc_operation_units,
                                     (select column_value org_id
                                        from table(l_org_list)) x_org
                              -- Условие, что карта находится на том же складе
                               where owner_id_1 = x_org.org_id)
                       group by u.tmc_id)) last_oper
        join (select column_value org_id from table(l_org_list)) x_org
          on last_oper.owner_id_1 = x_org.org_id
        join t_tmc tmc
          on last_oper.tmc_id = tmc.tmc_id
        left join t_tmc_sim sim
          on tmc.tmc_id = sim.tmc_id
        left join t_dic_sim_type dic_st
          on nvl(last_oper.sim_type, 1 /* sim.sim_type*/) = dic_st.id
        left join t_tmc_adsl_card adsl
          on tmc.tmc_id = adsl.tmc_id
        left join t_tmc_ruim ruim
          on tmc.tmc_id = ruim.tmc_id
        left join t_tmc_pay_card card
          on tmc.tmc_id = card.tmc_id
        left join t_tmc_modem_adsl mdm
          on tmc.tmc_id = mdm.tmc_id
        left join t_tmc_ont ont
          on tmc.tmc_id = ont.tmc_id
        left join t_model_ont om
          on ont.ont_model_id = om.id
        left join t_tmc_iptv iptv
          on tmc.tmc_id = iptv.tmc_id
        left join t_stb_model sm
          on iptv.stb_model_id = sm.id
            -- 77710
         and iptv.priznak = 7002
        left join t_ott_stb_model_info osmi
          on osmi.id = iptv.stb_model_id
         and iptv.priznak > 7003
        left join t_stb_model smod
          on smod.id = osmi.model_stb_id
        left join t_card_status_change status
          on sim.sim_imsi = status.crd_imsi
        left join t_tarif_by_at_id tar
          on last_oper.tar_id_1 = tar.At_Id
        left join t_dic_values val
          on last_oper.st_sklad_1 = val.dv_id
         and pi_tmc_type <= 7003
        left join t_dic_sim_color val_color
          on sim.sim_color = val_color.id 
        /*left join t_dic_values val_color
          on sim.sim_color = val_color.dv_id*/
        join t_organizations org
          on org.org_id = pi_org_id
        left join t_paydcard_status_change payd_status
          on card.tmc_id = payd_status.tmc_id
        left join t_modem_model_adsl pmca
          on mdm.modem_model = pmca.id_mark
        left join t_modem_adsl_port_type pt
          on pmca.id_type_port = pt.id_type
        left join t_tmc_phone tp
          on tmc.tmc_id = tp.tmc_id
        left join t_model_phone mp
          on tp.model_id = mp.model_id
        left join t_phone_model_cost pmc
          on mp.model_id = pmc.model_id
        left join t_tmc_modem_usb tmu
          on tmc.tmc_id = tmu.tmc_id
        left join t_modem_model_usb mmu
          on tmu.usb_model = mmu.id
       where
      -- Не рассматриваем утерю
       last_oper.st_sklad_1 <> 13 /*NOT IN (13, 14)*/
       and pi_tmc_type = nvl(iptv.priznak, tmc.tmc_type)
       and (pi_tmc_type != 7003 or (pmc.region_id in
       (select o.region_id
                                   from t_organizations o
                                  where o.org_id = last_oper.owner_id_1) and
       sysdate between pmc.ver_date_beg and
       nvl(pmc.ver_date_end, sysdate)))
       order by org.region_id,
                org.org_name,
                org.org_id,
                last_oper.tar_id_1,
                imsi;
    return res;
  exception
    when ex_org_id_is_null then
      po_err_num := 1001;
      po_err_msg := 'не верно задан ид организации';
      return null;
    when ex_tmc_type_is_null then
      po_err_num := 1002;
      po_err_msg := 'не верно задан тип тмц';
      return null;
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm;
      return null;
  end Get_Report_Akt_Sverki;
  -----------------------------------------------------------
  function Get_Report_Nakladnaya(pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                                 pi_tmc_type  in T_TMC.TMC_TYPE%type,
                                 pi_date      in T_TMC_OPERATIONS.OP_DATE%type,
                                 pi_worker_id in T_USERS.USR_ID%type,
                                 po_err_num   out pls_integer,
                                 po_err_msg   out t_Err_Msg,
                                 pi_op_id     in T_TMC_OPERATIONS.OP_ID%type := null,
                                 pi_gen_num   in number := 0)
    return sys_refcursor is
    res              sys_refcursor;
    date1            date;
    type1            pls_integer := null;
    l_gen_doc_number t_Err_Msg := '';
  begin
    if ((pi_org_id is null) or (pi_org_id <= -1)) then
      raise ex_org_id_is_null;
    end if;
  
    if (not Security_pkg.Check_Rights_str('EISSD.TMC_REPORTS.BILL.LIST',
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg,
                                          true,
                                          false)) then
      return null;
    end if;
  
    date1 := pi_date;
    if (date1 is null) then
      date1 := sysdate;
    end if;
  
    if (pi_tmc_type = c_tmc_sim_id or pi_tmc_type = c_tmc_ruim_id or
       pi_tmc_type = c_tmc_adsl_card_id) then
      type1 := pi_tmc_type;
    end if;
  
    if (pi_gen_num = 1) then
      l_gen_doc_number := Get_Dogovor_Number(pi_org_id, 8007);
    end if;
  
    if (pi_op_id is null) then
      open res for
        select distinct coalesce(TS.SIM_IMSI, TR.RUIM_IMSI, TA.IMSI) IMSI,
                        OPU.TAR_ID_1 TARIFF_ID,
                        T.TMC_TYPE,
                        OP.OP_ID,
                        l_gen_doc_number DOG_NUMBER,
                        coalesce(TS.SIM_CALLSIGN,
                                 TR.RUIM_CALLSIGN,
                                 to_number(TA.NSD)) CS
          from T_TMC_SIM             TS,
               T_TMC_RUIM            TR,
               T_TMC                 T,
               T_TMC_ADSL_CARD       TA,
               T_TMC_OPERATION_UNITS OPU,
               T_TMC_OPERATIONS      OP
         where T.TMC_ID = TS.TMC_ID(+)
           and T.tmc_id = TR.tmc_id(+)
           and T.tmc_id = TA.tmc_id(+)
           and T.TMC_ID = OPU.TMC_ID
           and OPU.OP_ID = OP.OP_ID
           and trunc(OP.OP_DATE) = trunc(date1)
           and OP.OP_TYPE in (18, 20, 21)
           and OPU.OWNER_ID_1 = pi_org_id
           and ((T.TMC_TYPE = type1) or (type1 is null))
         order by OP.OP_ID, TARIFF_ID, IMSI;
    elsif (pi_op_id = -100) then
      open res for
        select distinct coalesce(TS.SIM_IMSI, TR.RUIM_IMSI, TA.IMSI) IMSI,
                        OPU.TAR_ID_1 TARIFF_ID,
                        T.TMC_TYPE,
                        OP.OP_ID,
                        l_gen_doc_number DOG_NUMBER,
                        coalesce(TS.SIM_CALLSIGN,
                                 TR.RUIM_CALLSIGN,
                                 to_number(TA.NSD)) CS
          from T_TMC_SIM             TS,
               T_TMC_RUIM            TR,
               T_TMC                 T,
               T_TMC_ADSL_CARD       TA,
               T_TMC_OPERATION_UNITS OPU,
               T_TMC_OPERATIONS      OP
         where T.TMC_ID = TS.TMC_ID(+)
           and T.tmc_id = TR.tmc_id(+)
           and T.tmc_id = TA.tmc_id(+)
           and T.TMC_ID = OPU.TMC_ID
           and OPU.OP_ID = OP.OP_ID
           and trunc(OP.OP_DATE) = trunc(date1)
           and OP.OP_TYPE in (18, 20, 21)
           and OPU.OWNER_ID_1 = pi_org_id
           and ((T.TMC_TYPE = type1) or (type1 is null))
         order by TARIFF_ID, IMSI;
    else
      open res for
        select distinct coalesce(TS.SIM_IMSI, TR.RUIM_IMSI, TA.IMSI) IMSI,
                        OPU.TAR_ID_1 TARIFF_ID,
                        T.TMC_TYPE,
                        OP.OP_ID,
                        l_gen_doc_number DOG_NUMBER,
                        coalesce(TS.SIM_CALLSIGN,
                                 TR.RUIM_CALLSIGN,
                                 to_number(TA.NSD)) CS
          from T_TMC_SIM             TS,
               T_TMC_RUIM            TR,
               T_TMC                 T,
               T_TMC_ADSL_CARD       TA,
               T_TMC_OPERATION_UNITS OPU,
               T_TMC_OPERATIONS      OP
         where T.TMC_ID = TS.TMC_ID(+)
           and T.tmc_id = TR.tmc_id(+)
           and T.tmc_id = TA.tmc_id(+)
           and T.TMC_ID = OPU.TMC_ID
           and OPU.OP_ID = OP.OP_ID
           and trunc(OP.OP_DATE) = trunc(date1)
           and OP.OP_TYPE in (18, 20, 21)
           and OPU.OWNER_ID_1 = pi_org_id
           and ((T.TMC_TYPE = type1) or (type1 is null))
           and pi_op_id = OP.OP_ID
         order by TARIFF_ID, IMSI;
    end if;
    return res;
  exception
    when ex_org_id_is_null then
      po_err_num := 1001;
      po_err_msg := 'Не верно задан ИД организации';
      return null;
    when ex_tmc_type_is_null then
      po_err_num := 1002;
      po_err_msg := 'Не верно задан тип ТМЦ';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
  end Get_Report_Nakladnaya;
  ------------------------------------------------------------
  function Get_Report_Ostatki(pi_org_id         in array_num_2, -- 74819
                              pi_block          in number, -- 74819 Признак заблокированности организации
                              pi_org_relation   in num_tab, -- 74819 Массив связей
                              pi_tmc_type       in T_TMC.TMC_TYPE%type,
                              pi_num_type       in number, -- тип номера
                              pi_privilege_type in number, -- тип привилегированности
                              pi_worker_id      in T_USERS.USR_ID%type,
                              pi_report_form    in number, -- Форма отчета (1 - краткая, 2 - полная)
                              po_err_num        out pls_integer,
                              po_err_msg        out t_Err_Msg,
                              po_details        out sys_refcursor,
                              pi_gen_num        in number := 0)
    return sys_refcursor is
    res            sys_refcursor;
    res2           sys_refcursor := null;
    is_not_diler   number := NULL;
    l_perm_id      num_tab := num_tab();
    cnt            number;
    l_org_tab      Num_Tab;
    l_count        number;
    l_user_org_tab num_tab;
  begin
    logging_pkg.debug('pi_block:' || pi_block || 
                      ' pi_num_type:' || pi_num_type || 
                      ' pi_report_form' || pi_report_form || 
                      ' pi_tmc_type:' || pi_tmc_type,
                      'Get_Report_Ostatki');
  
    if (not Security_pkg.Check_User_Right_str('EISSD.TMC_REPORTS.OSTATKI',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;
  
    l_org_tab := get_orgs_tab_for_multiset(pi_orgs         => pi_org_id,
                                           pi_worker_id    => pi_worker_id,
                                           pi_block        => pi_block,
                                           pi_org_relation => pi_org_relation);
    select count(*) into l_count from table(pi_org_id);
  
    if (ORGS.Is_Have_USI_Job(pi_worker_id) +
       ORGS.Is_Have_SP_Job(pi_worker_id) = 0) then
      is_not_diler := 1;
    end if;
  
    select distinct p.prm_id bulk collect
      into l_perm_id
      from t_tmc_type_rights t
      join t_rights r
        on r.right_id = t.right_id
      join t_perm_rights pr
        on pr.pr_right_id = r.right_id
      join t_perm p
        on p.prm_id = pr.pr_prm_id
       and p.prm_type = 8500
     where t.tmc_type = Pi_TMC_type;
  
    l_user_org_tab := ORGS.get_user_orgs_by_prm(pi_worker_id => pi_worker_id,
                                                pi_rel_tab   => null,
                                                pi_prm_tab   => l_perm_id,
                                                pi_block     => pi_block,
                                                po_err_num   => po_err_num,
                                                po_err_msg   => po_err_msg);
  
    if l_count = 0 then
      l_org_tab := l_user_org_tab;
    else
      l_org_tab := intersects(l_org_tab, l_user_org_tab);
    end if;
  
    if pi_report_form = 1 or pi_tmc_type <> 8 then
      open res for
        select distinct last_t.ORG_ID,
                        OOO.ORG_NAME ORG_NAME,
                        last_t.TAR_ID,
                        coalesce(ont_m.ont_model,
                                 smod.stb_model,
                                 stb_m.stb_model,
                                 mp.name_model,
                                 mmu.usb_model,
                                 TTT.TITLE) TAR_NAME,
                        decode(last_t.tmc_type, 8, ttt.ver_date_end, null) tar_ver_date_end,
                        decode(last_t.tmc_type, 8, ttt.ver_date_beg, null) tar_ver_date_beg,
                        NVL2(last_t.TAR_ID,
                             TARSERV.Get_Conn_By_Tar_Id_Inner2(last_t.TAR_ID),
                             '') TAR_TYPE,
                        pi_tmc_type TMC_TYPE,
                        STOIMOST, -- стоимость (залоговая)
                        st_fact, -- стоимость (факт.)
                        last_t.ODD, -- количество
                        tmc_tmp_cost summ_zalog, --tmc_tmp_cost, -- сумма (залоговая)
                        summ_fact, -- сумма (факт.)
                        DOG_NUMBER,
                        num_type,
                        num_color,
                        dv_1.dv_name Str_type,
                        dv_2.name Str_color,
                        last_t.dog_number dogovor, --49446
                        org_pid,
                        nvl(org_pid.org_name, OOO.ORG_NAME) org_pid_name,
                        tmc_perm tmc_perm,
                        prm.prm_name prm_name,
                        sim_type,
                        sim_type_name,
                        osmi.is_second_hand,
                        dog_number as ots_dogovor
          from (select ORG_ID3 ORG_ID,
                       TAR_ID,
                       TMC_TYPE,
                       ST_ZALOG STOIMOST, -- стоимость (залоговая)
                       st_fact st_fact, -- фактическая тоимость (для модемов и карт оплаты)
                       count(*) ODD,
                       Sum(ST_ZALOG) tmc_tmp_cost, -- сумма (залоговая)
                       sum(st_fact) SUMM_FACT, -- сумма фактическая (для модемов и карт оплаты)
                       num_type,
                       num_color,
                       org_pid,
                       tmc_perm,
                       dog_number,
                       sim_type,
                       sim_type_name,
                       priznak
                  from (select distinct tor.ORG_ID ORG_ID3,
                                        coalesce(TS.TAR_ID,
                                                 TR.TAR_ID,
                                                 iptv.stb_model_id,
                                                 tp.model_id,
                                                 tum.usb_model) TAR_ID,
                                        T.TMC_ID,
                                        T.TMC_TYPE,
                                        nvl(Decode(T.TMC_TYPE,
                                                   c_tmc_modem_id,
                                                   omc.mort_cost,
                                                   c_tmc_payd_card_id,
                                                   omc.mort_cost,
                                                   t.tmc_tmp_cost),
                                            0) ST_ZALOG, --залоговая
                                        decode(tmc_type,
                                               c_tmc_sim_id,
                                               NVL2(ts.sim_callsign_city,
                                                    9002,
                                                    9001),
                                               null) num_type,
                                        decode(tmc_type,
                                               c_tmc_sim_id,
                                               ts.sim_color,
                                               null) num_color,
                                        nvl(Decode(t.tmc_type,
                                                   4,
                                                   tum.usb_full_cost,
                                                   7003,
                                                   pmc.cost_with_nds,
                                                   t.tmc_tmp_cost),
                                            0) ST_FACT,
                                        orgR.org_id org_pid,
                                        t.tmc_perm tmc_perm,
                                        dog.dog_number, --perov-av
                                        ts.sim_type,
                                        dic_st.name sim_type_name,
                                        iptv.priznak
                          from T_TMC T
                          Join T_ORG_TMC_STATUS OS
                            on T.TMC_ID = OS.TMC_ID
                          Join (Select distinct t.*
                                 from mv_org_tree t
                                where t.root_reltype <> 1006
                               Connect by prior t.org_id = t.org_pid
                                      and exists
                                (select column_value
                                             from table(l_org_tab)
                                            where column_value in
                                                  (2001272,
                                                   2001280,
                                                   2001825,
                                                   2001455,
                                                   2001433,
                                                   2001279,
                                                   2001454,
                                                   2001825,
                                                   1000001,
                                                   1000002,
                                                   1000003,
                                                   1000004,
                                                   1000005,
                                                   1000006,
                                                   1000007,
                                                   1000008,
                                                   1000009,
                                                   1000010)
                                               or t.org_reltype = 1001)
                                Start with t.org_id in
                                           (select column_value
                                              from table(l_org_tab))
                                       and t.org_reltype <> 1006) tor
                            on tor.ORG_ID = OS.ORG_ID
                          left Join T_TMC_SIM TS
                            on T.TMC_ID = TS.TMC_ID
                          left join t_dic_sim_type dic_st
                            on dic_st.id = ts.sim_type
                          Left join t_dic_sim_color dv
                            on dv.id = ts.sim_color
                          left join t_tmc_modem_usb tum
                            on t.tmc_id = tum.tmc_id
                          left Join T_TMC_RUIM TR
                            on T.tmc_id = TR.tmc_id
                          left Join t_Org_Mort_Cost omc
                            on omc.tmc_id = t.tmc_id
                           and omc.org_rel_id = tor.root_rel_id
                          left join t_tmc_pay_card TC
                            on t.tmc_id = tc.tmc_id
                          left join t_tmc_ont ont
                            on t.tmc_id = ont.tmc_id
                          left join t_tmc_iptv iptv
                            on t.tmc_id = iptv.tmc_id
                            and iptv.priznak = pi_tmc_type
                          left join t_tmc_phone tp
                            on t.tmc_id = tp.tmc_id
                          left join t_phone_model_cost pmc
                            on tp.model_id = pmc.model_id
                           and sysdate between pmc.ver_date_beg and
                               nvl(pmc.ver_date_end, sysdate)
                          left join t_organizations org
                            on t.org_id = org.org_id
                           and org.region_id = pmc.region_id
                          left join mv_org_tree rel
                            on rel.org_id = os.org_id
                           and rel.org_pid <> -1
                           and rel.root_org_pid <> -1
                           and rel.org_reltype not in (1005, 1006, 1009)
                          left join t_dogovor dog
                            on dog.org_rel_id = rel.root_rel_id
                           --and dog.is_enabled = 1
                           and (((pi_tmc_type = 6 and dog.dog_class_id = 2) or
                               (pi_tmc_type >= 7004 and
                               dog.dog_class_id in (10, 11, 12)) or
                               (pi_tmc_type != 6 and pi_tmc_type <= 7003 and
                               dog.dog_class_id not in (2, 4))))
                           and (os.dog_id is null or os.dog_id = dog.dog_id)
                          left join t_dogovor_prm dp
                            on dp.dp_dog_id = dog.dog_id
                           and dp.dp_is_enabled = 1
                          left join t_dic_region reg1
                            on reg1.org_id in (rel.org_id, rel.root_org_id)
                          left join t_dic_mrf mrf
                            on mrf.org_id in (rel.org_id, rel.org_pid)
                          left join t_org_ignore_right ir
                            on ir.org_id = os.org_id
                           and ir.type_org = 1
                          left join t_organizations orgR
                            on orgR.Org_Id in (mrf.org_id, reg1.org_id)
                            or (orgR.Org_Id = rel.root_org_id and
                               mrf.org_id is null and reg1.org_id is null)
                            or (ir.org_id is not null and
                               orgr.org_id = 2001269)
                         where OS.STATUS in (11, 14)
                           and (is_org_usi(os.org_id) = 1 or
                               dp.dp_prm_id in
                               (select * from table(l_perm_id)))
                           and t.is_deleted = 0
                           and (nvl(iptv.priznak, t.tmc_type) = pi_tmc_type )
                           and (t.tmc_type <> c_tmc_sim_id or
                               ((t.tmc_type = c_tmc_sim_id)
                               -- тип номера для симок
                               and (pi_num_type is null or
                               ((pi_num_type is not null) and
                               NVL2(ts.sim_callsign_city, 1, 0) = case
                                 when pi_num_type = 9001 then
                                  0
                                 when pi_num_type = 9002 then
                                  1
                               end))
                               -- привилигированность
                               and (pi_privilege_type is null or
                               ((pi_privilege_type is not null) and
                               ts.sim_color = pi_privilege_type))))
                           and (OS.ORG_ID in (select * from TABLE(l_org_tab)))
                           and (t.ORG_ID in (select * from TABLE(l_org_tab)))
                           )
                 group by ORG_ID3,
                          TAR_ID,
                          TMC_TYPE,
                          ST_ZALOG,
                          st_fact,
                          num_type,
                          num_color,
                          org_pid,
                          tmc_perm,
                          dog_number,
                          sim_type,
                          sim_type_name,
                          priznak
                 order by ORG_ID3 asc, TAR_ID asc) last_t
          left join t_tarif_by_at_id TTT
            on last_t.TAR_ID = TTT.at_ID
          left join T_ORGANIZATIONS OOO
            on last_t.ORG_ID = OOO.ORG_ID
          left join t_dic_values dv_1
            on last_t.num_type = dv_1.dv_id
          left join t_dic_sim_color dv_2
            on last_t.num_color = dv_2.id
          left join t_model_ont ont_m
            on ont_m.id = last_t.tar_id
          left join t_stb_model stb_m
            on stb_m.id = last_t.tar_id
           and last_t.priznak = 7002
          left join t_ott_stb_model_info osmi
            on osmi.id = last_t.tar_id
           and last_t.priznak > 7003
          left join t_stb_model smod
            on smod.id = osmi.model_stb_id
          left join t_model_phone mp
            on mp.model_id = last_t.tar_id
          left join t_organizations org_pid
            on last_t.org_pid = org_pid.org_id
          left join t_perm prm
            on prm.prm_id = last_t.tmc_perm
          left join t_modem_model_usb mmu
            on mmu.id = last_t.tar_id
         order by last_t.org_id,
                  last_t.dog_number,
                  dog_number,
                  last_t.TAR_ID;
    else
      open res for
        Select null ORG_ID,
               null ORG_NAME,
               null TAR_ID,
               null TAR_NAME,
               null tar_ver_date_end,
               null tar_ver_date_beg,
               null TAR_TYPE,
               null TMC_TYPE,
               null STOIMOST,
               null st_fact,
               null ODD,
               null summ_zalog,
               null summ_fact,
               null DOG_NUMBER,
               null num_type,
               null num_color,
               null Str_type,
               null Str_color,
               null dogovor,
               null org_pid,
               null org_pid_name,
               null tmc_perm,
               null prm_name,
               null sim_type,
               null sim_type_name
          from dual
         where 1 <> 1;
    end if;
  
    -- развётнутый отчёт
    if pi_report_form = 2 then
      -- Симки
      if (pi_tmc_type = c_tmc_sim_id) then
        open res2 for
          select distinct OS.ORG_ID ORG_ID,
                          TS.TAR_ID TAR_ID,
                          T.TMC_TYPE,
                          TS.SIM_IMSI IMSI,
                          TS.SIM_CALLSIGN CS,
                          ts.sim_callsign_city, -- гор. номер
                          DV.DV_NAME STATUS,
                          NVL(CSTS.CRD_STATUS, 1502) ACTIVE_STATUS,
                          NVL2(ts.sim_callsign_city, 9002, 9001) num_type,
                          ts.sim_color,
                          oso.org_name org_name,
                          orgr.org_id org_pid,
                          nvl(orgr.org_name, oso.org_name) org_pid_name,
                          tar.title TAR_NAME,
                          tar.tariff_type,
                          dv_tar.dv_name tariff_type_name,
                          t.tmc_perm,
                          prm.prm_name,
                          ts.sim_iccid,
                          tar.ver_date_beg,
                          tar.ver_date_end,
                          t.tmc_tmp_cost STOIMOST,
                          ts.sim_type sim_type,
                          dic_st.name sim_type_name,
                          null as ots_dogovor,
                          dim.description sim_imaging_name
            from T_TMC T
            join T_ORG_TMC_STATUS OS
              on T.TMC_ID = OS.TMC_ID
             and OS.STATUS in (11, 14)
            join T_TMC_SIM TS
              on TS.TMC_ID = T.TMC_ID
            left join t_dic_sim_type dic_st
              on ts.sim_type = dic_st.id
            left join T_DIC_VALUES DV
              on DV.DV_ID = OS.STATUS
            left join T_CARD_STATUS_CHANGE CSTS
              on TS.SIM_IMSI = CSTS.CRD_IMSI
            join t_organizations OSO
              on OS.ORG_ID = OSO.ORG_ID
            left join t_tarif_by_at_id tar
              on ts.TAR_ID = tar.at_ID
            left join t_dic_values dv_tar
              on dv_tar.dv_id = tar.tariff_type
            left join t_perm prm
              on t.tmc_perm = prm.prm_id
            join t_org_is_rtmob oir
              on OS.ORG_ID = oir.org_id
            left join t_dic_region dr
              on OS.ORG_ID = dr.org_id
            left Join mv_org_tree tree
              on tree.ORG_ID = OS.ORG_ID
             and ((oir.is_org_rtm = 0 and dr.org_id is not null and
                 tree.root_org_pid <> -1) or
                 -- Для Краснодарского филиала, т.к. у него родителя в РТМ нет
                 (dr.org_id = 2004862) or
                 (dr.org_id is null and
                 (tree.root_reltype = 1002 or tree.root_org_pid <> -1)))
            left join t_dogovor dog
              on dog.org_rel_id = tree.root_rel_id
             and dog.dog_class_id not in (2, 4)
             --and dog.is_enabled = 1
            left join t_dogovor_prm dp
              on dp.dp_dog_id = dog.dog_id
             and dp.dp_is_enabled = 1
            left join t_organizations org_pid
              on org_pid.org_id = tree.org_pid
            left join mv_org_tree rel
              on rel.org_id = os.org_id
             and rel.org_pid <> -1
             and rel.root_org_pid <> -1
             and rel.org_reltype not in (1005, 1006, 1009)
            left join t_dic_region reg1
              on reg1.org_id in (rel.org_id, rel.root_org_id)
            left join t_dic_mrf mrf
              on mrf.org_id in (rel.org_id, rel.org_pid)
            left join t_org_ignore_right ir
              on ir.org_id = os.org_id
             and ir.type_org = 1
            left join t_organizations orgR
              on orgR.Org_Id in (mrf.org_id, reg1.org_id)
              or (orgR.Org_Id = rel.root_org_id and mrf.org_id is null and
                 reg1.org_id is null)
              or (ir.org_id is not null and orgr.org_id = 2001269)
            LEFT JOIN t_sim_by_imaging_type img ON (TO_NUMBER(ts.sim_imsi) BETWEEN img.imsi_range_start AND img.imsi_range_end)
            LEFT JOIN t_dic_sim_imaging dim ON dim.id = img.sim_imaging_type
           where t.is_deleted = 0
             and (is_org_usi(os.org_id) = 1 or dog.dog_number is null or
                 dp.dp_prm_id in (select * from table(l_perm_id)))
             and (l_count = 0 or
                 OS.ORG_ID in
                 (Select distinct t.org_id
                     from mv_org_tree t
                    where t.root_reltype <> 1006
                   Connect by prior t.org_id = t.org_pid
                          and exists
                    (select column_value
                                 from table(l_org_tab)
                                where column_value in
                                      (2001272,
                                       2001280,
                                       2001825,
                                       2001455,
                                       2001433,
                                       2001279,
                                       2001454,
                                       2001825,
                                       1000001,
                                       1000002,
                                       1000003,
                                       1000004,
                                       1000005,
                                       1000006,
                                       1000007,
                                       1000008,
                                       1000009,
                                       1000010)
                                   or t.org_reltype = 1001)
                    Start with t.org_id in
                               (select column_value from table(l_org_tab))
                           and t.org_reltype <> 1006))
             and (pi_tmc_type = T.TMC_TYPE)
             and (OS.ORG_ID in (select * from TABLE(l_org_tab)))
                -- тип номера
             and (pi_num_type is null or ((pi_num_type is not null) and
                 NVL2(ts.sim_callsign_city, 1, 0) = case
                   when pi_num_type = 9001 then
                    0
                   when pi_num_type = 9002 then
                    1
                 end))
                -- привилигированность
             and (pi_privilege_type is null or
                 ((pi_privilege_type is not null) and
                 ts.sim_color = pi_privilege_type))
           order by OS.ORG_ID, TAR_ID, num_type, sim_color, cs asc;
      
        -- ADSL-карты
      elsif (pi_tmc_type = c_tmc_adsl_card_id) then
        open res2 for
          SELECT DISTINCT OS.ORG_ID      ORG_ID,
                          T.TMC_TYPE,
                          TA.IMSI        IMSI,
                          TA.NSD         CS,
                          DV.DV_NAME     STATUS,
                          1501           ACTIVE_STATUS,
                          DOG.DOG_NUMBER
            FROM T_TMC T
            JOIN T_TMC_ADSL_CARD TA
              ON TA.TMC_ID = T.TMC_ID
            JOIN T_ORG_TMC_STATUS OS
              ON OS.TMC_ID = T.TMC_ID
            JOIN T_DIC_VALUES DV
              ON DV.DV_ID = OS.STATUS
            JOIN T_ORGANIZATIONS OSO
              ON OS.ORG_ID = OSO.ORG_ID
            JOIN T_ORG_IS_RTMOB OIR
              ON os.org_id = OIR.ORG_ID
            LEFT JOIN T_DIC_REGION DR
              ON os.org_id = DR.ORG_ID
            JOIN MV_ORG_TREE TOR
              ON TOR.ORG_ID = OS.ORG_ID
             AND TOR.ROOT_RELTYPE <> 1006
             AND ((OS.ORG_ID = 2001825 AND TOR.ROOT_ORG_PID <> -1) OR
                 (OIR.IS_ORG_RTM = 0 AND DR.ORG_ID IS NOT NULL AND
                 TOR.ROOT_ORG_PID <> -1) OR
                 (OS.ORG_ID <> 2001825 AND DR.ORG_ID IS NULL AND
                 (TOR.ROOT_RELTYPE = 1002 OR TOR.ROOT_ORG_PID <> -1)))
            LEFT JOIN T_DOGOVOR DOG
              ON DOG.ORG_REL_ID = TOR.ROOT_REL_ID
             AND DOG.DOG_CLASS_ID NOT IN (2, 4)
             --AND DOG.IS_ENABLED = 1
            LEFT JOIN T_DOGOVOR_PRM DP
              ON DP.DP_DOG_ID = DOG.DOG_ID
             AND DP.DP_IS_ENABLED = 1
           WHERE OS.STATUS in (11, 14)
             AND (IS_ORG_USI(OS.ORG_ID) = 1 OR
                 DP.DP_PRM_ID IN (SELECT * FROM TABLE(L_perm_ID)))
             AND T.IS_DELETED = 0
             AND (L_COUNT = 0 OR
                 OS.ORG_ID IN
                 (SELECT DISTINCT T.ORG_ID
                     FROM MV_ORG_TREE T
                    WHERE T.ROOT_RELTYPE NOT IN (1005, 1006)
                   CONNECT BY PRIOR T.ORG_ID = T.ORG_PID
                          AND EXISTS
                    (SELECT COLUMN_VALUE
                                 FROM TABLE(L_ORG_TAB)
                                WHERE COLUMN_VALUE IN
                                      (2001272,
                                       2001280,
                                       2001825,
                                       2001455,
                                       2001433,
                                       2001279,
                                       2001454,
                                       2001825,
                                       1000001,
                                       1000002,
                                       1000003,
                                       1000004,
                                       1000005,
                                       1000006,
                                       1000007,
                                       1000008,
                                       1000009,
                                       1000010)
                                   OR T.ORG_RELTYPE = 1001)
                    START WITH T.ORG_ID IN
                               (SELECT COLUMN_VALUE FROM TABLE(L_ORG_TAB))
                           AND T.ORG_RELTYPE NOT IN (1005, 1006)))
             AND (pi_TMC_TYPE = T.TMC_TYPE)
             AND (OS.ORG_ID IN (SELECT * FROM TABLE(L_ORG_TAB)))
           ORDER BY ORG_ID ASC, DOG_NUMBER, CS ASC, IMSI ASC;
      
        -- Карты оплаты
      elsif (pi_tmc_type = c_tmc_payd_card_id) then
        open res2 for
          select distinct OS.ORG_ID             ORG_ID,
                          T.TMC_TYPE,
                          TC.CARD_NUMBER        IMSI, -- серийный номер
                          tc.card_ident         CS, -- id
                          DV.DV_NAME            STATUS,
                          psc.card_active_state ACTIVE_STATUS,
                          t.tmc_tmp_cost        ST_NOMINAL, -- номинальная стоимость
                          omc.mort_cost         ST_ZALOG, -- залоговая стоимость
                          dog.dog_number
            from t_tmc_pay_card   TC,
                 T_DIC_VALUES     DV,
                 t_organizations  OSO,
                 T_ORG_TMC_STATUS OS
            join T_TMC T
              on os.tmc_id = t.tmc_id
            left join (select psc1.tmc_id,
                              psc1.card_active_state,
                              max(psc1.card_change_date) card_change_date
                         from t_paydcard_status_change PSC1
                        group by psc1.tmc_id, psc1.card_active_state) PSC
              on psc.tmc_id = t.tmc_id -- последнее изменение статуса карты
            join t_org_is_rtmob oir
              on OS.ORG_ID = oir.org_id
            left join t_dic_region dr
              on OS.ORG_ID = dr.org_id
            Join mv_org_tree tor
              on tor.ORG_ID = OS.ORG_ID
             and ((oir.is_org_rtm = 0 and dr.org_id is not null and
                 tor.root_org_pid <> -1) or
                 (dr.org_id is null and
                 (tor.root_reltype = 1002 or tor.root_org_pid <> -1)))
            left join t_dogovor dog
              on dog.org_rel_id = tor.root_rel_id
             and dog.dog_class_id = 2
             --and dog.is_enabled = 1
            left join t_dogovor_prm dp
              on dp.dp_dog_id = dog.dog_id
             and dp.dp_is_enabled = 1
            left Join t_Org_Mort_Cost omc
              on omc.tmc_id = t.tmc_id
             and omc.org_rel_id = tor.root_rel_id
           where OS.STATUS in (11, 14)
             and (is_org_usi(os.org_id) = 1 or
                 dp.dp_prm_id in (select * from table(l_perm_id)))
             and t.is_deleted = 0
             and OS.ORG_ID = OSO.ORG_ID
             and (l_count = 0 or
                 OS.ORG_ID in
                 ( -- 44915
                   Select distinct t.org_id
                     from mv_org_tree t
                    where t.root_reltype <> 1006
                   Connect by prior t.org_id = t.org_pid
                          and exists
                    (select column_value
                                 from table(l_org_tab)
                                where column_value in
                                      (2001272,
                                       2001280,
                                       2001825,
                                       2001455,
                                       2001433,
                                       2001279,
                                       2001454,
                                       2001825,
                                       1000001,
                                       1000002,
                                       1000003,
                                       1000004,
                                       1000005,
                                       1000006,
                                       1000007,
                                       1000008,
                                       1000009,
                                       1000010)
                                   or t.org_reltype = 1001)
                    Start with t.org_id in
                               (select column_value from table(l_org_tab))
                           and t.org_reltype <> 1006))
             and (pi_tmc_type = T.TMC_TYPE)
             and (OS.ORG_ID in (select * from TABLE(l_org_tab)))
             and T.tmc_id = TC.tmc_id
             and DV.DV_ID = OS.STATUS
           order by ORG_ID     asc,
                    dog_number,
                    ST_NOMINAL,
                    ST_ZALOG,
                    CS         asc,
                    IMSI       asc;
        -- Модемы
      elsif (pi_tmc_type = c_tmc_modem_id) then
        open res2 for
          select distinct OS.ORG_ID      ORG_ID,
                          T.TMC_TYPE,
                          TM.MODEM_SER   IMSI, -- серийный номер
                          tm.name_type   CS, -- тип порта
                          DV.DV_NAME     STATUS,
                          t.tmc_tmp_cost ST_NOMINAL, -- номинальная тоимость
                          omc.mort_cost  ST_ZALOG, -- залоговая стоимость
                          name_mark      mark,
                          dog.dog_number
            from (select tm1.tmc_id,
                         tm1.modem_ser,
                         pt.name_type,
                         pmc.name_mark
                    from t_tmc_modem_adsl TM1
                    left join t_modem_model_adsl PMC
                      on tm1.modem_model = pmc.id_mark
                    left join t_modem_adsl_port_type PT
                      on pmc.id_type_port = pt.id_type) TM
            JOIN T_TMC T
              ON TM.TMC_ID = T.TMC_ID
            JOIN T_ORG_TMC_STATUS OS
              ON OS.TMC_ID = T.TMC_ID
            JOIN T_DIC_VALUES DV
              ON DV.DV_ID = OS.STATUS
            JOIN T_ORGANIZATIONS OSO
              ON OS.ORG_ID = OSO.ORG_ID
            join t_org_is_rtmob oir
              on OS.ORG_ID = oir.org_id
            left join t_dic_region dr
              on OS.ORG_ID = dr.org_id
            Join mv_org_tree tor
              on tor.ORG_ID = OS.ORG_ID
             and ((oir.is_org_rtm = 0 and dr.org_id is not null and
                 tor.root_org_pid <> -1) or
                 (dr.org_id is null and
                 (tor.root_reltype = 1002 or tor.root_org_pid <> -1)))
            left join t_dogovor dog
              on dog.org_rel_id = tor.root_rel_id
             and dog.dog_class_id not in (2, 4)
             --and dog.is_enabled = 1
            left join t_dogovor_prm dp
              on dp.dp_dog_id = dog.dog_id
             and dp.dp_is_enabled = 1
            LEFT JOIN T_ORG_MORT_COST OMC
              ON OMC.TMC_ID = T.TMC_ID
             AND OMC.ORG_REL_ID = TOR.ID
           WHERE OS.STATUS in (11, 14)
             and (is_org_usi(os.org_id) = 1 or
                 dp.dp_prm_id in (select * from table(l_perm_id)))
             AND T.IS_DELETED = 0
             AND (L_COUNT = 0 OR
                 OS.ORG_ID IN
                 ( -- 44915
                   SELECT DISTINCT T.ORG_ID
                     FROM MV_ORG_TREE T
                    WHERE T.ROOT_RELTYPE NOT IN (1005, 1006)
                   CONNECT BY PRIOR T.ORG_ID = T.ORG_PID
                          AND EXISTS
                    (SELECT COLUMN_VALUE
                                 FROM TABLE(L_ORG_TAB)
                                WHERE COLUMN_VALUE IN
                                      (2001272,
                                       2001280,
                                       2001825,
                                       2001455,
                                       2001433,
                                       2001279,
                                       2001454,
                                       2001825,
                                       1000001,
                                       1000002,
                                       1000003,
                                       1000004,
                                       1000005,
                                       1000006,
                                       1000007,
                                       1000008,
                                       1000009,
                                       1000010)
                                   OR T.ORG_RELTYPE = 1001)
                    START WITH T.ORG_ID IN
                               (SELECT COLUMN_VALUE FROM TABLE(L_ORG_TAB))
                           AND T.ORG_RELTYPE NOT IN (1005, 1006)))
             AND (PI_TMC_TYPE = T.TMC_TYPE)
             AND (OS.ORG_ID IN (SELECT * FROM TABLE(L_ORG_TAB)))
           ORDER BY ORG_ID     ASC,
                    DOG_NUMBER,
                    ST_NOMINAL,
                    ST_ZALOG,
                    CS         ASC,
                    IMSI       ASC;
        --USB-модемы
      elsif (pi_tmc_type = c_tmc_usb_modem_id) then
        open res2 for
          select distinct OS.ORG_ID,
                          T.TMC_TYPE,
                          TM.Usb_Ser     IMSI, -- серийный номер
                          DV.DV_NAME     STATUS,
                          tm.cost        ST_NOMINAL, -- номинальная тоимость
                          tm.usb_model   mark,
                          dog.dog_number
            from (select tm1.tmc_id, tm1.usb_ser, um.usb_model, um.cost
                    from t_tmc_modem_usb TM1
                    left join t_modem_model_usb um
                      on um.id = tm1.usb_model) tm
            join T_TMC T
              on tm.tmc_id = t.tmc_id
            join T_ORG_TMC_STATUS OS
              on os.tmc_id = t.tmc_id
             and OS.STATUS in (11, 14)
            join T_DIC_VALUES DV
              on DV.DV_ID = OS.STATUS
            join t_organizations OSO
              on OS.ORG_ID = OSO.ORG_ID
            join t_org_is_rtmob oir
              on OS.ORG_ID = oir.org_id
            left join t_dic_region dr
              on OS.ORG_ID = dr.org_id
            Join mv_org_tree tor
              on tor.ORG_ID = OS.ORG_ID
             and tor.root_reltype <> 1006
             and ((oir.is_org_rtm = 0 and dr.org_id is not null and
                 tor.root_org_pid <> -1) or
                 (dr.org_id is null and
                 (tor.root_reltype = 1002 or tor.root_org_pid <> -1)))
            left join t_dogovor dog
              on dog.org_rel_id = tor.root_rel_id
             and dog.dog_class_id not in (2, 4)
             --and dog.is_enabled = 1
            left join t_dogovor_prm dp
              on dp.dp_dog_id = dog.dog_id
             and dp.dp_is_enabled = 1
             --and dp.dp_is_enabled = 1
            left Join t_Org_Mort_Cost omc
              on omc.tmc_id = t.tmc_id
             and omc.org_rel_id = tor.root_rel_id
           where t.is_deleted = 0
             and (is_org_usi(os.org_id) = 1 or
                 dp.dp_prm_id in (select * from table(l_perm_id)))
             and (l_count = 0 or
                 OS.ORG_ID in
                 ( -- 44915
                   Select distinct t.org_id
                     from mv_org_tree t
                    where t.root_reltype <> 1006
                   Connect by prior t.org_id = t.org_pid
                          and exists
                    (select column_value
                                 from table(l_org_tab)
                                where column_value in
                                      (2001272,
                                       2001280,
                                       2001825,
                                       2001455,
                                       2001433,
                                       2001279,
                                       2001454,
                                       2001825,
                                       1000001,
                                       1000002,
                                       1000003,
                                       1000004,
                                       1000005,
                                       1000006,
                                       1000007,
                                       1000008,
                                       1000009,
                                       1000010)
                                   or t.org_reltype = 1001)
                    Start with t.org_id in
                               (select column_value from table(l_org_tab))
                           and t.org_reltype <> 1006))
             and (pi_tmc_type = T.TMC_TYPE)
             and (OS.ORG_ID in (select * from TABLE(l_org_tab)))
           order by OS.ORG_ID      asc,
                    dog.dog_number,
                    mark,
                    ST_NOMINAL,
                    IMSI           asc;
        -- FTTX
      elsif (pi_tmc_type = 7001) then
        open res2 for
          select distinct OS.ORG_ID,
                          T.TMC_TYPE,
                          TM.ont_ser, -- серийный номер
                          DV.DV_NAME STATUS,
                          t.tmc_tmp_cost ST_NOMINAL, -- номинальная тоимость
                          omc.mort_cost ST_ZALOG, -- залоговая стоимость
                          tm.id mark_id,
                          tm.ont_model mark,
                          dog.dog_number,
                          sum(t.tmc_tmp_cost) SUM_NOMINAL, -- номинальная тоимость
                          sum(omc.mort_cost) SUM_ZALOG, -- залоговая стоимость
                          count(*) as cnt
            from (select ton.tmc_id, ton.ont_ser, tom.ont_model, tom.id
                    from t_tmc_ont ton
                    left join t_model_ont tom
                      on tom.id = ton.ont_model_id) tm,
                 T_DIC_VALUES DV,
                 t_organizations OSO,
                 T_ORG_TMC_STATUS OS
            join T_TMC T
              on os.tmc_id = t.tmc_id
            Join mv_org_tree tor
              on tor.ORG_ID = OS.ORG_ID
             and tor.root_org_pid <> -1
            left join t_dogovor dog
              on dog.org_rel_id = tor.root_rel_id
             and dog.dog_class_id not in (2, 4)
             --and dog.is_enabled = 1
            left join t_dogovor_prm dp
              on dp.dp_dog_id = dog.dog_id
             and dp.dp_is_enabled = 1
            left Join t_Org_Mort_Cost omc
              on omc.tmc_id = t.tmc_id
             and omc.org_rel_id = tor.root_rel_id
           where OS.STATUS in (11, 14)
             and (is_org_usi(os.org_id) = 1 or
                 dp.dp_prm_id in (select * from table(l_perm_id)))
             and t.is_deleted = 0
             and OS.ORG_ID = OSO.ORG_ID
             and (l_count = 0 or
                 OS.ORG_ID in
                 ( -- 44915
                   Select distinct t.org_id
                     from mv_org_tree t
                    where t.root_reltype <> 1006
                   Connect by prior t.org_id = t.org_pid
                          and exists
                    (select column_value
                                 from table(l_org_tab)
                                where column_value in
                                      (2001272,
                                       2001280,
                                       2001825,
                                       2001455,
                                       2001433,
                                       2001279,
                                       2001454,
                                       2001825,
                                       1000001,
                                       1000002,
                                       1000003,
                                       1000004,
                                       1000005,
                                       1000006,
                                       1000007,
                                       1000008,
                                       1000009,
                                       1000010)
                                   or t.org_reltype = 1001)
                    Start with t.org_id in
                               (select column_value from table(l_org_tab))
                           and t.org_reltype <> 1006))
             and (pi_tmc_type = T.TMC_TYPE)
             and (OS.ORG_ID in (select * from TABLE(l_org_tab)))
             and T.tmc_id = Tm.tmc_id
             and DV.DV_ID = OS.STATUS
           group by OS.ORG_ID,
                    dog.dog_number,
                    T.TMC_TYPE,
                    TM.ont_ser,
                    DV.DV_NAME,
                    t.tmc_tmp_cost,
                    omc.mort_cost,
                    tm.id,
                    tm.ont_model
           order by ORG_ID         asc,
                    dog.dog_number,
                    ST_NOMINAL,
                    ST_ZALOG,
                    cnt,
                    ont_ser        asc;
        -- STB
      elsif (pi_tmc_type = 7002) then
        open res2 for
          select distinct OS.ORG_ID,
                          T.TMC_TYPE,
                          tm.tmc_id stb_ser, -- серийный номер
                          DV.DV_NAME STATUS,
                          t.tmc_tmp_cost ST_NOMINAL, -- номинальная тоимость
                          omc.mort_cost ST_ZALOG, -- залоговая стоимость
                          tm.stb_model mark_id,
                          tm.stb_model mark,
                          dog.dog_number,
                          sum(t.tmc_tmp_cost) SUM_NOMINAL, -- номинальная тоимость
                          sum(omc.mort_cost) SUM_ZALOG, -- залоговая стоимость
                          count(*) as cnt
            from (select ti.tmc_id, sm.stb_model
                    from t_tmc_iptv ti
                    left join t_stb_model sm
                      on sm.id = ti.stb_model_id
                   where ti.priznak = pi_tmc_type) tm,
                 T_DIC_VALUES DV,
                 t_organizations OSO,
                 T_ORG_TMC_STATUS OS
            join T_TMC T
              on os.tmc_id = t.tmc_id
            Join mv_org_tree tor
              on tor.ORG_ID = OS.ORG_ID
             and tor.root_org_pid <> -1
            left join t_dogovor dog
              on dog.org_rel_id = tor.root_rel_id
             and dog.dog_class_id not in (2, 4)
             --and dog.is_enabled = 1
            left join t_dogovor_prm dp
              on dp.dp_dog_id = dog.dog_id
             and dp.dp_is_enabled = 1
            left Join t_Org_Mort_Cost omc
              on omc.tmc_id = t.tmc_id
             and omc.org_rel_id = tor.root_rel_id
           where OS.STATUS in (11, 14)
             and (is_org_usi(os.org_id) = 1 or
                 dp.dp_prm_id in (select * from table(l_perm_id)))
             and t.is_deleted = 0
             and OS.ORG_ID = OSO.ORG_ID
             and (l_count = 0 or
                 OS.ORG_ID in
                 ( -- 44915
                   Select distinct t.org_id
                     from mv_org_tree t
                    where t.root_reltype <> 1006
                   Connect by prior t.org_id = t.org_pid
                          and exists
                    (select column_value
                                 from table(l_org_tab)
                                where column_value in
                                      (2001272,
                                       2001280,
                                       2001825,
                                       2001455,
                                       2001433,
                                       2001279,
                                       2001454,
                                       2001825,
                                       1000001,
                                       1000002,
                                       1000003,
                                       1000004,
                                       1000005,
                                       1000006,
                                       1000007,
                                       1000008,
                                       1000009,
                                       1000010)
                                   or t.org_reltype = 1001)
                    Start with t.org_id in
                               (select column_value from table(l_org_tab))
                           and t.org_reltype <> 1006))
             and (pi_tmc_type = T.TMC_TYPE)
             and (OS.ORG_ID in (select * from TABLE(l_org_tab)))
             and T.tmc_id = Tm.tmc_id
             and DV.DV_ID = OS.STATUS
           group by OS.ORG_ID,
                    dog.dog_number,
                    T.TMC_TYPE,
                    tm.tmc_id,
                    DV.DV_NAME,
                    t.tmc_tmp_cost,
                    omc.mort_cost,
                    stb_model
           order by ORG_ID         asc,
                    dog.dog_number,
                    ST_NOMINAL,
                    ST_ZALOG,
                    cnt,
                    stb_ser        asc;
      
      elsif (pi_tmc_type = 7003) then
        open res2 for
          select distinct os.org_id,
                          t.tmc_type,
                          tm.serial_number imsi, -- серийный номер
                          dv.dv_name status,
                          tm.cost_with_nds ST_NOMINAL, -- номинальная тоимость
                          omc.mort_cost st_zalog, -- залоговая стоимость
                          tm.name_model mark,
                          dog.dog_number,
                          sum(tm.cost_with_nds) SUM_NOMINAL, -- номинальная тоимость
                          sum(omc.mort_cost) SUM_ZALOG, -- залоговая стоимость
                          count(*) as cnt
            from (select tp.tmc_id,
                         tp.serial_number,
                         mp.name_model,
                         pmc.cost_with_nds,
                         pmc.region_id
                    from t_tmc_phone tp
                    left join t_model_phone mp
                      on mp.model_id = tp.model_id
                    left join t_phone_model_cost pmc
                      on tp.model_id = pmc.model_id
                     and sysdate between pmc.ver_date_beg and
                         nvl(pmc.ver_date_end, sysdate)) tm,
                 t_dic_values dv,
                 t_organizations oso,
                 t_org_tmc_status os
            join t_tmc t
              on os.tmc_id = t.tmc_id
            join t_org_is_rtmob oir
              on OS.ORG_ID = oir.org_id
            left join t_dic_region dr
              on OS.ORG_ID = dr.org_id
            Join mv_org_tree tor
              on tor.ORG_ID = OS.ORG_ID
             and ((oir.is_org_rtm = 0 and dr.org_id is not null and
                 tor.root_org_pid <> -1) or
                 (dr.org_id is null and
                 (tor.root_reltype = 1002 or tor.root_org_pid <> -1)))
            left join t_dogovor dog
              on dog.org_rel_id = tor.root_rel_id
             and dog.dog_class_id not in (2, 4)
             --and dog.is_enabled = 1
            left join t_dogovor_prm dp
              on dp.dp_dog_id = dog.dog_id
             and dp.dp_is_enabled = 1
            left join t_org_mort_cost omc
              on omc.tmc_id = t.tmc_id
             and omc.org_rel_id = tor.root_rel_id
           where os.status in (11, 14)
             and (is_org_usi(os.org_id) = 1 or
                 dp.dp_prm_id in (select * from table(l_perm_id)))
             and t.is_deleted = 0
             and os.org_id = oso.org_id
             and (l_count = 0 or
                 OS.ORG_ID in
                 ( -- 44915
                   Select distinct t.org_id
                     from mv_org_tree t
                    where t.root_reltype <> 1006
                   Connect by prior t.org_id = t.org_pid
                          and exists
                    (select column_value
                                 from table(l_org_tab)
                                where column_value in
                                      (2001272,
                                       2001280,
                                       2001825,
                                       2001455,
                                       2001433,
                                       2001279,
                                       2001454,
                                       2001825,
                                       1000001,
                                       1000002,
                                       1000003,
                                       1000004,
                                       1000005,
                                       1000006,
                                       1000007,
                                       1000008,
                                       1000009,
                                       1000010)
                                   or t.org_reltype = 1001)
                    Start with t.org_id in
                               (select column_value from table(l_org_tab))
                           and t.org_reltype <> 1006))
             and (pi_tmc_type = t.tmc_type)
             and (os.org_id in (select * from TABLE(l_org_tab)))
             and t.tmc_id = tm.tmc_id
             and dv.dv_id = os.status
             and tm.region_id = oso.region_id
           group by os.org_id,
                    dog.dog_number,
                    t.tmc_type,
                    tm.serial_number,
                    dv.dv_name,
                    tm.cost_with_nds,
                    omc.mort_cost,
                    tm.name_model
           order by org_id         asc,
                    dog.dog_number,
                    mark,
                    ST_NOMINAL,
                    st_zalog,
                    imsi           asc;
      elsif (pi_tmc_type > 7003) then
        open res2 for
          select distinct os.ORG_ID,
                          pi_tmc_type TMC_TYPE,
                          iptv.SERIAL_NUMBER, -- серийный номер
                          iptv.MAC_ADDRESS,
                          osmi.is_second_hand,
                          decode(iptv.state_id,
                                 1,
                                 'Принято',
                                 2,
                                 'Продано',
                                 3,
                                 'Б/У',
                                 4,
                                 'Брак',
                                 5,
                                 'Утеряно',
                                 6,
                                 'Удалено',
                                 null) STATUS,
                          iptv.stb_model_id MARK_ID,
                          smod.stb_model MARK,
                          dog.DOG_NUMBER,
                          t.tmc_tmp_cost RETAIL_PRICE,
                          t.tmc_tmp_cost SUM_PRICE,
                          1 as CNT,
                          t.tmc_perm,
                          p.prm_name,
                          oso.org_name org_name,
                          orgr.org_id org_pid,
                          nvl(orgr.org_name, oso.org_name) org_pid_name
            from t_tmc_iptv iptv
            join t_ott_stb_model_info osmi
              on osmi.id = iptv.stb_model_id
            join t_stb_model smod
              on smod.id = osmi.model_stb_id
            join t_tmc t
              on t.tmc_id = iptv.tmc_id
            left join t_perm p
              on p.prm_id = t.tmc_perm
            join t_org_tmc_status os
              on os.tmc_id = t.tmc_id
            join t_organizations oso
              on os.org_id = oso.org_id
            join t_org_is_rtmob oir
              on OS.ORG_ID = oir.org_id
            left join t_dic_region dr
              on OS.ORG_ID = dr.org_id
            Join mv_org_tree tor
              on tor.ORG_ID = OS.ORG_ID
             and ((oir.is_org_rtm = 0 and dr.org_id is not null and
                 tor.root_org_pid <> -1) or
                 (oir.is_org_rtm = 0 and dr.org_id is not null and
                 dr.mrf_id <> 7) or
                 (dr.org_id is null and
                 (tor.root_reltype = 1002 or tor.root_org_pid <> -1)))
            left join t_dogovor dog
              on dog.org_rel_id = tor.root_rel_id
             and dog.dog_class_id in (10, 11, 12)
             --and dog.is_enabled = 1
            left join t_dogovor_prm dp
              on dp.dp_dog_id = dog.dog_id
             and dp.dp_is_enabled = 1
            left join t_organizations orgR
              on GetRootOrg(OS.ORG_ID) = orgR.Org_Id
            left join t_org_tmc_status ots
              on ots.tmc_id = t.tmc_id
           where iptv.priznak = pi_tmc_type
             and os.status in (11, 14)
             and (is_org_usi(os.org_id) = 1 or
                 dp.dp_prm_id in (select * from table(l_perm_id)))
             and t.is_deleted = 0
             and (l_count = 0 or
                 os.org_id in
                 (Select distinct t.org_id
                     from mv_org_tree t
                    where t.root_reltype <> 1006
                   Connect by prior t.org_id = t.org_pid
                          and exists
                    (select column_value
                                 from table(l_org_tab)
                                where column_value in
                                      (2001272,
                                       2001280,
                                       2001825,
                                       2001455,
                                       2001433,
                                       2001279,
                                       2001454,
                                       2001825,
                                       1000001,
                                       1000002,
                                       1000003,
                                       1000004,
                                       1000005,
                                       1000006,
                                       1000007,
                                       1000008,
                                       1000009,
                                       1000010)
                                   or t.org_reltype = 1001)
                    Start with t.org_id in
                               (select column_value from table(l_org_tab))
                           and t.org_reltype <> 1006))
             and pi_tmc_type = nvl(iptv.priznak, t.tmc_type)
             and (os.org_id in (select * from table(l_org_tab)))
             and (dog.dog_id is null or dog.dog_id = ots.dog_id)
           order by org_id             asc,
                    dog.dog_number,
                    smod.stb_model,
                    t.tmc_tmp_cost,
                    cnt,
                    iptv.SERIAL_NUMBER asc;
      end if;
      -- 47787
    else
      open res2 for -- Т.к. это только для симок, то курсор под симки
        Select null ORG_ID,
               null TAR_ID,
               null TMC_TYPE,
               null IMSI,
               null CS,
               null sim_callsign_city,
               null STATUS,
               null ACTIVE_STATUS,
               null num_type,
               null sim_color,
               null org_name,
               null org_pid,
               null org_pid_name,
               null title,
               null tariff_type,
               null tariff_type_name,
               null tmc_perm,
               null prm_name,
               null sim_iccid,
               null ver_date_beg,
               null ver_date_end,
               null STOIMOST,
               null sim_type,
               null sim_type_name
          from dual
         where 1 <> 1;
    end if;
    po_details := res2;
    return res;
  exception
    when ex_org_id_is_null then
      po_err_num := 1001;
      po_err_msg := 'Не верно задан ИД организации';
      po_details := null;
      return null;
    when ex_tmc_type_is_null then
      po_err_num := 1002;
      po_err_msg := 'Не верно задан тип ТМЦ';
      po_details := null;
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      po_details := null;
      return null;
  end Get_Report_Ostatki;
  -----------------------------------------------------------------------
  function Get_Report_Move_Counterparts_1(pi_org_id_0  in T_ORGANIZATIONS.ORG_ID%type,
                                          pi_org_id_1  in T_ORGANIZATIONS.ORG_ID%type,
                                          pi_tmc_type  in T_TMC.TMC_TYPE%type,
                                          pi_date_0    in T_TMC_OPERATIONS.OP_DATE%type,
                                          pi_date_1    in T_TMC_OPERATIONS.OP_DATE%type,
                                          pi_worker_id in T_USERS.USR_ID%type,
                                          po_err_num   out pls_integer,
                                          po_err_msg   out t_Err_Msg,
                                          pi_gen_num   in number := 0)
    return sys_refcursor is
    res              sys_refcursor;
    type1            pls_integer := null;
    l_date_0         date := trunc(sysdate);
    l_date_1         date := trunc(sysdate);
    l_gen_doc_number t_Err_Msg := '';
    l_is_root        pls_integer;
    l_is_root1       pls_integer;
  begin
    logging_pkg.debug('pi_org_id_0=' || pi_org_id_0 || ' pi_org_id_1=' ||
                      pi_org_id_1 || ' pi_tmc_type=' || pi_tmc_type ||
                      ' pi_date_0=' || pi_date_0 || ' pi_date_1=' ||
                      pi_date_1 || ' pi_gen_num=' || pi_gen_num ||
                      ' pi_worker_id=' || pi_worker_id,
                      'Get_Report_Move_Counterparts_1');
    if ((pi_org_id_0 is null) or (pi_org_id_0 <= -1)) then
      raise ex_org_id_is_null;
    end if;
    if (not Security_pkg.Check_Rights_str('EISSD.TMC_REPORTS.MOVE_TMC_COUNTERPARTS',
                                          pi_org_id_0,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg,
                                          true,
                                          false)) then
      return null;
    end if;
  
    if (pi_tmc_type = c_tmc_sim_id or pi_tmc_type = c_tmc_ruim_id or
       pi_tmc_type = c_tmc_adsl_card_id
       or pi_tmc_type = c_tmc_modem_id or pi_tmc_type = c_tmc_payd_card_id or
       pi_tmc_type = c_tmc_usb_modem_id
       or pi_tmc_type = c_tmc_ont or pi_tmc_type = c_tmc_stb
       or pi_tmc_type = 7003) then    
      type1 := pi_tmc_type;
    elsif pi_tmc_type > 7003 then
      type1 := 7002;
    end if;
  
    if (pi_date_0 is not null) then
      l_date_0 := trunc(pi_date_0);
    end if;
  
    if (pi_date_1 is not null) then
      l_date_1 := trunc(pi_date_1);
    end if;
  
    if (pi_gen_num = 1) then
      l_gen_doc_number := Get_Dogovor_Number(pi_org_id_0, 8002);
    end if;
  
    if (orgs.Get_Root_Org_Or_Self(pi_org_id_0) = pi_org_id_0) then
      l_is_root := 1;
    else
      l_is_root := 0;
    end if;
    if (orgs.Get_Root_Org_Or_Self(pi_org_id_1) = pi_org_id_1) then
      l_is_root1 := 1;
    else
      l_is_root1 := 0;
    end if;
  
    open res for
      select ttabl.ORG_PID,
             (select OOO.ORG_NAME
                from T_ORGANIZATIONS OOO
               where ttabl.org_pid = OOO.ORG_ID(+)) ORG_NAME,
             ttabl.TMC_TYPE,
             sum(ttabl.polucheno) inc,
             sum(ttabl.Sale) - sum(ttabl.vozvr) + sum(nvl(ott_stb_prod, 0)) sal,
             sum(ttabl.change_tar) cht,
             sum(ttabl.rev_brak) brk,
             sum(ttabl.rev_uter) ute,
             sum(ttabl.rev_izb) izb,
             sum(ttabl.udaleno) del_tmc,
             sum(ttabl.change_sim) change_sim,
             sum(ttabl.reserv_delivery) reserv_delivery,
             l_gen_doc_number DOG_NUMBER
        from (
              ---------------------------- Получено от поставщиков-------------------------------------------------
              select distinct OPU.OWNER_ID_1 org_pid,
                              T.TMC_TYPE TMC_TYPE,
                              count(*) polucheno,
                              0 sale,
                              0 change_tar,
                              0 rev_brak,
                              0 rev_uter,
                              0 rev_izb,
                              0 udaleno,
                              0 change_sim,
                              0 vozvr,
                              0 unit_id,
                              0 ott_stb_prod,
                              0 reserv_delivery
                from T_TMC T
                join T_TMC_OPERATION_UNITS OPU
                  on T.TMC_ID = OPU.TMC_ID
                join T_TMC_OPERATIONS OP
                  on OPU.OP_ID = OP.OP_ID
                left join T_TMC_IPTV IP
                  on IP.TMC_ID = T.TMC_ID
               where (OP.OP_TYPE = 18 or OP.OP_TYPE = 20)
                 and OPU.OWNER_ID_1 = pi_org_id_0
                 and trunc(OP.OP_DATE) <= l_date_1
                 and trunc(OP.OP_DATE) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
                 AND nvl(ip.priznak, t.tmc_type) = PI_TMC_TYPE
               group by OPU.OWNER_ID_1, T.TMC_TYPE
              union all
              ------------------------------------- ПРОДАНО ----------------------------------------------------------
              select distinct pi_org_id_0 ORG_PID /*ORR.ORG_PID*/,
                              T.TMC_TYPE,
                              0 polucheno,
                              count(*) Sale,
                              0 change_tar,
                              0 rev_brak,
                              0 rev_uter,
                              0 rev_izb,
                              0 udaleno,
                              0 change_sim,
                              0 vozvr,
                              opu.unit_id,
                              0 ott_stb_prod,
                              0 reserv_delivery
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     T_ABONENT             AB,
                     T_TMC_IPTV            IP
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE = 22
                 and OP.OP_ID = AB.ID_OP
                 and AB.IS_DELETED = 0
                 and pi_org_id_0 = (case
                       when l_is_root = 0 then
                        AB.ORG_ID
                       else
                        AB.ROOT_ORG_ID
                     end)
                 and trunc(AB.CHANGE_STATUS_DATE) <= l_date_1
                 and trunc(AB.CHANGE_STATUS_DATE) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
                 AND IP.TMC_ID(+) = T.TMC_ID
                 AND (PI_TMC_TYPE < 7002 OR
                     ip.priznak = PI_TMC_TYPE)
               group by pi_org_id_0, T.TMC_TYPE, opu.unit_id
              union all
              -- 57180 Количество отмен продажи/возвратов
              select distinct pi_org_id_0 ORG_PID /*ORR.ORG_PID*/,
                              T.TMC_TYPE,
                              0 polucheno,
                              0 sale,
                              0 change_tar,
                              0 rev_brak,
                              0 rev_uter,
                              0 rev_izb,
                              0 udaleno,
                              0 change_sim,
                              decode(T.TMC_TYPE,
                                     4,
                                     count(*),
                                     7003,
                                     count(*),
                                     0) vozvr,
                              0 unit_id,
                              0 ott_stb_prod,
                              0 reserv_delivery
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     T_ORGANIZATIONS       ORGn,
                     T_TMC_IPTV            IP
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE = 536
                 and pi_org_id_0 = (case
                       when l_is_root = 0 then
                        ORGn.Org_Id
                       else
                        ORGn.Root_Org_Id
                     end)
                 and OPU.OWNER_ID_1 = ORGn.Org_Id
                 and trunc(op.op_date) <= l_date_1
                 and trunc(op.op_date) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
                 AND T.TMC_ID NOT IN
                     (SELECT A.AB_TMC_ID
                        FROM T_ABONENT A
                       WHERE A.IS_DELETED = 1)
                 AND IP.TMC_ID(+) = T.TMC_ID
                 AND (PI_TMC_TYPE < 7002 OR
                     IP.PRIZNAK = PI_TMC_TYPE)
               group by pi_org_id_0, T.TMC_TYPE
              union all
              -----------------------------------------------------------------------------
              -- Продажа OTT-STB
              select distinct pi_org_id_0 ORG_PID,
                              T.TMC_TYPE,
                              0 polucheno,
                              0 Sale,
                              0 change_tar,
                              0 rev_brak,
                              0 rev_uter,
                              0 rev_izb,
                              0 udaleno,
                              0 change_sim,
                              0 vozvr,
                              0 unit_id,
                              count(*) ott_stb_prod,
                              0 reserv_delivery
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     T_TMC_IPTV            TI
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE = 22
                 and OPU.OWNER_ID_0 = pi_org_id_0
                 and trunc(OP.OP_DATE) <= l_date_1
                 and trunc(OP.OP_DATE) >= l_date_0
                 and (type1 = 7002 or type1 is null)
                 AND TI.TMC_ID = T.TMC_ID
                 AND TI.PRIZNAK = PI_TMC_TYPE
                 AND PI_TMC_TYPE = 7004
               group by OPU.OWNER_ID_0, T.TMC_TYPE
              union all
              ---------------------------------------- Сменили тариф  ----------------------------------------------
              select distinct OPU.OWNER_ID_0 ORG_PID /*ORR.ORG_PID*/,
                              T.TMC_TYPE,
                              0 polucheno,
                              0 Sale,
                              count(*) change_tar,
                              0 rev_brak,
                              0 rev_uter,
                              0 rev_izb,
                              0 udaleno,
                              0 change_sim,
                              0 vozvr,
                              0 unit_id,
                              0 ott_stb_prod,
                              0 reserv_delivery
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     T_ORGANIZATIONS       ORGn,
                     T_TMC_IPTV            IP
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE in (26, 1901, 1902) --= 19 --задача 27414
                 and pi_org_id_0 = (case
                       when l_is_root = 0 then
                        ORGn.Org_Id
                       else
                        ORGn.Root_Org_Id
                     end)
                 and OPU.OWNER_ID_0 = ORGn.Org_Id
                 and trunc(OP.OP_DATE) <= l_date_1
                 and trunc(OP.OP_DATE) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
                 AND IP.TMC_ID(+) = T.TMC_ID
                 AND (PI_TMC_TYPE < 7002 OR
                     IP.PRIZNAK = PI_TMC_TYPE)
               group by OPU.OWNER_ID_0, T.TMC_TYPE
              union all
              ---------------------------------------- Ревизия (брак)  --------------------------
              select distinct pi_org_id_0 ORG_PID /*ORR.ORG_PID*/,
                              T.TMC_TYPE,
                              0 polucheno,
                              0 Sale,
                              0 change_tar,
                              count(*) rev_brak,
                              0 rev_uter,
                              0 rev_izb,
                              0 udaleno,
                              0 change_sim,
                              0 vozvr,
                              0 unit_id,
                              0 ott_stb_prod,
                              0 reserv_delivery
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     T_ORGANIZATIONS       ORGn,
                     T_TMC_IPTV            IP
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE = 23
                 and OPU.ST_SKLAD_1 = 14
                 and (pi_org_id_0 = (case
                       when l_is_root = 0 then
                        ORGn.Org_Id
                       else
                        ORGn.Root_Org_Id
                     end))
                 and (OPU.OWNER_ID_0 = ORGn.Org_Id)
                 and trunc(OP.OP_DATE) <= l_date_1
                 and trunc(OP.OP_DATE) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
                 AND IP.TMC_ID(+) = T.TMC_ID
                 AND (PI_TMC_TYPE < 7002 OR
                     PI_TMC_TYPE = IP.PRIZNAK )
               group by pi_org_id_0, T.TMC_TYPE
              union all
              ---------------------------------------- Ревизия (утеряно)  --------------------------
              select distinct pi_org_id_0 ORG_PID /*ORR.ORG_PID*/,
                              T.TMC_TYPE,
                              0 polucheno,
                              0 Sale,
                              0 change_tar,
                              0 rev_brak,
                              count(*) rev_uter,
                              0 rev_izb,
                              0 udaleno,
                              0 change_sim,
                              0 vozvr,
                              0 unit_id,
                              0 ott_stb_prod,
                              0 reserv_delivery
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     T_ORGANIZATIONS       ORGn,
                     T_TMC_IPTV            IP
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE = 23
                 and OPU.ST_SKLAD_1 = 13
                 and (pi_org_id_0 = (case
                       when l_is_root = 0 then
                        ORGn.Org_Id
                       else
                        ORGn.Root_Org_Id
                     end))
                 and (OPU.OWNER_ID_0 = ORGn.Org_Id)
                 and trunc(OP.OP_DATE) <= l_date_1
                 and trunc(OP.OP_DATE) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
                 AND IP.TMC_ID(+) = T.TMC_ID
                 AND (PI_TMC_TYPE < 7002 OR
                     PI_TMC_TYPE =  IP.PRIZNAK)
               group by pi_org_id_0, T.TMC_TYPE
              union all
              ---------------------------------------- Ревизия (избыток)  --------------------------
              select distinct pi_org_id_0 ORG_PID /*ORR.ORG_PID*/,
                              T.TMC_TYPE,
                              0 polucheno,
                              0 Sale,
                              0 change_tar,
                              0 rev_brak,
                              0 rev_uter,
                              count(*) rev_izb,
                              0 udaleno,
                              0 change_sim,
                              0 vozvr,
                              0 unit_id,
                              0 ott_stb_prod,
                              0 reserv_delivery
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     T_ORGANIZATIONS       ORGn,
                     T_TMC_IPTV            IP
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE = 23
                 and OPU.ST_SKLAD_1 = 15
                 and pi_org_id_0 = (case
                       when l_is_root = 0 then
                        ORGn.Org_Id
                       else
                        ORGn.Root_Org_Id
                     end)                    
                 and OPU.OWNER_ID_0 = ORGn.Org_Id
                 and trunc(OP.OP_DATE) <= l_date_1
                 and trunc(OP.OP_DATE) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
                 AND IP.TMC_ID(+) = T.TMC_ID
                 AND (PI_TMC_TYPE < 7002 OR
                     PI_TMC_TYPE = IP.PRIZNAK)
               group by pi_org_id_0, T.TMC_TYPE
              union all
              ---------------------------------------- Удалено  --------------------------
              select distinct pi_org_id_0 ORG_PID /*ORR.ORG_PID*/,
                              T.TMC_TYPE,
                              0 polucheno,
                              0 Sale,
                              0 change_tar,
                              0 rev_brak,
                              0 rev_uter,
                              0 rev_izb,
                              count(*) udaleno,
                              0 change_sim,
                              0 vozvr,
                              0 unit_id,
                              0 ott_stb_prod,
                              0 reserv_delivery
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     T_ORGANIZATIONS       ORGn,
                     T_TMC_IPTV            IP
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE = 530
                 and OPU.ST_SKLAD_1 = 216
                    -- 40076
                 and opu.st_sklad_0 <> 13
                 and pi_org_id_0 = (case
                       when l_is_root = 0 then
                        ORGn.Org_Id
                       else
                        ORGn.Root_Org_Id
                     end)
                 and OPU.OWNER_ID_0 = ORGn.Org_Id
                 and trunc(OP.OP_DATE) <= l_date_1
                 and trunc(OP.OP_DATE) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
                 AND IP.TMC_ID(+) = T.TMC_ID
                 AND (PI_TMC_TYPE < 7002 OR
                     PI_TMC_TYPE = IP.PRIZNAK)
               group by pi_org_id_0, T.TMC_TYPE
              union all
              ---------------------------------------- Замена сим-карты  --------------------------
              select distinct pi_org_id_0 ORG_PID /*ORR.ORG_PID*/,
                              T.TMC_TYPE,
                              0 polucheno,
                              0 Sale,
                              0 change_tar,
                              0 rev_brak,
                              0 rev_uter,
                              0 rev_izb,
                              0 udaleno,
                              count(*) change_sim,
                              0 vozvr,
                              0 unit_id,
                              0 ott_stb_prod,
                              0 reserv_delivery
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     T_ORGANIZATIONS       ORGn,
                     T_TMC_IPTV            IP
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE = 529
                 and pi_org_id_0 = (case
                       when l_is_root = 0 then
                        ORGn.Org_Id
                       else
                        ORGn.Root_Org_Id
                     end)
                 and OPU.OWNER_ID_0 = ORGn.Org_Id
                 and trunc(OP.OP_DATE) <= l_date_1
                 and trunc(OP.OP_DATE) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
                 AND IP.TMC_ID(+) = T.TMC_ID
                 AND (PI_TMC_TYPE < 7002 OR
                     PI_TMC_TYPE = IP.PRIZNAK)
               group by pi_org_id_0, T.TMC_TYPE
              union all
              --------------------------- Забронировано для доставки  --------------------------
              select distinct pi_org_id_0 ORG_PID,
                              T.TMC_TYPE,
                              0 polucheno,
                              0 Sale,
                              0 change_tar,
                              0 rev_brak,
                              0 rev_uter,
                              0 rev_izb,
                              0 udaleno,
                              0 change_sim,
                              0 vozvr,
                              0 unit_id,
                              0 ott_stb_prod,
                              count(*) reserv_delivery
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     T_ORGANIZATIONS       ORGn
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE = 545
                 and pi_org_id_0 = (case
                       when l_is_root = 0 then
                        ORGn.Org_Id
                       else
                        ORGn.Root_Org_Id
                     end)
                 and OPU.OWNER_ID_0 = ORGn.Org_Id
                 and trunc(OP.OP_DATE) <= l_date_1
                 and trunc(OP.OP_DATE) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
               group by pi_org_id_0, T.TMC_TYPE) ttabl
       group by ttabl.tmc_type, ttabl.org_pid;
    return res;
  exception
    when ex_org_id_is_null then
      po_err_num := 1001;
      po_err_msg := 'Не верно задан ИД организации';
      return null;
    when ex_tmc_type_is_null then
      po_err_num := 1002;
      po_err_msg := 'Не верно задан тип ТМЦ';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
  end Get_Report_Move_Counterparts_1;
  -----------------------------------------------------------------------
  function Get_Report_Move_Counterparts_2(pi_org_id_0  in T_ORGANIZATIONS.ORG_ID%type,
                                          pi_org_id_1  in T_ORGANIZATIONS.ORG_ID%type,
                                          pi_tmc_type  in T_TMC.TMC_TYPE%type,
                                          pi_date_0    in T_TMC_OPERATIONS.OP_DATE%type,
                                          pi_date_1    in T_TMC_OPERATIONS.OP_DATE%type,
                                          pi_worker_id in T_USERS.USR_ID%type,
                                          po_err_num   out pls_integer,
                                          po_err_msg   out t_Err_Msg)
    return sys_refcursor is
    res      sys_refcursor;
    type1    pls_integer := null;
    l_date_0 date := trunc(sysdate);
    l_date_1 date := trunc(sysdate);
  begin
    logging_pkg.debug('pi_org_id_0=' || pi_org_id_0 || ' pi_org_id_1=' ||
                      pi_org_id_1 || ' pi_tmc_type=' || pi_tmc_type ||
                      ' pi_date_0=' || pi_date_0 || ' pi_date_1=' ||
                      pi_date_1 || ' pi_worker_id=' || pi_worker_id,
                      'Get_Report_Move_Counterparts_2');
    if ((pi_org_id_0 is null) or (pi_org_id_0 <= -1)) then
      raise ex_org_id_is_null;
    end if;
    if (not Security_pkg.Check_Rights_str('EISSD.TMC_REPORTS.MOVE_TMC_COUNTERPARTS',
                                          pi_org_id_0,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg,
                                          true,
                                          false)) then
      return null;
    end if;
  
    if (pi_tmc_type = c_tmc_sim_id or pi_tmc_type = c_tmc_ruim_id or
       pi_tmc_type = c_tmc_adsl_card_id or
       pi_tmc_type = c_tmc_usb_modem_id
       --
       or pi_tmc_type = c_tmc_modem_id or pi_tmc_type = c_tmc_payd_card_id
       --
       --neveroff 16.02.2011 FTTx, IPTV
       or pi_tmc_type = c_tmc_ont or pi_tmc_type = c_tmc_stb
       -- 39105
       or pi_tmc_type = 7003) then
      type1 := pi_tmc_type;
    elsif pi_tmc_type > 7003 then
      type1 := 7002;
    end if;
  
    if (pi_date_0 is not null) then
      l_date_0 := trunc(pi_date_0);
    end if;
  
    if (pi_date_1 is not null) then
      l_date_1 := trunc(pi_date_1);
    end if;
  
    open res for
      select ttabl.ORG2_ID,
             (select OOO.ORG_NAME
                from T_ORGANIZATIONS OOO
               where ttabl.org2_id = OOO.ORG_ID(+)) ORG_NAME,
             ttabl.TMC_TYPE,
             sum(ttabl.Transf) - nvl(sum(ttabl.dil_move), 0) tra,
             sum(ttabl.Back) - nvl(sum(ttabl.dil_back), 0) bac,
             sum(ttabl.dil_move) dil_move,
             sum(ttabl.dil_back) dil_back
        from (select ORR.ORG_ID org2_id,
                     T.TMC_TYPE,
                     count(*) Transf,
                     0 Back,
                     0 dil_move,
                     0 dil_back
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     mv_org_tree           ORR,
                     T_TMC_IPTV            IPTV
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE = 20
                 and ORR.ORG_PID = pi_org_id_0
                 and ((pi_org_id_1 is null) or (ORR.ORG_ID = pi_org_id_1))
                 and ORR.ORG_ID = OPU.OWNER_ID_1
                 and ORR.ORG_PID = OPU.OWNER_ID_0
                 and trunc(OP.OP_DATE) <= l_date_1
                 and trunc(OP.OP_DATE) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
                 AND IPTV.TMC_ID(+) = T.TMC_ID
                 AND (PI_TMC_TYPE < 7002 OR
                     PI_TMC_TYPE = IPTV.PRIZNAK)
               group by ORR.ORG_ID, T.TMC_TYPE
              union all
              ----------------------------------------------------------------------
              -- OTT-STB Продано контрагенту
              select ORR.ORG_ID org2_id,
                     T.TMC_TYPE,
                     0 Transf,
                     0 Back,
                     count(*) dil_move,
                     0 dil_back
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     mv_org_tree           ORR,
                     T_TMC_IPTV            IPTV
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE = 541
                 and ORR.ORG_PID = pi_org_id_0
                 and ((pi_org_id_1 is null) or (ORR.ORG_ID = pi_org_id_1))
                 and ORR.ORG_ID = OPU.OWNER_ID_1
                 and ORR.ORG_PID = OPU.OWNER_ID_0
                 and trunc(OP.OP_DATE) <= l_date_1
                 and trunc(OP.OP_DATE) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
                 AND IPTV.TMC_ID(+) = T.TMC_ID
                 AND (PI_TMC_TYPE < 7002 OR
                     PI_TMC_TYPE = IPTV.PRIZNAK)
               group by ORR.ORG_ID, T.TMC_TYPE
              union all
              -----------------------------------------------------------------------------------------------
              select ORR.ORG_ID org2_id,
                     T.TMC_TYPE,
                     0 Transf,
                     count(*) Back,
                     0 dil_move,
                     0 dil_back
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     mv_org_tree           ORR,
                     T_TMC_IPTV            IPTV
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE in (21, 536, 543, 544)
                 and ORR.ORG_PID = pi_org_id_0
                 and ((pi_org_id_1 is null) or (ORR.ORG_ID = pi_org_id_1))
                 and ORR.ORG_ID = OPU.OWNER_ID_0
                 and ORR.ORG_PID = OPU.OWNER_ID_1
                 and trunc(OP.OP_DATE) <= l_date_1
                 and trunc(OP.OP_DATE) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
                 AND IPTV.TMC_ID(+) = T.TMC_ID
                 AND (PI_TMC_TYPE < 7002 OR
                     PI_TMC_TYPE = IPTV.PRIZNAK)
               group by ORR.ORG_ID, T.TMC_TYPE
              union all
              ----------------------------------------------------------------------
              -- OTT-STB Выкуплено у контрагента
              select ORR.ORG_ID org2_id,
                     T.TMC_TYPE,
                     0 Transf,
                     0 Back,
                     0 dil_move,
                     count(*) dil_back
                from T_TMC                 T,
                     T_TMC_OPERATION_UNITS OPU,
                     T_TMC_OPERATIONS      OP,
                     mv_org_tree           ORR,
                     T_TMC_IPTV            IPTV
               where T.TMC_ID = OPU.TMC_ID
                 and OPU.OP_ID = OP.OP_ID
                 and OP.OP_TYPE = 542
                 and ORR.ORG_PID = pi_org_id_0
                 and ((pi_org_id_1 is null) or (ORR.ORG_ID = pi_org_id_1))
                 and ORR.ORG_ID = OPU.OWNER_ID_0
                 and ORR.ORG_PID = OPU.OWNER_ID_1
                 and trunc(OP.OP_DATE) <= l_date_1
                 and trunc(OP.OP_DATE) >= l_date_0
                 and ((T.TMC_TYPE = type1) or (type1 is null))
                 AND IPTV.TMC_ID(+) = T.TMC_ID
                 AND (PI_TMC_TYPE < 7002 OR
                     PI_TMC_TYPE = IPTV.PRIZNAK)
               group by ORR.ORG_ID, T.TMC_TYPE) ttabl
       group by ttabl.org2_id, ttabl.tmc_type;
    return res;
  exception
    when ex_org_id_is_null then
      po_err_num := 1001;
      po_err_msg := 'Не верно задан ИД организации';
      return null;
    when ex_tmc_type_is_null then
      po_err_num := 1002;
      po_err_msg := 'Не верно задан тип ТМЦ';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
  end Get_Report_Move_Counterparts_2;

  ------------------------------------------------------------
  function Get_Report_Active_TMC(pi_tmc_type       in T_TMC.TMC_TYPE%type,
                                 pi_date_0         in date,
                                 pi_date_1         in date,
                                 pi_region_id      in t_dic_region.REG_ID%type,
                                 pi_with_ab_at_cfe in T_CARD_STATUS_CHANGE.CRD_REQUEST%type,
                                 pi_worker_id      in T_USERS.USR_ID%type,
                                 po_err_num        out pls_integer,
                                 po_err_msg        out t_Err_Msg,
                                 pi_gen_num        in number := 0)
    return sys_refcursor is
    res              sys_refcursor;
    type1            pls_integer := null;
    l_gen_doc_number t_Err_Msg := '';
  begin
    if (not Security_pkg.Check_User_Right_str('EISSD.TMC_REPORT.ACTIVE_TMC',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;
    if (pi_tmc_type = c_tmc_sim_id or pi_tmc_type = c_tmc_ruim_id or
       pi_tmc_type = c_tmc_adsl_card_id or
       pi_tmc_type = c_tmc_payd_card_id) then
      type1 := pi_tmc_type;
    else
      po_err_num := 1;
      po_err_msg := 'Неверный тип ТМЦ.';
      return null;
    end if;
  
    if (pi_gen_num = 1) then
      l_gen_doc_number := Get_Dogovor_Number(null, 8008);
    end if;
  
    if (pi_tmc_type = 6) then
      open res for
        select distinct tc.card_number crd_imsi,
                        (case
                          when csc2.card_active_state = 1601 then
                           1
                          else
                           0
                        end) CRD_STATUS,
                        csc2.card_change_date CRD_CH_DATE,
                        NULL CRD_PHONE,
                        NULL CRD_REGION,
                        NULL CRD_REQUEST,
                        t.tmc_id TMC_ID,
                        NULL AB_ID,
                        /*tc.real_owner_id CRD_ORG_ID,*/
                        t.org_id CRD_ORG_ID,
                        org.org_name CRD_ORG_NAME,
                        l_gen_doc_number DOG_NUMBER,
                        null crd_asr_date,
                        decode(t.tmc_id,
                               null,
                               'Нет в ЕИССД',
                               decode( /*tc.real_owner_id,*/t.org_id,
                                      null,
                                      'Продана',
                                      'На складе')) org_status
          from (select csc.tmc_id, max(csc.card_change_date) crd_ch_date
                  from t_paydcard_status_change CSC
                 where trunc(csc.card_change_date) >= pi_date_0
                   and trunc(csc.card_change_date) <= pi_date_1
                 group by csc.card_full_id) TAB
          join t_paydcard_status_change CSC2
            on tab.tmc_id = csc2.tmc_id
           and tab.crd_ch_date = csc2.card_change_date
          join t_tmc_pay_card tc
            on tab.tmc_id = tc.tmc_id
          join t_tmc t
            on tc.tmc_id = t.tmc_id
           and t.is_deleted = 0
          left join t_organizations org
            on /*tc.real_owner_id*/
         t.org_id = org.org_id
         where t.tmc_type = type1
         order by trunc(csc2.card_change_date) asc;
    else
      open res for
        select csc.crd_imsi,
               (case
                 when csc.crd_status = 1501 then
                  1
                 else
                  0
               end) crd_status,
               csc.crd_ch_date,
               csc.crd_phone,
               csc.crd_region,
               nvl2(a.remote_id, 1, 0) crd_request,
               t.tmc_id,
               a.ab_id,
               /*ts.real_owner_id crd_org_id,*/
               t.org_id crd_org_id,
               o.org_name crd_org_name,
               l_gen_doc_number DOG_NUMBER,
               a.ab_reg_date crd_asr_date,
               decode(t.tmc_id,
                      null,
                      'Нет в ЕИССД',
                      decode( /*ts.real_owner_id,*/t.org_id,
                             null,
                             'Продана',
                             'На складе')) org_status
          from t_card_status_change csc,
               t_tmc_sim            ts,
               t_tmc                t,
               t_org_tmc_status     ots,
               t_abonent            a,
               t_organizations      o
         where ts.sim_imsi = csc.crd_imsi
           and t.tmc_id = ts.tmc_id
           and t.tmc_type = 8
           and t.is_deleted = 0
           and t.tmc_id = ots.tmc_id
           and csc.crd_status = 1501
           and trunc(csc.crd_ch_date) >= pi_date_0
           and trunc(csc.crd_ch_date) <= pi_date_1
           and (pi_region_id is null or pi_region_id = csc.crd_region)
           and t.tmc_id = a.ab_tmc_id(+)
           and /*ts.real_owner_id*/
               t.org_id = o.org_id(+);
    end if;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
  end Get_Report_Active_TMC;
  -----------------------------------------------------------------------
  function Get_Dogovor_Number(pi_id_org      in t_organizations.org_id%type,
                              pi_id_type_dog in number) return t_Err_Msg is
    l_id_org  t_organizations.org_id%type;
    l_cur_val number;
    l_prefix  t_organizations.prefix%type := '';
    res       t_Err_Msg;
  begin
    l_id_org := pi_id_org;
    if (pi_id_org = -1) then
      l_id_org := null;
    end if;
  
    begin
      select t.cur_val
        into l_cur_val
        from t_num_seq_dog t
       where ((l_id_org is null and t.id_org is null) or
             (t.id_org = l_id_org))
         and t.id_type_dog = pi_id_type_dog;
    exception
      when NO_DATA_FOUND then
        l_cur_val := 1;
        insert into t_num_seq_dog
          (id_org, id_type_dog, cur_val)
        values
          (l_id_org, pi_id_type_dog, l_cur_val);
    end;
    if (pi_id_org is not null) then
      begin
        select t.prefix
          into l_prefix
          from t_organizations t
         where t.org_id = pi_id_org
           and t.prefix is not null;
      exception
        when NO_DATA_FOUND then
          l_prefix := '';
      end;
    end if;
    --   if (l_prefix is not null) then l_prefix:=l_prefix||'-'; end if;
    case
      when pi_id_type_dog = 8011 then
        res := l_prefix || '-' || TO_CHAR(l_cur_val) || 'п';
      when pi_id_type_dog = 8012 then
        res := l_prefix || '-' || TO_CHAR(l_cur_val) || 'б';
      when pi_id_type_dog = 8013 then
        res := l_prefix || '-' || TO_CHAR(l_cur_val) || 'у';
      Else
        res := l_prefix || ' №' || TO_CHAR(l_cur_val);
    End Case;
    update t_num_seq_dog t
       set t.cur_val = l_cur_val + 1
     where (t.id_org = l_id_org or l_id_org is null)
       and t.id_type_dog = pi_id_type_dog;
    return res;
  end Get_Dogovor_Number;
  -----------------------------------------------------------------------
  -- Возвращает шахматку по счетам за период (ДДС)
  -----------------------------------------------------------------------
  function Get_Acc_Shahmatka(pi_org_pid   in t_organizations.org_id%type,
                             pi_org_id    in t_organizations.org_id%type,
                             pi_Date_Top  in t_acc_transfer.tr_date%type,
                             pi_Date_End  in t_acc_transfer.tr_date%type,
                             pi_worker_id in T_USERS.USR_ID%type,
                             pi_dog_id    in t_dogovor.dog_id%type,
                             po_err_num   out pls_integer,
                             po_err_msg   out t_Err_Msg) return sys_refcursor is
    res        sys_refcursor;
    l_Date_Top date;
    l_Date_End date;
    l_dog_id   number;
    l_over_sum number; --сумма овердрафта
  begin
    logging_pkg.debug(pi_dog_id || ' pi_Date_Top=' || pi_Date_Top ||
                      ' pi_Date_End=' || pi_Date_End,
                      'Get_Acc_Shahmatka');
    if pi_dog_id = -1 then
      l_dog_id := null;
    else
      l_dog_id := pi_dog_id;
    end if;
    -- вычислим сумму овердрафта
    select sum(tas.overdraft)
      into l_over_sum
      from t_accounts ta
      join t_acc_owner tao
        on tao.owner_id = ta.acc_owner_id
      join t_acc_schema tas
        on tas.id_acc = ta.acc_id
     where ((l_dog_id is not null and tao.owner_ctx_id = l_dog_id) or
           (l_dog_id is null and
           tao.owner_ctx_id in
           (select td.dog_id
                from t_org_relations tor, t_dogovor td
               where tor.org_pid = pi_org_pid
                 and tor.org_id = pi_org_id
                 and tor.id = td.org_rel_id)));
    If pi_Date_Top is Null then
      l_Date_Top := To_Date('01.01.2000', 'DD.MM.YYYY');
    Else
      l_Date_Top := trunc(pi_Date_Top);
    End If;
    If pi_Date_End is Null then
      l_Date_End := Sysdate;
    Else
      l_Date_End := pi_Date_End;
    End If;
  
    open res for
      Select -- t.dog_id, -- доп.поле
       ttable,
       Max(org_name) org_name,
       max(dv_name) dv_name,
       tr_Date,
       sum(decode(In_Out, 2, 0, amount)) amount,
       max(Decode(In_Out, 0, CH, '')) CH_Out,
       max(Decode(In_Out, 1, CH, '')) CH_In,
       max(callsign_1) callsign,
       max(Decode(dv, 5001, Saldo, Null)) Lic_Ch,
       max(Decode(dv, 5001, Saldo + l_over_sum, Null)) Lic_Ch_over, -- с учётом овердрафта
       max(Decode(dv, 5004, Saldo, Null)) Rez_Ch,
       max(Decode(dv, 5006, Saldo, Null)) Uter_Ch,
       max(Decode(dv, 5007, Saldo, Null)) Plata_Ch,
       max(Decode(dv, 5008, Saldo, Null)) Avans_Ch /**/
        From (Select Pk_Acc,
                     org_name,
                     NVL(dv_name, 'корректировка') ||
                     NVL2(tr_cause, ' (' || tr_cause || ')', '') dv_name,
                     tr_Date,
                     amount,
                     CH,
                     callsign_1,
                     Dv,
                     In_Out,
                     tTable,
                     sum(decode(In_Out, 0, -1, 1) * amount) over(Partition by dv order by tr_Date, ttable, Pk_Acc) Saldo
                From (select tap.pay_id Pk_Acc,
                             org.org_name,
                             dv_op.dv_name,
                             tap.pay_date tr_Date,
                             tap.amount,
                             dv.Dv_Name CH,
                             null callsign_1,
                             dv.Dv_Id Dv,
                             Decode(tap.tr_type, 6007, 0, 1) In_Out,
                             0 tTable,
                             null tr_cause
                        from t_acc_pay tap
                        Left Join t_Dic_Values dv_Op
                          on dv_Op.Dv_Id = tap.tr_type
                        Join t_Accounts acc
                          on tap.acc_id = acc.acc_id
                        Join t_Dic_Values dv
                          on dv.Dv_Id = acc.acc_type
                        Join t_acc_owner tao
                          on tao.owner_id = acc.acc_owner_id
                        left join t_Dogovor dog
                          on dog.dog_id = tao.owner_ctx_id
                            --
                         and ((l_dog_id is not null and dog.dog_id = l_dog_id) or
                             (l_dog_id is null and
                             dog.dog_id in
                             (select td.dog_id
                                  from t_org_relations tor, t_dogovor td
                                 where tor.org_pid = pi_org_pid
                                   and tor.org_id = pi_org_id
                                   and tor.id = td.org_rel_id)))
                      --
                        Join t_Org_Relations tor
                          on tor.id = dog.org_rel_id
                        Join t_organizations org
                          on tor.org_id = org.org_id
                         and tor.org_reltype in (1003, 1004, 1007, 999)
                       Where /**/ /*((l_dog_id is null and org.org_id = l_Org_Id)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  or l_dog_id is not null)*/
                      /*Trunc*/
                       (tap.pay_date) between l_Date_Top and l_Date_End
                      Union All
                      Select Decode(tat.tr_type,
                                    6029,
                                    tat.tr_id,
                                    tat.tmc_op_id) pk_Acc,
                             (org.org_name) org_name,
                             --                             (dv_op.dv_name) dv_name,
                             decode(dv_op.dv_name,
                                    null,
                                    dv_op2.dv_name,
                                    dv_op.dv_name) dv_name,
                             tat.tr_date,
                             (tat.amount) amount,
                             (dv.Dv_Name) CH,
                             null /*(tou.callsign_1)*/ callsign_1,
                             (dv.Dv_Id) Dv,
                             2 In_Out,
                             1 tTable,
                             tat.tr_cause
                        from t_acc_transfer tat
                        left Join t_tmc_operations tto
                          on tat.tmc_op_id = tto.op_id
                        Left Join t_dic_values dv_Op
                          on tto.op_type = dv_op.dv_id
                        left join t_dic_values dv_op2
                          on dv_op2.dv_id = tat.tr_type
                        Join t_Accounts acc
                          on tat.dst_acc_id = acc.acc_id
                        Join t_dic_values dv
                          on acc.Acc_Type = dv.dv_id
                        Join t_acc_owner tao
                          on (tao.owner_id = acc.acc_owner_id)
                        left join t_Dogovor dog
                          on dog.dog_id = tao.owner_ctx_id
                            --
                         and ((l_dog_id is not null and dog.dog_id = l_dog_id) or
                             (l_dog_id is null and
                             dog.dog_id in
                             (select td.dog_id
                                  from t_org_relations tor, t_dogovor td
                                 where tor.org_pid = pi_org_pid
                                   and tor.org_id = pi_org_id
                                   and tor.id = td.org_rel_id)))
                      --
                        Join t_Org_Relations tor
                          on tor.id = dog.org_rel_id
                        Join t_organizations org
                          on tor.org_id = org.org_id
                         and tor.org_reltype in (1003, 1004, 1007, 999)
                      /*Left Join t_Tmc_Operation_Units tou on tou.op_id =
                          tto.op_id
                      and tto.op_type = 22*/
                       Where /*Trunc*/
                       (tat.tr_date) between l_Date_Top + 2 / 24 and
                       l_Date_End + 2 / 24
                      
                      Union All
                      Select Decode(tat.tr_type,
                                    6029,
                                    tat.tr_id,
                                    tat.tmc_op_id) pk_Acc,
                             (org.org_name) org_name,
                             --                             (dv_op.dv_name) dv_name,
                             decode(dv_op.dv_name,
                                    null,
                                    dv_op2.dv_name,
                                    dv_op.dv_name) dv_name,
                             tat.tr_date,
                             (tat.amount) amount,
                             (dv.Dv_Name) CH,
                             null /*(tou.callsign_1)*/ callsign_1,
                             (dv.Dv_Id) Dv,
                             0 In_Out,
                             1 tTable,
                             tat.tr_cause
                        from t_acc_transfer tat
                        Left Join t_tmc_operations tto
                          on tat.tmc_op_id = tto.op_id
                        Left Join t_dic_values dv_Op
                          on tto.op_type = dv_op.dv_id
                        left join t_dic_values dv_op2
                          on dv_op2.dv_id = tat.tr_type
                        Join t_Accounts acc
                          on tat.src_acc_id = acc.acc_id
                        Left Join t_dic_values dv
                          on acc.Acc_Type = dv.dv_id
                        Join t_acc_owner tao
                          on (tao.owner_id = acc.acc_owner_id)
                        left join t_Dogovor dog
                          on dog.dog_id = tao.owner_ctx_id
                            --
                         and ((l_dog_id is not null and dog.dog_id = l_dog_id) or
                             (l_dog_id is null and
                             dog.dog_id in
                             (select td.dog_id
                                  from t_org_relations tor, t_dogovor td
                                 where tor.org_pid = pi_org_pid
                                   and tor.org_id = pi_org_id
                                   and tor.id = td.org_rel_id)))
                      --
                        Join t_Org_Relations tor
                          on tor.id = dog.org_rel_id
                        Join t_organizations org
                          on tor.org_id = org.org_id
                         and tor.org_reltype in (1003, 1004, 1007, 999)
                      /*Left Join t_Tmc_Operation_Units tou on tou.op_id =
                          tto.op_id
                      and tto.op_type = 22*/
                       Where /*Trunc*/
                       (tat.tr_date) between l_Date_Top + 2 / 24 and
                       l_Date_End + 2 / 24)) t
      
       Group by tr_Date, ttable, Pk_Acc
       order by tr_Date, ttable, Pk_Acc;
    return res;
  
  exception
    when ex_org_id_is_null then
      po_err_num := 1001;
      po_err_msg := 'Неверно задан ИД договора.';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      /*open res for
      select null from dual where 1 <> 1;*/
    
      return null;
  End Get_Acc_Shahmatka;

  --------------------------------------------------------------
  -- Возвращает шахматку по лицевому счету за период (ДДС)
  --------------------------------------------------------------
  function Get_Acc_Person(pi_org_Id    in t_organizations.org_id%type,
                          pi_Date_Top  in t_acc_transfer.tr_date%type,
                          pi_Date_End  in t_acc_transfer.tr_date%type,
                          pi_worker_id in T_USERS.USR_ID%type,
                          po_err_num   out pls_integer,
                          po_err_msg   out t_Err_Msg) return sys_refcursor is
    res        sys_refcursor;
    l_Date_Top date;
    l_Date_End date;
  begin
    if ((pi_org_id is null) or (pi_org_id <= -1)) then
      raise ex_org_id_is_null;
    end if;
    -- Проверка прав доступа
    if (not Security_pkg.Check_Rights_str('EISSD.ACCOUNTS.REPORT.MOVE_FUNDS',
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      return null;
    end if;
  
    If pi_Date_Top is Null then
      l_Date_Top := To_Date('01.01.2000', 'DD.MM.YYYY');
    Else
      l_Date_Top := pi_Date_Top;
    End If;
    If pi_Date_End is Null then
      l_Date_End := Sysdate;
    Else
      l_Date_End := pi_Date_End;
    End If;
    /*   Top_Salo:=report.Get_Acc_Saldo(pi_org_Id => pi_org_Id,
    pi_Date => l_Date_Top,
    pi_worker_id => pi_worker_id,
    po_err_num => po_err_num,
    po_err_msg => po_err_msg); /**/
    Open Res for
      Select org_name,
             Saldo_Top,
             Saldo_Top_Over, -- сальдо с учётом схемы счёта
             op_6000,
             op_6016,
             op_6018,
             op_6023,
             op_6025,
             op_6026,
             (NVL(op_Prihod, 0) -
             (NVL(op_6000, 0) + NVL(op_6016, 0) + NVL(op_6018, 0) +
             NVL(op_6023, 0) + NVL(op_6025, 0) + NVL(op_6026, 0) -
             NVL(op_6027, 0) - NVL(op_6028, 0))) Op_PrihodOther,
             
             (NVL(op_prihod, 0) + NVL(op_6027, 0) + NVL(op_6028, 0)) op_prihod,
             op_6003,
             op_6007,
             op_6011,
             op_6012,
             op_6024,
             (op_6027 + op_6028),
             (NVL(op_Rashod, 0) -
             (NVL(op_6003, 0) + NVL(op_6007, 0) + NVL(op_6011, 0) +
             NVL(op_6012, 0) + NVL(op_6024, 0))) op_RashodOther,
             (NVL(op_Rashod, 0) + NVL(op_6027, 0) + NVL(op_6028, 0)) op_Rashod,
             ((NVL(op_Prihod, 0)) - (NVL(op_Rashod, 0)) + NVL(Saldo_Top, 0)) Saldo_End,
             ((NVL(op_Prihod, 0)) - (NVL(op_Rashod, 0)) + NVL(Saldo_Top, 0) +
             nvl(Saldo_Top_Over, 0) - nvl(Saldo_Top, 0) /*2 последних слагаемых дают величину овердрафта*/
             ) Saldo_End_Over
        From (Select org_Id,
                     Max(org_name) org_name,
                     /*02.07.09*/
                     acc_operations.Get_acc_Saldo2(dog_id, l_Date_Top, 5001) Saldo_Top,
                     acc_operations.Get_Acc_Lic_Saldo_Schema(dog_id,
                                                             l_Date_Top) Saldo_Top_Over,
                     /*02.07.09*/
                     Sum(Decode(Dv_Id, 6000, amount, Null)) op_6000,
                     Sum(Decode(Dv_Id, 6016, amount, Null)) op_6016,
                     Sum(Decode(Dv_Id, 6018, amount, Null)) op_6018,
                     Sum(Decode(Dv_Id, 6023, amount, Null)) op_6023,
                     Sum(Decode(Dv_Id, 6025, amount, Null)) op_6025,
                     Sum(Decode(Dv_Id, 6026, amount, Null)) op_6026,
                     Sum(Decode(In_Out, 1, amount, Null)) op_Prihod,
                     Sum(Decode(Dv_Id, 6003, amount, Null)) op_6003,
                     Sum(Decode(Dv_Id, 6007, amount, Null)) op_6007,
                     Sum(Decode(Dv_Id, 6011, amount, Null)) op_6011,
                     Sum(Decode(Dv_Id, 6012, amount, Null)) op_6012,
                     Sum(Decode(Dv_Id, 6024, amount, Null)) op_6024,
                     -1 * nvl(Sum(Decode(Dv_Id, 6027, amount, Null)), 0) op_6027,
                     -1 * nvl(Sum(Decode(Dv_Id, 6028, amount, Null)), 0) op_6028,
                     Sum(Decode(In_Out, 0, amount, Null)) op_Rashod
                from (select org.org_Id,
                             org.org_name,
                             dv_op.dv_id,
                             tap.amount,
                             dog.dog_id,
                             Decode(tap.tr_type, 6007, 0, 1) In_Out
                        from t_acc_pay tap
                        Left Join t_Dic_Values dv_Op
                          on dv_Op.Dv_Id = tap.tr_type
                      
                        Join t_Accounts acc
                          on tap.acc_id = acc.acc_id
                        Join t_acc_owner tao
                          on (tao.owner_id = acc.acc_owner_id)
                        join t_Dogovor dog
                          on dog.dog_id = tao.owner_ctx_id
                        Join t_Org_Relations tor
                          on tor.id = dog.org_rel_id
                        Join t_organizations org
                          on tor.org_id = org.org_id
                         and tor.org_reltype in (c_rel_tp_dlr_sl, 999)
                      
                       Where tor.org_pid = Pi_Org_Id
                         and Trunc(tap.pay_date /* - 2 / 24*/) between
                             l_Date_Top and l_Date_End
                         and acc.acc_type = 5001
                      
                      Union All
                      Select org.org_Id,
                             org.org_name,
                             dv_op.dv_id,
                             tat.amount,
                             dog.dog_id,
                             1 In_Out
                        from t_acc_transfer tat
                        Left Join t_dic_values dv_Op
                          on tat.Tr_type = dv_op.dv_id
                      
                        Join t_Accounts acc
                          on tat.dst_acc_id = acc.acc_id
                        Join t_acc_owner tao
                          on (tao.owner_id = acc.acc_owner_id)
                        join t_Dogovor dog
                          on dog.dog_id = tao.owner_ctx_id
                        Join t_Org_Relations tor
                          on tor.id = dog.org_rel_id
                        Join t_organizations org
                          on tor.org_id = org.org_id
                         and tor.org_reltype in (c_rel_tp_dlr_sl, 999)
                      
                       Where tor.org_pid = Pi_Org_Id
                         and acc.acc_type = 5001
                         and Trunc(tat.tr_date - 2 / 24) between l_Date_Top and
                             l_Date_End
                      
                      Union All
                      Select org.org_Id,
                             org.org_name,
                             dv_op.dv_id,
                             tat.amount,
                             dog.dog_id,
                             0 In_Out
                        from t_acc_transfer tat
                        Left Join t_dic_values dv_Op
                          on tat.Tr_type = dv_op.dv_id
                      
                        Join t_Accounts acc
                          on tat.src_acc_id = acc.acc_id
                        Join t_acc_owner tao
                          on (tao.owner_id = acc.acc_owner_id)
                        join t_Dogovor dog
                          on dog.dog_id = tao.owner_ctx_id
                        Join t_Org_Relations tor
                          on tor.id = dog.org_rel_id
                        Join t_organizations org
                          on tor.org_id = org.org_id
                         and tor.org_reltype in (c_rel_tp_dlr_sl, 999)
                      
                       Where tor.org_pid = Pi_Org_Id
                         and acc.acc_type = 5001
                         and Trunc(tat.tr_date - 2 / 24) between l_Date_Top and
                             l_Date_End)
               Group by org_Id, dog_id)
       Order By org_name;
  
    return Res;
  exception
    when ex_dog_id_is_null then
      po_err_num := 1001;
      po_err_msg := 'Не верно задан ИД договора!';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      /*open res for
      select null from dual where 1 <> 1;*/
    
      return null;
  End;

  --------------------------------------------------------------
  -- Возвращает шахматку по резервному счету за период (ДДС)
  --------------------------------------------------------------
  function Get_Acc_Reserv(pi_org_Id    in t_organizations.org_id%type,
                          pi_Date_Top  in t_acc_transfer.tr_date%type,
                          pi_Date_End  in t_acc_transfer.tr_date%type,
                          pi_worker_id in T_USERS.USR_ID%type,
                          po_err_num   out pls_integer,
                          po_err_msg   out t_Err_Msg) return sys_refcursor is
    res        sys_refcursor;
    l_Date_Top date;
    l_Date_End date;
  Begin
    if ((pi_org_id is null) or (pi_org_id <= -1)) then
      raise ex_org_id_is_null;
    end if;
    -- Проверка прав доступа
    if (not Security_pkg.Check_Rights_str('EISSD.ACCOUNTS.REPORT.MOVE_FUNDS',
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
    
      return null;
    end if;
    If pi_Date_Top is Null then
      l_Date_Top := To_Date('01.01.2000', 'DD.MM.RRRR');
    Else
      l_Date_Top := pi_Date_Top;
    End If;
    If pi_Date_End is Null then
      l_Date_End := Sysdate;
    Else
      l_Date_End := pi_Date_End;
    End If;
  
    Open Res for
      Select t.*,
             (NVL(op_Prihod, 0) - (NVL(op_6003, 0) + NVL(op_6024, 0))) Op_PrihodOther,
             (NVL(op_Rashod, 0) - (NVL(op_6014_15, 0) + NVL(op_6013_23, 0) +
             NVL(op_6025, 0) + NVL(op_6018, 0))) op_RashodOther,
             
             op_Rashod,
             NVL(op_Prihod, 0) - NVL(op_Rashod, 0) + NVL(Saldo_Top, 0) Saldo_End
        From (Select org_Id,
                     Max(org_name) org_name,
                     
                     acc_operations.Get_acc_Saldo2(dog_id, l_Date_Top, 5004) Saldo_Top,
                     Sum(Decode(Dv_Id, 6003, amount, Null)) op_6003,
                     Sum(Decode(Dv_Id, 6024, amount, Null)) op_6024,
                     Sum(Decode(In_Out, 1, amount, Null)) op_Prihod,
                     Sum(Decode(Dv_Id,
                                6014,
                                amount,
                                Decode(Dv_Id, 6015, amount, Null))) op_6014_15,
                     Sum(Decode(Dv_Id,
                                6013,
                                amount,
                                Decode(Dv_Id, 6023, amount, Null))) op_6013_23,
                     Sum(Decode(Dv_Id, 6025, amount, Null)) op_6025,
                     Sum(Decode(Dv_Id, 6018, amount, Null)) op_6018,
                     Sum(Decode(In_Out, 0, amount, Null)) op_Rashod
                from (Select org.org_Id,
                             org.org_name,
                             dv_op.dv_id,
                             tat.amount,
                             dog.dog_id,
                             1 In_Out
                        from t_acc_transfer tat
                        Left Join t_dic_values dv_Op
                          on tat.Tr_type = dv_op.dv_id
                      
                        Join t_Accounts acc
                          on tat.dst_acc_id = acc.acc_id
                        Join t_acc_owner tao
                          on (tao.owner_id = acc.acc_owner_id)
                        join t_Dogovor dog
                          on dog.dog_id = tao.owner_ctx_id
                        Join t_Org_Relations tor
                          on tor.id = dog.org_rel_id
                        Join t_organizations org
                          on tor.org_id = org.org_id
                         and tor.org_reltype in (c_rel_tp_dlr_sl, 999)
                       Where tor.org_pid = Pi_Org_Id
                         and acc.acc_type = 5004
                         and Trunc(tat.tr_date - 2 / 24) between l_Date_Top and
                             l_Date_End
                      
                      Union All
                      Select org.org_Id,
                             org.org_name,
                             dv_op.dv_id,
                             tat.amount,
                             dog.dog_id,
                             0 In_Out
                      
                        from t_acc_transfer tat
                        Left Join t_dic_values dv_Op
                          on tat.Tr_type = dv_op.dv_id
                      
                        Join t_Accounts acc
                          on tat.src_acc_id = acc.acc_id
                        Join t_acc_owner tao
                          on (tao.owner_id = acc.acc_owner_id)
                        join t_Dogovor dog
                          on dog.dog_id = tao.owner_ctx_id
                        Join t_Org_Relations tor
                          on tor.id = dog.org_rel_id
                        Join t_organizations org
                          on tor.org_id = org.org_id
                         and tor.org_reltype in (c_rel_tp_dlr_sl, 999)
                      
                       Where tor.org_pid = Pi_Org_Id
                         and acc.acc_type = 5004
                         and Trunc(tat.tr_date - 2 / 24) between l_Date_Top and
                             l_Date_End)
              
               Group by org_Id, dog_id) t
       Order By org_name;
  
    return Res;
  exception
    when ex_dog_id_is_null then
      po_err_num := 1001;
      po_err_msg := 'Не верно задан ИД договора!';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      /*open res for
      select null from dual where 1 <> 1;*/
    
      return null;
    
  End Get_Acc_Reserv;

  ------------------------------------------------------------------
  -- Перемещения ТМЦ по ВСЕМУ диллеру (сводный отчёт по движению ТМЦ)
  ------------------------------------------------------------------
  function Get_TMC_SHahmatka_2(pi_org_Id      in t_organizations.org_id%type,
                               Pi_TMC_type    in number,
                               pi_Date_Top    in t_Tmc_Operations.Op_Date%type,
                               pi_Date_End    in t_Tmc_Operations.Op_Date%type,
                               pi_worker_id   in T_USERS.USR_ID%type,
                               pi_tar_include in number,
                               po_err_num     out pls_integer,
                               po_err_msg     out varchar2)
    return sys_refcursor is
    res              sys_refcursor;
    l_Date_Top       date;
    l_Date_End       date;
    l_org_region     number;
    l_gen_doc_number t_Err_Msg := '';
  begin
    /*logging_pkg.debug('Pi_TMC_type:' || Pi_TMC_type,
    c_package || 'Get_TMC_SHahmatka_2');*/
    if ((pi_org_id is null) or (pi_org_id <= -1)) then
      raise ex_org_id_is_null;
    end if;
  
    -- Проверка прав доступа
    if (not Security_pkg.Check_Rights_str('EISSD.TMC_REPORT.NAKLADNAYA.VIEW',
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      return null;
    end if;
  
    If pi_Date_Top is Null then
      l_Date_Top := To_Date('01.01.2000', 'DD.MM.RRRR');
    Else
      l_Date_Top := pi_Date_Top;
    End If;
    If pi_Date_End is Null then
      l_Date_End := Sysdate;
    Else
      l_Date_End := pi_Date_End;
    End If;
  
    select ORG.REGION_ID
      into l_org_region
      from T_ORGANIZATIONS ORG
     where ORG.ORG_ID = pi_org_id;
  
    if pi_tar_include = 0 then
      Open res for
      
        select t1.subdiller,
               t1.org_name,
               t1.tar_id,
               pi_tmc_Type TMC_TYPE,
               tar.TITLE TAR_NAME,
               nvl(BEG_KOL, 0) TopSaldo,
               nvl(t1.prihod, 0) + nvl(t1.PRIHOD_PEREMESH, 0) GET,
               nvl(t1.Izlishki_Nedostacha, 0) IZB,
               nvl(t1.PRIHOD_TAR, 0) CHTAR, --
               
               nvl(t1.PRODAGA, 0) PRODAGA,
               nvl(t1.UTERYA_ACTIVE, 0) LOST_ACTIVE,
               nvl(t1.UTERYA_NOT_ACTIVE, 0) LOST_NOT_ACT,
               nvl(t1.RASHOD_VOZVRAT, 0) BACK_RAS,
               
               nvl(t1.RASHOD_TAR, 0) CHTAR_RAS, --
               nvl(t1.RASHOD, 0) RASHOD,
               nvl(t1.endd_kol, 0) END_KOL,
               Null DOG_NUMBER -- номер договора
          from (Select ost.subdiller,
                       ost.org_name,
                       ost.tar_id,
                       -- начльный остаток
                       sum(case
                             when ost.op_type = 18 -- Приход
                                  and ost.op_date < Trunc(L_DATE_top) and
                                  ost.owner_id_1 = ost.subdiller then
                              1
                           
                             when ost.op_type = 20 -- Приход (Перемещение)
                                  and ost.owner_id_1 = ost.subdiller and ost.lvl_owner = 2 and
                                  ost.op_date < Trunc(L_DATE_top) then
                              1
                             when ost.op_type = 20 -- Расход (перемещение ниже по дереву)
                                  and ost.owner_id_0 = ost.subdiller and
                                  ost.op_date < Trunc(L_DATE_top) then
                              -1
                           
                             when ost.op_type in (21, 536, 543, 544) -- Расход (Возврат)
                                  and ost.op_date < Trunc(L_DATE_top) and ost.lvl_owner = 1 and
                                  ost.owner_id_0 = ost.subdiller then
                              -1
                           
                             when ost.op_type = 22 -- Расход (Продажа по всем у дереву)
                                  and ost.op_date < Trunc(L_DATE_top) and lvl is Null and
                                  ((org_reltype = 1006 and ost.owner_id_0 = ost.subdiller) or
                                  org_reltype <> 1006) then
                             
                              -1
                           
                             when ost.op_type = 23 -- Расход (утеря по всемку дереву)
                                  and ost.st_sklad_1 = 13 and ost.op_date < L_DATE_top then
                              -1
                           
                           /*14.05.09*/
                             when ost.op_type in (1901, 26, 22, 1902) -- Смена тарифа - расход
                                 --                            and ost.owner_id_0 = PI_ORG_ID
                                  and ost.lvl = 1 and ost.op_date < trunc(L_DATE_top) then
                              -1
                           
                             when ost.op_type in (1901, 26, 1902) -- Смена тарифа - Приход
                                 --                             and ost.owner_id_0 = PI_ORG_ID
                                  and ost.lvl = 2 and ost.op_date < trunc(L_DATE_top) then
                              1
                           /*14.05.09*/
                           
                             else
                              0
                           end) BEG_KOL,
                       -- приход
                       Sum(Case
                             when ost.op_date >= Trunc(L_DATE_top) and ost.op_date < Trunc(L_DATE_END) and
                                  ost.op_type = 18 and ost.owner_id_1 = ost.subdiller then
                              1
                             else
                              0
                           end) PRIHOD,
                       -- издишки/недостача
                       0 IZLISHKI_NEDOSTACHA,
                       /*14.05.09*/
                       --приход (смена тарифа)
                       Sum((Case
                             when ost.op_date >= Trunc(L_DATE_top) and ost.op_date < L_DATE_END and
                                  ost.op_type in (1901, 26, 22, 1902) then
                              decode(ost.lvl, 2, 1, 0)
                             else
                              0
                           end)) PRIHOD_TAR,
                       
                       -- расход (смена тарифа)
                       Sum((Case
                             when ost.op_date >= Trunc(L_DATE_top) and ost.op_date < Trunc(L_DATE_END) and
                                  ost.op_type in (1901, 26, 22, 1902) then
                              decode(ost.lvl, 1, 1, 0)
                             else
                              0
                           end)) RASHOD_TAR,
                       /*14.05.09*/
                       
                       -- приход (перемещение)
                       Sum(Case
                             when ost.op_date >= Trunc(L_DATE_top) and ost.op_date < Trunc(L_DATE_END) and
                                  ost.op_type = 20 and ost.owner_id_1 = ost.subdiller and
                                  ost.lvl_owner = 2 then
                              1
                             else
                              0
                           end) PRIHOD_PEREMESH,
                       
                       -- Расход (перемещение ниже по дереву)
                       Sum(Case
                             when ost.op_date >= Trunc(L_DATE_top) and ost.op_date < Trunc(L_DATE_END) and
                                  ost.op_type = 20 and ost.owner_id_0 = ost.subdiller then
                              1
                             else
                              0
                           end) RASHOD,
                       
                       -- расход( возврат)
                       Sum(Case
                             when ost.op_date >= Trunc(L_DATE_top) and ost.op_date < Trunc(L_DATE_END) and
                                  ost.op_type in (21, 536, 543, 544) and
                                  ost.owner_id_0 = ost.subdiller and ost.lvl_owner = 1 then
                              1
                             else
                              0
                           end) RASHOD_VOZVRAT,
                       -- расход (продажа)
                       Sum(Case
                             when ost.op_date >= Trunc(L_DATE_top) and ost.op_date < Trunc(L_DATE_END) and
                                  ((org_reltype = 1006 and ost.owner_id_0 = ost.subdiller) or
                                  org_reltype <> 1006) and ost.op_type = 22 --and NVL(ost.lvl, 2) = 2
                             
                              then
                              1
                             else
                              0
                           end) PRODAGA,
                       -- расход (утеря)
                       Sum(Case
                             when ost.op_date >= Trunc(L_DATE_top) and ost.op_date < Trunc(L_DATE_END) and
                                  ost.op_type = 23 and ost.st_sklad_1 = 13
                                 /*14.05.09*/
                                  and (ost.tmc_type = 6 and ost.card_active_state = 1601 or
                                  ost.tmc_type = 8 and ost.sim_active_state = 1501 or
                                  ost.tmc_type = 9 and ost.ruim_active_state = 1501 or
                                  ost.tmc_type = 7000 and ost.active_state = 1501)
                             /*14.05.09*\*/
                              then
                              1
                             else
                              0
                           end) UTERYA_ACTIVE,
                       
                       /*14.05.09*/
                       -- расход (утеря/ не активировано)
                       Sum((Case
                             when ost.op_date >= Trunc(L_DATE_top) and ost.op_date < Trunc(L_DATE_END) and
                                  ost.op_type = 23 and ost.st_sklad_1 = 13 and
                                  (ost.tmc_type = 6 and ost.card_active_state = 1602 or
                                  ost.tmc_type = 8 and ost.sim_active_state = 1502 or
                                  ost.tmc_type = 9 and ost.ruim_active_state = 1502 or
                                  ost.tmc_type = 7000 and ost.active_state = 1502) then
                              1
                             else
                              0
                           end)) UTERYA_NOT_ACTIVE,
                       /*14.05.09*/
                       sum(case
                             when ost.op_type = 18 -- Приход
                                  and ost.op_date < Trunc(L_DATE_END) and
                                  ost.owner_id_1 = ost.subdiller then
                              1
                           
                             when ost.op_type = 20 -- Приход (Перемещение)
                                  and ost.lvl_owner = 2 and ost.owner_id_1 = ost.subdiller and
                                  ost.op_date < Trunc(L_DATE_END) then
                              1
                             when ost.op_type = 20 -- Расход (перемещение ниже по дереву)
                                  and ost.owner_id_0 = ost.subdiller and
                                  ost.op_date < Trunc(L_DATE_END) then
                              -1
                           
                             when ost.op_type in (21, 536, 543, 544) -- Расход (Возврат)
                                  and ost.op_date < Trunc(L_DATE_END) and ost.lvl_owner = 1 and
                                  ost.owner_id_0 = ost.subdiller then
                              -1
                           
                             when ost.op_type = 22 -- Расход (Продажа по всем у дереву)
                                  and ost.op_date < Trunc(L_DATE_END) and
                                  ((org_reltype = 1006 and ost.owner_id_0 = ost.subdiller) or
                                  org_reltype <> 1006) and lvl is Null then
                              -1
                             when ost.op_type in (1901, 26, 22, 1902) and lvl = 1 -- Расход (смена тарифа)
                                  and ost.op_date < Trunc(L_DATE_END) then
                              -1
                             when ost.op_type in (1901, 26, 1902) and lvl = 2 -- Приход (смена тарифа)
                                  and ost.op_date < Trunc(L_DATE_END) then
                              1
                           
                             when ost.op_type = 23 -- Расход (утеря по всемку дереву)
                                  and ost.st_sklad_1 = 13 and ost.op_date < L_DATE_END then
                              -1
                             else
                              0
                           end) ENDD_KOL
                  From (Select tou.tmc_id,
                               org.org_name,
                               tto.op_type,
                               tto.op_date,
                               tou.owner_id_0,
                               tou.owner_id_1,
                               decode(l.lvl,
                                      1,
                                      tou.tar_id_0,
                                      NVL(tou.tar_id_1, tou.tar_id_0)) tar_id,
                               tou.st_sklad_0,
                               tou.st_sklad_1,
                               nvl(dillers.subdiller, dillers.diller) subdiller,
                               l1.lvl_owner,
                               l.lvl, -- уровень тарифа
                               /*14.05.09*/
                               t.tmc_type,
                               ts.sim_active_state,
                               tr.ruim_active_state,
                               tc.card_active_state,
                               tac.active_state,
                               tc.card_full_cost    card_full_cost, -- номинальная стоимость карт платы
                               pt.name_type, -- тип порта модема
                               dillers.org_reltype
                        /*14.05.09*/
                          from t_tmc_operations tto
                          Join t_tmc_operation_units tou
                            on tou.op_id = tto.op_id
                           and Nvl(tou.error_id, 0) < 1
                          left join (Select level lvl_owner
                                      from dual
                                    connect by level <= 2) l1
                            on tto.op_type in (20, 21, 543, 544)
                          join t_tmc t
                            on tou.tmc_id = t.tmc_id
                        /*14.05.09*/
                          left join (Select level lvl
                                      from dual
                                    connect by level <= 2) l
                            on (tto.op_type in (1901, 26, 1902) and
                               tou.tar_id_0 <> tou.tar_id_1)
                            or (tto.op_type = 22 and tou.tar_id_0 is Null)
                        
                        /*14.05.09*/
                          Join (Select tor.org_id,
                                      tor.org_reltype,
                                      Connect_By_Root(tor.org_id) diller,
                                      SubStr(Sys_Connect_By_Path(tor.org_id,
                                                                 '/'),
                                             Decode(InStr(Sys_Connect_By_Path(tor.org_id,
                                                                              '/'),
                                                          '/',
                                                          2),
                                                    0,
                                                    500,
                                                    InStr(Sys_Connect_By_Path(tor.org_id,
                                                                              '/'),
                                                          '/',
                                                          2)) + 1,
                                             7) subdiller
                                 from t_org_relations tor
                               Connect By Prior tor.org_id = tor.org_pid
                                Start with tor.org_id = PI_ORG_ID
                                       and NVL(org_Pid, 0) > 0) Dillers
                            on (Dillers.subdiller = tou.owner_id_0 and
                               Dillers.subdiller = Dillers.org_id and
                               tto.op_type in (20, 21, 543, 544) and
                               l1.lvl_owner = 1) -- Расход (перемещение + возврат)
                            or (Dillers.subdiller = tou.owner_id_1 and
                               Dillers.subdiller = Dillers.org_id and
                               tto.op_type in (20, 21, 536, 543, 544) and
                               l1.lvl_owner = 2) -- Приход (перемещение + возврат)
                            or (Dillers.org_id = tou.Owner_Id_1 -- Приход
                               and tto.op_type in (18, 1901, 26, 1902))
                            or ((Dillers.Org_Id = tou.Owner_Id_0) and
                               tto.op_type = 22) -- Расход (Продажа)
                            or ((Dillers.Org_Id = tou.Owner_Id_0) and
                               tto.op_type = 23 and tou.st_sklad_1 = 13) -- Расход (Утеря)
                          Join t_Organizations org
                            on Dillers.subdiller = org.org_id
                        /*14.05.09*/
                          left join t_tmc_sim ts
                            on t.tmc_id = ts.tmc_id
                        -- and t.tmc_type = 8
                        -- and pi_tmc_type = 8
                          left join t_tmc_ruim tr
                            on t.tmc_id = tr.tmc_id
                        --and t.tmc_type = 9
                        --and pi_tmc_type = 9
                          left join t_tmc_pay_card tc
                            on t.tmc_id = tc.tmc_id
                        --and t.tmc_type = 6
                        --and pi_tmc_type = 6
                          left join t_tmc_adsl_card tac
                            on t.tmc_id = tac.tmc_id
                        --    and t.tmc_type = 7000
                        --  and pi_tmc_type = 7000
                          left join t_tmc_modem_adsl tm
                            on t.tmc_id = tm.tmc_id
                        -- and t.tmc_type = 5
                        --and pi_tmc_type = 5
                          left join t_modem_model_adsl pmc
                            on tm.modem_model = pmc.id_mark
                          left join t_modem_adsl_port_type pt
                            on pmc.id_type_port = pt.id_type
                        
                        /*14.05.09*/
                         Where trunc(tto.op_date) < L_DATE_END
                           and t.tmc_type = decode(pi_tmc_type,
                                                   7004,
                                                   7002,
                                                   7005,
                                                   7002,
                                                   pi_tmc_type)
                         Order by tou.tmc_id, tto.op_date) OST
                 where ost.subdiller <> PI_ORG_ID
                 Group by ost.subdiller, ost.org_name, ost.tar_id
                 order by ost.subdiller, ost.org_name
                
                ) T1
          left join t_tarif_by_at_id tar
            on tar.AT_ID = t1.tar_id
         where BEG_KOL + prihod + PRIHOD_PEREMESH + Izlishki_Nedostacha +
               PRIHOD_TAR + PRODAGA + UTERYA_ACTIVE + UTERYA_NOT_ACTIVE +
               RASHOD_VOZVRAT + RASHOD_TAR + endd_kol <> 0;
    
    else
      Open res for
        select t1.subdiller,
               t1.org_name,
               Pi_TMC_type TMC_TYPE,
               '-' TAR_NAME,
               nvl(BEG_KOL, 0) TopSaldo,
               /* занесём это в одну колонку
               nvl(t1.prihod, 0) PRIHOD,
               nvl(t1.PRIHOD_PEREMESH, 0) PERED_IN,*/
               nvl(t1.prihod, 0) + nvl(t1.PRIHOD_PEREMESH, 0) GET,
               nvl(t1.Izlishki_Nedostacha, 0) IZB,
               nvl(t1.PRIHOD_TAR, 0) CHTAR, --
               
               nvl(t1.PRODAGA, 0) PRODAGA,
               nvl(t1.UTERYA_ACTIVE, 0) LOST_ACTIVE,
               nvl(t1.UTERYA_NOT_ACTIVE, 0) LOST_NOT_ACT,
               nvl(t1.RASHOD_VOZVRAT, 0) BACK_RAS,
               
               nvl(t1.RASHOD_TAR, 0) CHTAR_RAS, --
               nvl(t1.RASHOD, 0) RASHOD, --
               
               /*14.05.09*/
               
               /*14.05.09*/
               /* nvl(BEG_KOL, 0) + nvl(t1.prihod, 0) +
               nvl(t1.PRIHOD_PEREMESH, 0) \*- nvl(t1.RASHOD_PEREMESH, 0)*\ + nvl(t1.Izlishki_Nedostacha, 0) \*+
               nvl(t1.Prihod_Tar, 0)*\ - nvl(t1.PRODAGA, 0) -
               nvl(t1.RASHOD_VOZVRAT, 0)\*+ nvl(t1.PRIHOD_VOZVRAT, 0)*\ - nvl(t1.UTERYA, 0) \*- nvl(t1.rashod_Tar, 0)*\ END_KOL,*/
               nvl(t1.endd_kol, 0) END_KOL,
               l_gen_doc_number DOG_NUMBER -- номер договора
          from (Select ost.subdiller,
                       ost.org_name,
                       /* nvl(ost.card_full_cost, 0) card_full_cost, -- номинальная стоимость карт оплаты
                       nvl(name_type, 0) name_type, -- тип порта модема*/
                       -- начльный остаток
                       sum(case
                             when ost.op_type = 18 -- Приход
                                  and Trunc(ost.op_date) < Trunc(l_date_top) and
                                  ost.owner_id_1 = ost.subdiller then
                              1
                           
                             when ost.op_type = 20 -- Приход (Перемещение)
                                  and ost.owner_id_1 = ost.subdiller and
                                  Trunc(ost.op_date) < Trunc(l_date_top) then
                              1
                             when ost.op_type = 20 -- Расход (перемещение ниже по дереву)
                                  and ost.owner_id_0 = ost.subdiller and
                                  Trunc(ost.op_date) < Trunc(l_date_top) then
                              -1
                           
                             when ost.op_type in (21, 536, 543, 544) -- Расход (Возврат)
                                  and Trunc(ost.op_date) < Trunc(l_date_top) and
                                  ost.owner_id_0 = ost.subdiller then
                              -1
                           
                             when ost.op_type = 22 -- Расход (Продажа по всем у дереву)
                                  and Trunc(ost.op_date) < Trunc(l_date_top) and
                                  ((org_reltype = 1006 and
                                  ost.owner_id_0 = ost.subdiller) or
                                  org_reltype <> 1006) then
                             
                              -1
                           
                             when ost.op_type = 23 -- Расход (утеря по всемку дереву)
                                  and ost.st_sklad_1 = 13 and
                                  trunc(ost.op_date) < l_date_top then
                              -1
                           
                           /*14.05.09*/
                             when ost.op_type in (1901, 26, 1902) -- Смена тарифа - расход
                                  and ost.owner_id_0 = pi_org_Id and ost.lvl = 1 and
                                  trunc(ost.op_date) < trunc(l_date_top) then
                              -1
                           
                             when ost.op_type in (1901, 26, 1902) -- Смена тарифа - Приход
                                  and ost.owner_id_0 = pi_org_Id and ost.lvl = 2 and
                                  trunc(ost.op_date) < trunc(l_date_top) then
                              1
                           /*14.05.09*/
                           
                             else
                              0
                           end) BEG_KOL,
                       -- приход
                       Sum(Case
                             when Trunc(ost.op_date) >= Trunc(l_date_top) and
                                  Trunc(ost.op_date) < Trunc(l_date_end) and
                                  ost.op_type = 18 and
                                  ost.owner_id_1 = ost.subdiller then
                              1
                             else
                              0
                           end) PRIHOD,
                       -- издишки/недостача
                       0 IZLISHKI_NEDOSTACHA,
                       /*14.05.09*/
                       --приход (смена тарифа)
                       Sum((Case
                             when Trunc(ost.op_date) >= Trunc(l_date_top) and
                                  Trunc(ost.op_date) < l_date_end and
                                  ost.op_type in (1901, 26, 1902) then
                              decode(ost.lvl, 2, 1, 0)
                             else
                              0
                           end)) PRIHOD_TAR,
                       
                       -- расход (смена тарифа)
                       Sum((Case
                             when Trunc(ost.op_date) >= Trunc(l_date_top) and
                                  Trunc(ost.op_date) < Trunc(l_date_end) and
                                  ost.op_type in (1901, 26, 1902) then
                              decode(ost.lvl, 1, 1, 0)
                             else
                              0
                           end)) RASHOD_TAR,
                       /*14.05.09*/
                       
                       -- приход (перемещение)
                       Sum(Case
                             when Trunc(ost.op_date) >= Trunc(l_date_top) and
                                  Trunc(ost.op_date) < Trunc(l_date_end) and
                                  ost.op_type = 20 and
                                  ost.owner_id_1 = ost.subdiller then
                              1
                             else
                              0
                           end) PRIHOD_PEREMESH,
                       
                       -- Расход (перемещение ниже по дереву)
                       Sum(Case
                             when Trunc(ost.op_date) >= Trunc(l_date_top) and
                                  Trunc(ost.op_date) < Trunc(l_date_end) and
                                  ost.op_type = 20 and ost.org_reltype <> 1006 and
                                  ost.owner_id_0 = ost.subdiller then
                             
                              1
                             else
                              0
                           end) RASHOD,
                       -- расход( возврат)
                       Sum(Case
                             when Trunc(ost.op_date) >= Trunc(l_date_top) and
                                  Trunc(ost.op_date) < Trunc(l_date_end) and
                                  ost.op_type in (21, 536, 543, 544) and
                                  ost.owner_id_0 = ost.subdiller then
                              1
                             else
                              0
                           end) RASHOD_VOZVRAT,
                       -- расход (продажа)
                       Sum(Case
                             when Trunc(ost.op_date) >= Trunc(l_date_top) and
                                  Trunc(ost.op_date) < Trunc(l_date_end) and
                                  ost.op_type = 22 and
                                  ((org_reltype = 1006 and
                                   ost.owner_id_0 = ost.subdiller) or
                                   org_reltype <> 1006) then
                             
                              1
                             else
                              0
                           end) PRODAGA,
                       -- расход (утеря)
                       Sum(Case
                             when Trunc(ost.op_date) >= Trunc(l_date_top) and
                                  Trunc(ost.op_date) < Trunc(l_date_end) and
                                  ost.op_type = 23 and ost.st_sklad_1 = 13
                                 /*14.05.09*/
                                  and (ost.tmc_type = 6 and
                                       ost.card_active_state = 1601 or
                                       ost.tmc_type = 8 and
                                       ost.sim_active_state = 1501 or
                                       ost.tmc_type = 9 and
                                       ost.ruim_active_state = 1501 or
                                       ost.tmc_type = 7000 and
                                       ost.active_state = 1501)
                             /*14.05.09*\*/
                              then
                              1
                             else
                              0
                           end) UTERYA_ACTIVE,
                       
                       /*14.05.09*/
                       -- расход (утеря/ не активировано)
                       Sum((Case
                             when Trunc(ost.op_date) >= Trunc(l_date_top) and
                                  Trunc(ost.op_date) < Trunc(l_date_end) and
                                  ost.op_type = 23 and ost.st_sklad_1 = 13 and
                                  (ost.tmc_type = 6 and
                                   ost.card_active_state = 1602 or
                                   ost.tmc_type = 8 and
                                   ost.sim_active_state = 1502 or
                                   ost.tmc_type = 9 and
                                   ost.ruim_active_state = 1502 or
                                   ost.tmc_type = 7000 and
                                   ost.active_state = 1502) then
                              1
                             else
                              0
                           end)) UTERYA_NOT_ACTIVE,
                       /*14.05.09*/
                       sum(case
                             when ost.op_type = 18 -- Приход
                                  and Trunc(ost.op_date) < Trunc(l_date_end) and
                                  ost.owner_id_1 = ost.subdiller then
                              1
                           
                             when ost.op_type = 20 -- Приход (Перемещение)
                                 --and ost.lvl_owner = 2
                                  and ost.owner_id_1 = ost.subdiller and
                                  Trunc(ost.op_date) < Trunc(l_date_end) then
                              1
                             when ost.op_type = 20 -- Расход (перемещение ниже по дереву)
                                 --and ost.lvl_owner = 2
                                  and ost.owner_id_0 = ost.subdiller and
                                  Trunc(ost.op_date) < Trunc(l_date_end) and
                                  ost.org_reltype <> 1006 then
                             
                              -1
                           
                             when ost.op_type in (21, 536, 543, 544) -- Расход (Возврат)
                                  and Trunc(ost.op_date) < Trunc(l_date_end)
                                 --and ost.lvl_owner = 1
                                  and ost.owner_id_0 = ost.subdiller then
                              -1
                           
                             when ost.op_type = 22 -- Расход (Продажа по всем у дереву)
                                  and Trunc(ost.op_date) < Trunc(l_date_end) and
                                  ((org_reltype = 1006 and
                                  ost.owner_id_0 = ost.subdiller) or
                                  org_reltype <> 1006) then
                             
                              -1
                           
                             when ost.op_type = 23 -- Расход (утеря по всемку дереву)
                                  and ost.st_sklad_1 = 13 and
                                  trunc(ost.op_date) < l_date_end then
                              -1
                             else
                              0
                           end) ENDD_KOL
                  From (Select org.org_name,
                               tto.op_type,
                               tto.op_date,
                               tou.owner_id_0,
                               tou.owner_id_1,
                               tou.st_sklad_0,
                               tou.st_sklad_1,
                               nvl(dillers.subdiller, dillers.diller) subdiller,
                               l1.lvl_owner,
                               l.lvl, -- уровень тарифа
                               /*14.05.09*/
                               t.tmc_type,
                               ts.sim_active_state,
                               tr.ruim_active_state,
                               tc.card_active_state,
                               tac.active_state,
                               tc.card_full_cost    card_full_cost, -- номинальная стоимость карт платы
                               pt.name_type, -- тип порта модема
                               dillers.org_reltype
                        /*14.05.09*/
                          from t_tmc_operations tto
                          Join t_tmc_operation_units tou
                            on tou.op_id = tto.op_id
                           and Nvl(tou.error_id, 0) < 1
                          left join (Select level lvl_owner
                                      from dual
                                    connect by level <= 2) l1
                            on tto.op_type in (20, 21, 543, 544)
                          join t_tmc t
                            on tou.tmc_id = t.tmc_id
                        /*14.05.09*/
                          left join (Select level lvl
                                      from dual
                                    connect by level <= 2) l
                            on tto.op_type in (1901, 26, 1902)
                           and tou.tar_id_0 <> tou.tar_id_1
                        /*14.05.09*/
                          Join (Select tor.org_id,
                                      tor.org_reltype,
                                      Connect_By_Root(tor.org_id) diller,
                                      SubStr(Sys_Connect_By_Path(tor.org_id,
                                                                 '/'),
                                             Decode(InStr(Sys_Connect_By_Path(tor.org_id,
                                                                              '/'),
                                                          '/',
                                                          2),
                                                    0,
                                                    500,
                                                    InStr(Sys_Connect_By_Path(tor.org_id,
                                                                              '/'),
                                                          '/',
                                                          2)) + 1,
                                             7) subdiller
                                 from t_org_relations tor
                               Connect By Prior tor.org_id = tor.org_pid
                                Start with tor.org_id = pi_org_Id
                                       and NVL(org_Pid, 0) > 0) Dillers
                            on (Dillers.org_id = tou.owner_id_0 and
                               tto.op_type in (20, 21, 543, 544) and
                               l1.lvl_owner = 1) -- Расход (перемещение + возврат)
                            or (Dillers.org_id = tou.owner_id_1 and
                               tto.op_type in (20, 21, 536, 543, 544) and
                               l1.lvl_owner = 2) -- Приход (перемещение + возврат)
                            or (Dillers.org_id = tou.Owner_Id_1 -- Приход
                               and tto.op_type = 18)
                            or ((Dillers.Org_Id = tou.Owner_Id_0) and
                               tto.op_type = 22) -- Расход (Продажа)
                            or ((Dillers.Org_Id = tou.Owner_Id_0) and
                               tto.op_type = 23 and tou.st_sklad_1 = 13) -- Расход (Утеря)
                          Join t_Organizations org
                            on Dillers.subdiller = org.org_id
                        /*14.05.09*/
                          left join t_tmc_sim ts
                            on t.tmc_id = ts.tmc_id
                        -- and t.tmc_type = 8
                        -- and pi_tmc_type = 8
                          left join t_tmc_ruim tr
                            on t.tmc_id = tr.tmc_id
                        --and t.tmc_type = 9
                        --and pi_tmc_type = 9
                          left join t_tmc_pay_card tc
                            on t.tmc_id = tc.tmc_id
                        --and t.tmc_type = 6
                        --and pi_tmc_type = 6
                          left join t_tmc_adsl_card tac
                            on t.tmc_id = tac.tmc_id
                        --    and t.tmc_type = 7000
                        --  and pi_tmc_type = 7000
                          left join t_tmc_modem_adsl tm
                            on t.tmc_id = tm.tmc_id
                        -- and t.tmc_type = 5
                        --and pi_tmc_type = 5
                          left join t_modem_model_adsl pmc
                            on tm.modem_model = pmc.id_mark
                          left join t_modem_adsl_port_type pt
                            on pmc.id_type_port = pt.id_type
                        
                        /*14.05.09*/
                         Where trunc(tto.op_date) < l_date_end
                           and t.tmc_type = decode(pi_tmc_type,
                                                   7004,
                                                   7002,
                                                   7005,
                                                   7002,
                                                   pi_tmc_type)) OST
                 where ost.subdiller <> pi_org_id
                 Group by ost.subdiller, ost.org_name
                 order by ost.subdiller, ost.org_name
                
                ) T1;
    end if;
    return res;
  exception
    when ex_org_id_is_null then
      po_err_num := 1001;
      po_err_msg := 'Не верно задан ИД организации';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      /*open res for
      select null from dual where 1 <> 1;*/
      return null;
  End Get_TMC_SHahmatka_2;
  --------------------------------------------------------------
  function Get_Report_Move_Tmc_Diller(pi_org_id       in array_num_2, -- 74819
                                      pi_block        in number, -- 74819 Признак заблокированности организации
                                      pi_org_relation in num_tab, -- 74819 Массив связей
                                      Pi_TMC_type     in number,
                                      pi_Date_Top     in t_Tmc_Operations.Op_Date%type,
                                      pi_Date_End     in t_Tmc_Operations.Op_Date%type,
                                      pi_worker_id    in T_USERS.USR_ID%type,
                                      po_err_num      out pls_integer,
                                      po_err_msg      out varchar2)
    return sys_refcursor is
    res          sys_refcursor;
    l_Date_Top   date;
    l_date_cache date;
    l_Date_End   date;
    --l_org_region     number;
    l_gen_doc_number t_Err_Msg := '';
    l_count          number;
    l_org_tab        Num_Tab;
    l_user_org_tab   num_tab;
    l_perm_id        num_tab := num_tab();
    l_date_begin     date;
  begin
    l_date_begin := systimestamp;
    logging_pkg.info('pi_org_id := ' || get_str_by_array_num_2(pi_org_id) || '; ' ||
                     'pi_block := ' || pi_block || '; ' ||
                     'pi_org_relation := ' ||
                     get_str_by_num_tab(pi_org_relation) || '; ' ||
                     'Pi_TMC_type := ' || Pi_TMC_type || '; ' ||
                     'pi_Date_Top := ' || pi_Date_Top || '; ' ||
                     'pi_Date_End := ' || pi_Date_End || '; ' ||
                     'pi_worker_id := ' || pi_worker_id || '; ',
                     'Get_Report_Move_Tmc_Diller');
    select count(*) into l_count from table(pi_org_id);
    if l_count = 0 then
      raise ex_org_id_is_null;
    end if;
    -- Проверка прав доступа
    if (not Security_pkg.Check_User_Right_str('EISSD.TMC_REPORTS.MOVE_TMC',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;
  
    select distinct p.prm_id bulk collect
      into l_perm_id
      from t_tmc_type_rights t
      join t_rights r
        on r.right_id = t.right_id
      join t_perm_rights pr
        on pr.pr_right_id = r.right_id
      join t_perm p
        on p.prm_id = pr.pr_prm_id
       and p.prm_type = 8500
     where t.tmc_type = Pi_TMC_type;
  
    l_org_tab := get_orgs_tab_for_multiset(pi_orgs         => pi_org_id,
                                           pi_worker_id    => pi_worker_id,
                                           pi_block        => pi_block,
                                           pi_org_relation => pi_org_relation);
  
    l_user_org_tab := ORGS.get_user_orgs_by_prm(pi_worker_id => pi_worker_id,
                                                pi_rel_tab   => num_tab(),
                                                pi_prm_tab   => l_perm_id,
                                                pi_block     => pi_block,
                                                po_err_num   => po_err_num,
                                                po_err_msg   => po_err_msg);
  
    l_org_tab := intersects(l_org_tab, l_user_org_tab);
  
    If pi_Date_Top is Null then
      l_Date_Top   := To_Date('01.01.2000', 'DD.MM.RRRR');
      l_date_cache := l_date_top;
    Else
      l_Date_Top := pi_Date_Top;
      select max(c.c_date)
        into l_date_cache
        from t_tmc_opun_cach c
       where c.c_date <= pi_date_top;
    End If;
    If pi_Date_End is Null then
      l_Date_End := Sysdate;
    Else
      l_Date_End := pi_Date_End;
    End If;
    Open res for
      select t1.tar_id,
             coalesce(mp.name_model,
                      mmu.usb_model,
                      sm.stb_model,
                      smm.stb_model ||
                      decode(osm.date_end,
                             null,
                             '',
                             ' (' || osm.date_beg || '-' || osm.date_end || ')'),
                      (select Max(vt.title)
                         from t_tarif_by_at_id vt
                        where t1.tar_id = vt.at_id)) tar_name,
             pi_TMC_type TMC_TYPE,
             osm.is_second_hand,
             nvl(BEG_KOL, 0) TopSaldo,
             nvl(t1.PRIHOD_PEREMESH, 0) PERED_IN,
             nvl(t1.prihod, 0) PRIHOD,
             nvl(t1.prihod_tar, 0) PRIHOD_TAR,
             nvl(t1.PRIHOD_VOZVRAT, 0) PRIHOD_VOZVRAT, -- Сюда попадают возвраты оборудования из-за ошибки АСР
             nvl(t1. VOZVRAT_CLIENTOM, 0) VOZVRAT_CLIENTOM, -- OTT-STB - возврат клиентом
             nvl(t1.rashod_tar, 0) RASHOD_TAR,
             nvl(t1.Izlishki_Nedostacha, 0) IZB,
             nvl(t1.PRODAGA, 0) PRODAGA,
             nvl(t1.RASHOD_VOZVRAT, 0) BACK_RAS,
             nvl(t1.UTERYA, 0) UTERYA,
             nvl(t1.UDALENIE, 0) DELTMC_RAS,
             nvl(t1.ZAMENA_SIM, 0) CHSIM_RAS,
             l_gen_doc_number DOG_NUMBER, -- номер договора
             nvl(t1.beg_kol, 0) + nvl(t1.endd_kol, 0) END_KOL,
             
             --------------------------------------------------------
             nvl(t1.RASHOD_PEREDANO, 0) RASHOD_PEREDANO,
             BRAK
        from (Select Max(ost.tar_id) tar_id,
                     -- начльный остаток
                     sum(case
                           when ost.op_type = 0 then
                            ost.tmc_count
                           when ost.op_type in (18, 24, 536) -- Приход и отмена продажи
                                and ost.op_date < Trunc(l_date_top) then
                            1
                           when ost.op_type in (1901, 26, 1902) -- Приход (смена тарифа)
                                and ost.op_date < Trunc(l_date_top) and
                                ost.lvl = 2 then
                            1
                           when ost.op_type in (1901, 26, 22, 1902) -- Расход (смена тарифа)
                                and ost.op_date < Trunc(l_date_top) and
                                ost.lvl = 1 then
                            -1
                           when ost.op_type = 20 -- Погашение (Перемещение)
                                and ost.op_date < Trunc(l_Date_Top) and
                                ost.owner_id_0 in
                                (select column_value from table(l_org_tab)) and
                                ost.owner_id_1 in
                                (select column_value from table(l_org_tab)) then
                            0
                           when ost.op_type = 20 -- Приход (Перемещение)
                                and ost.op_date < Trunc(l_Date_Top) and
                                ost.owner_id_1 in
                                (select column_value from table(l_org_tab)) then
                            1
                           when ost.op_type = 20 -- Расход (Перемещение)
                                and ost.op_date < Trunc(l_Date_Top) and
                                ost.owner_id_0 in
                                (select column_value from table(l_org_tab)) then
                            -1
                           when ost.op_type = 21 -- Погашение (Возврат)
                                and ost.op_date < Trunc(l_Date_Top) and
                                ost.owner_id_0 in
                                (select column_value from table(l_org_tab)) and
                                ost.owner_id_1 in
                                (select column_value from table(l_org_tab)) then
                            0
                           when ost.op_type = 21 -- Приход (Возврат)
                                and ost.op_date < Trunc(l_date_top) and
                                ost.owner_id_1 in
                                (select column_value from table(l_org_tab)) then
                            1
                           when ost.op_type = 21 -- Расход (Возврат)
                                and ost.op_date < Trunc(l_date_top) and
                                ost.owner_id_0 in
                                (select column_value from table(l_org_tab)) then
                            -1
                           when ost.op_type = 22 -- Расход (Продажа по всем у дереву)
                                and ost.op_date < Trunc(l_date_top) and
                                ost.lvl is Null then
                            -1
                           when ost.op_type = 23 -- Расход (утеря по всемку дереву)
                                and ost.st_sklad_1 = 13 and
                                ost.op_date < l_Date_top then
                            -1
                           when ost.op_type = 530 -- удаление
                                and ost.st_sklad_1 = 216 and
                               -- 40076
                                ost.st_sklad_0 <> 13 and
                                (pi_tmc_type <> 7004 or ost.st_sklad_0 <> 14) and
                                ost.owner_id_0 in
                                (select column_value from table(l_org_tab)) and
                                ost.op_date < trunc(l_Date_top) then
                            -1
                           when ost.op_type = 529 -- замена сим-карты
                                and ost.st_sklad_1 = 12 and
                                ost.owner_id_0 in
                                (select column_value from table(l_org_tab)) and
                                ost.op_date < trunc(l_Date_top) then
                            -1
                           else
                            0
                         end) BEG_KOL,
                     -- приход
                     Sum(Case
                           when ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < Trunc(l_Date_End)+ 1 + 2 / 24 and
                                ((pi_tmc_type <> 7004 and ost.op_type in (18, 24)) or
                                (pi_tmc_type = 7004 and ost.op_type = 18)) then
                            1
                           else
                            0
                         end) PRIHOD,
                     -- Возврат клиентом
                     Sum(Case
                           when ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < Trunc(l_Date_End)+ 1 + 2 / 24 and
                                (pi_tmc_type = 7004 and ost.op_type = 24) then
                            1
                           else
                            0
                         end) VOZVRAT_CLIENTOM,
                     -- приход( возврат)
                     Sum(Case
                           when ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < Trunc(l_Date_End)+ 1 + 2 / 24 and
                                ost.op_type in (536, 21, 543, 544) and
                                ost.owner_id_1 in
                                (select column_value from table(l_org_tab)) then
                            1
                           else
                            0
                         end) PRIHOD_VOZVRAT,
                     -- издишки/недостача
                     0 IZLISHKI_NEDOSTACHA,
                     -- приход (перемещение)
                     Sum(Case
                           when ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < Trunc(l_Date_End)+ 1 + 2 / 24 and
                                ost.op_type = 20 and
                                ost.owner_id_1 in
                                (select column_value from table(l_org_tab)) then
                            1
                           else
                            0
                         end) PRIHOD_PEREMESH,
                     -- Приход (смена тарифа)
                     Sum(Case
                           when ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < Trunc(l_Date_End)+ 1 + 2 / 24 and
                                ost.op_type in (1901, 26, 1902) and ost.lvl = 2 then
                            1
                           else
                            0
                         end) PRIHOD_TAR,
                     -- Hасход (смена тарифа)
                     Sum(Case
                           when ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < Trunc(l_Date_End)+ 1 + 2 / 24 and
                                ost.op_type in (1901, 26, 22, 1902) and
                                ost.lvl = 1 then
                            1
                           else
                            0
                         end) Rashod_TAR,
                     -- расход( возврат)
                     Sum(Case
                           when ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < Trunc(l_Date_End)+ 1 + 2 / 24 and
                                ost.op_type in (21, 543, 544) and
                                ost.owner_id_0 in
                                (select column_value from table(l_org_tab)) then
                            1
                           else
                            0
                         end) RASHOD_VOZVRAT,
                     -- расход (продажа)
                     Sum(Case
                           when ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < Trunc(l_Date_End)+ 1 + 2 / 24 and
                                ost.op_type = 22 and NVL(ost.lvl, 2) = 2 then
                            1
                           else
                            0
                         end) PRODAGA,
                     -- расход (утеря)
                     Sum(Case
                           when ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < Trunc(l_Date_End)+ 1 + 2 / 24 and
                                ost.op_type = 23 and ost.st_sklad_1 = 13 then
                            1
                           else
                            0
                         end) UTERYA,
                     -- расход (удаление)
                     Sum((Case
                           when ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < Trunc(l_Date_End)+ 1 + 2 / 24 and
                                ost.op_type = 530 and
                               -- 40076
                                ost.st_sklad_0 <> 13 and ost.st_sklad_1 = 216 and
                                (pi_tmc_type <> 7004 or ost.st_sklad_0 <> 14) then
                            1
                           else
                            0
                         end)) UDALENIE,
                     -- расход (замена сим-карты)
                     Sum((Case
                           when ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < Trunc(l_Date_End)+ 1 + 2 / 24 and
                                ost.op_type = 529 and ost.st_sklad_1 = 12 then
                            1
                           else
                            0
                         end)) ZAMENA_SIM,
                     sum(case
                           when ost.op_type in (18, 24, 536) -- Приход
                                and ost.op_date < Trunc(l_date_end)+ 1 + 2 / 24 and
                                ost.op_date >= Trunc(l_Date_Top) then
                            1
                           when ost.op_type in (1901, 26, 1902) -- Приход (смена тарифа)
                                and ost.op_date < Trunc(l_date_end)+ 1 + 2 / 24 and
                                ost.op_date >= Trunc(l_Date_Top) and ost.lvl = 2 then
                            1
                           when ost.op_type in (1901, 26, 22, 1902) -- Расход (смена тарифа)
                                and ost.op_date < Trunc(l_date_end)+ 1 + 2 / 24 and
                                ost.op_date >= Trunc(l_Date_Top) and ost.lvl = 1 then
                            -1
                           when ost.op_type = 20 -- Погашение (Перемещение)
                                and ost.op_date < Trunc(l_date_end)+ 1 + 2 / 24 and
                                ost.op_date >= Trunc(l_Date_Top) and
                                ost.owner_id_0 in
                                (select column_value from table(l_org_tab)) and
                                ost.owner_id_1 in
                                (select column_value from table(l_org_tab)) then
                            0
                           when ost.op_type = 20 -- Приход (Перемещение)
                                and ost.op_date < Trunc(l_date_end)+ 1 + 2 / 24 and
                                ost.op_date >= Trunc(l_Date_Top) and
                                ost.owner_id_1 in
                                (select column_value from table(l_org_tab)) then
                            1
                           when ost.op_type = 20 -- Расход (Перемещение)
                                and ost.op_date < Trunc(l_date_end)+ 1 + 2 / 24 and
                                ost.op_date >= Trunc(l_Date_Top) and
                                ost.owner_id_0 in
                                (select column_value from table(l_org_tab)) then
                            -1
                           when ost.op_type = 21 -- Погашение (Возврат)
                                and ost.op_date < Trunc(l_date_end)+ 1 + 2 / 24 and
                                ost.op_date >= Trunc(l_Date_Top) and
                                ost.owner_id_0 in
                                (select column_value from table(l_org_tab)) and
                                ost.owner_id_1 in
                                (select column_value from table(l_org_tab)) then
                            0
                           when (ost.op_type in (21, 543, 544) and
                                ost.owner_id_0 in
                                (select column_value from table(l_org_tab)))
                               -- Расход (Возврат)
                                and ost.op_date < Trunc(l_date_end)+ 1 + 2 / 24 and
                                ost.op_date >= Trunc(l_Date_Top) then
                            -1
                           when ost.op_type = 21 -- Приход (Возврат)
                                and ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < trunc(l_Date_end)+ 1 + 2 / 24 and
                                ost.owner_id_1 in
                                (select column_value from table(l_org_tab)) then
                            1
                           when ost.op_type = 22 -- Расход (Продажа по всем у дереву)
                                and ost.op_date < Trunc(l_date_end)+ 1 + 2 / 24 and
                                ost.op_date >= Trunc(l_Date_Top) and
                                ost.lvl is Null then
                            -1
                           when ost.op_type = 23 -- Расход (утеря по всемку дереву)
                                and ost.st_sklad_1 = 13 and
                                ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < trunc(l_Date_end)+ 1 + 2 / 24 then
                            -1
                           when ost.op_type = 23 -- Расход (брак по всемку дереву)
                                and ost.st_sklad_1 = 14 and pi_tmc_type = 7004 and
                                ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < trunc(l_Date_end)+ 1 + 2 / 24 then
                            -1
                           when ost.op_type = 530 -- удаление
                                and ost.st_sklad_1 = 216 and
                               -- 40076
                                ost.st_sklad_0 <> 13 and
                                (pi_tmc_type <> 7004 or ost.st_sklad_0 <> 14) and
                                ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < trunc(l_Date_end)+ 1 + 2 / 24 then
                            -1
                           when ost.op_type = 529 -- замена сим-карты
                                and ost.st_sklad_1 = 12 and
                                ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < trunc(l_Date_end)+ 1 + 2 / 24 then
                            -1
                           else
                            0
                         end) ENDD_KOL,
                     Sum(Case
                           when ost.op_type in (20) -- Расход (ПЕРЕМЕЩЕНИЕ)
                                and ost.op_date < Trunc(l_date_end)+ 1 + 2 / 24 and
                                ost.op_date >= Trunc(l_Date_Top) and
                                ost.owner_id_0 in
                                (select column_value from table(l_org_tab)) then
                            1
                           else
                            0
                         end) RASHOD_PEREDANO,
                     -- расход (брак)
                     Sum(Case
                           when ost.op_date >= Trunc(l_Date_Top) and
                                ost.op_date < Trunc(l_Date_End)+ 1 + 2 / 24 and
                                ost.op_type = 23 and ost.st_sklad_1 = 14 then
                            1
                           else
                            0
                         end) BRAK
                From (Select distinct tto.op_type,
                                      tto.op_date - 2 / 24 op_date,
                                      -- 35948 (37477) Брендированное оборудование (телефоны)
                                      decode(t.tmc_type,
                                             7003,
                                             tp.model_id,
                                             4,
                                             tmu.usb_model,
                                             7002,
                                             ti.stb_model_id,
                                             decode(tto.op_type,
                                                    530,
                                                    tar_id_0,
                                                    decode(l.lvl,
                                                           1,
                                                           tou.tar_id_0,
                                                           tou.tar_id_1))) tar_id,
                                      tou.tar_id_1,
                                      tou.owner_id_0,
                                      tou.owner_id_1,
                                      tou.st_sklad_0,
                                      tou.st_sklad_1,
                                      l.lvl,
                                      0 as tmc_count,
                                      tou.unit_id
                        from t_tmc_operations tto
                        Join t_tmc_operation_units tou
                          on tou.op_id = tto.op_id
                        join t_tmc t
                          on tou.tmc_id = t.tmc_id
                        left join t_tmc_phone tp
                          on t.tmc_id = tp.tmc_id
                        left join t_tmc_modem_usb tmu
                          on t.tmc_id = tmu.tmc_id
                        left join t_tmc_iptv ti
                          on ti.tmc_id = t.tmc_id
                        left join (Select level lvl
                                    from dual
                                  connect by level <= 2) l
                          on (tto.op_type in (1901, 26, 1902) and
                             NVL(tou.tar_id_0, 0) <> NVL(tou.tar_id_1, 0))
                          or (tto.op_type = 22 and t.tmc_type = 8 and
                             tou.tar_id_0 is Null)
                        left join (Select level lvl_owner
                                    from dual
                                  connect by level <= 2) l1
                          on tto.op_type in (20, 21, 543, 544)
                        left join t_dogovor td
                          on td.dog_id = tto.op_dog_id
                        Join (select distinct tor.org_id,
                                             tor.org_reltype,
                                             tor.root_rel_id
                               from mv_org_tree tor
                              where tor.org_id in
                                    (select column_value from table(l_org_tab))
                                and tor.root_reltype not in
                                    (1006, 1009, 1005)) Dillers
                          on dillers.org_id = (case
                               when tto.op_type in (20, 21, 543, 544) and
                                    l1.lvl_owner = 1 then
                                tou.owner_id_0
                               when tto.op_type in (20, 21, 543, 544) and
                                    l1.lvl_owner = 2 then
                                tou.owner_id_1
                               when tto.op_type in (18, 24) then
                                tou.Owner_Id_1
                               when tto.op_type = 536 then
                                tou.Owner_Id_1
                               when tto.op_type in (1901, 26, 1902) then
                                tou.Owner_Id_1
                               when tto.op_type = 22 then
                                tou.Owner_Id_0
                               when tto.op_type = 23 and tou.st_sklad_1 = 13 then
                                tou.Owner_Id_0
                               when tto.op_type = 530 and tou.st_sklad_1 = 216 then
                                tou.Owner_Id_0
                               when tto.op_type = 23 and pi_tmc_type = 7004 and
                                    tou.st_sklad_1 = 14 then
                                tou.Owner_Id_0
                               when tto.op_type = 529 and tou.st_sklad_1 = 12 then
                                tou.Owner_Id_0
                               else
                                null
                             end)
                            ---------------------------------------------------------
                         and (td.dog_id is null or
                             td.org_rel_id = dillers.root_rel_id or
                             dillers.org_reltype = 1001)
                      ---------------------------------------------------------
                       Where tto.op_date < trunc(l_date_end) + 1 + 2 / 24
                         and tto.op_date >= trunc(l_date_cache) + 2 / 24
                         and nvl(tou.error_id, 0) = 0
                         and nvl(ti.priznak, t.tmc_type) = Pi_TMC_type
                      ----------- cache ----------------------
                      union all
                      Select 0,
                             null,
                             cache.tar_id,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             cache.tmc_count,
                             null unit_id
                        from t_tmc_opun_cach cache
                        Join (select column_value org_id from table(l_org_tab)) Dillers
                          on Dillers.org_id = cache.org_id
                       where cache.tmc_type = pi_tmc_type -- тут все нормально, хранится именно 7004
                            /*DECODE(pi_tmc_type, 7004, 7002, Pi_TMC_type)*/
                         and cache.c_date = l_date_cache) OST
               Group by ost.tar_id) T1
        left join t_model_phone mp
          on mp.model_id = t1.tar_id
         and pi_tmc_type = 7003
        left join t_modem_model_usb mmu
          on mmu.id = t1.tar_id
         and pi_tmc_type = 4
        left join t_stb_model sm
          on sm.id = t1.tar_id
         and pi_tmc_type = 7002
        left join t_ott_stb_model_info osm
          on osm.id = t1.tar_id
         and pi_tmc_type > 7003
        left join t_stb_model smm
          on smm.id = osm.model_stb_id
       where (nvl(BEG_KOL, 0) <> 0 or
             PRIHOD + PRIHOD_VOZVRAT + IZLISHKI_NEDOSTACHA +
             PRIHOD_PEREMESH + PRIHOD_TAR + VOZVRAT_CLIENTOM + RASHOD_TAR +
             RASHOD_VOZVRAT + PRODAGA + UTERYA + UDALENIE + ZAMENA_SIM +
             RASHOD_PEREDANO + BRAK <> 0)
       order by tar_name;
    return res;
  
  exception
    when ex_org_id_is_null then
      po_err_num := 1001;
      po_err_msg := 'Не верно задан ИД организации';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
    
  End Get_Report_Move_Tmc_Diller;

  --------------------------------------------------------------

  function Get_Sells(pi_date    in date,
                     po_err_num out pls_integer,
                     po_err_msg out varchar2) return sys_refcursor is
  
    res          sys_refcursor;
    l_date       date;
    l_gsm_right  num_tab;
    l_adsl_right num_tab;
  begin
  
    if pi_date is null then
      l_date := sysdate;
    else
      l_date := pi_date;
    end if;
  
    select tp.pr_right_id bulk collect
      into l_gsm_right -- продажа gsm
      from t_perm_rights tp
     where tp.pr_prm_id = 2000;
  
    select tp.pr_right_id bulk collect
      into l_adsl_right -- продажа adsl
      from t_perm_rights tp
     where tp.pr_prm_id = 2001;
  
    open res for
    
      Select org2.prefix PREFIX,
             nvl(t2.cnt_dil, 0) DILER,
             nvl(t1.cnt_gsm, 0) GSM,
             nvl(t4.cnt_adsl, 0) ADSL,
             (case
               when nvl(t1.change_status_date, trunc(sysdate)) <
                    nvl(t4.change_status_date, trunc(sysdate)) then
                (nvl(t1.cnt_gsm, 0) + nvl(t4.cnt_adsl, 0)) /
                (trunc(sysdate - 1) -
                nvl(t1.change_status_date, trunc(sysdate)))
               else
                (nvl(t1.cnt_gsm, 0) + nvl(t4.cnt_adsl, 0)) /
                (trunc(sysdate - 1) -
                nvl(t4.change_status_date, trunc(sysdate)))
             end) IN_DAY,
             nvl(t3.cnt_tp, 0) SALE_POINT
        from (Select *
                from t_org_relations vot
               where vot.Org_Id in (2001272,
                                    2001280,
                                    2001270,
                                    2001454,
                                    2001455,
                                    2001433,
                                    2001279)) org
      -- абоненты gsm
        left Join (Select t.Root_Org1,
                          nvl(count(*), 0) cnt_gsm,
                          nvl(min(trunc(tto.op_date)), trunc(sysdate - 1)) change_status_date
                     from (Select Substr(sys_connect_By_Path(vot.Org_Id, '\'),
                                         2,
                                         7) Root_Org1,
                                  vot.org_id
                             from t_org_relations vot
                           Connect By Prior vot.Org_Id = vot.Org_Pid
                            Start with vot.Org_Id in (2001272,
                                                      2001280,
                                                      2001270,
                                                      2001454,
                                                      2001455,
                                                      2001433,
                                                      2001279)) t
                     Join t_tmc_operation_units tou
                       on tou.owner_id_0 = t.org_id
                     join t_tmc_operations tto
                       on tou.op_id = tto.op_id
                      and tto.op_type = 22
                     join t_tmc tt
                       on tou.tmc_id = tt.tmc_id
                      and tt.tmc_type = c_tmc_sim_id
                    where trunc(tto.op_date) < trunc(l_date)
                      and tou.st_sklad_1 = 12
                    Group by t.Root_Org1) t1
          on org.Org_Id = t1.Root_Org1
      -- adsl подключения
        left Join (Select t.Root_Org1,
                          nvl(count(*), 0) cnt_adsl,
                          nvl(min(trunc(tto.op_date)), trunc(sysdate - 1)) change_status_date
                     from (Select Substr(sys_connect_By_Path(vot.Org_Id, '\'),
                                         2,
                                         7) Root_Org1,
                                  vot.org_id
                             from t_org_relations vot
                           Connect By Prior vot.Org_Id = vot.Org_Pid
                            Start with vot.Org_Id in (2001272,
                                                      2001280,
                                                      2001270,
                                                      2001454,
                                                      2001455,
                                                      2001433,
                                                      2001279)) t
                     Join t_tmc_operation_units tou
                       on tou.owner_id_0 = t.org_id
                     join t_tmc_operations tto
                       on tou.op_id = tto.op_id
                      and tto.op_type = 22
                     join t_tmc tt
                       on tou.tmc_id = tt.tmc_id
                      and tt.tmc_type = c_tmc_adsl_card_id
                    where trunc(tto.op_date) < trunc(l_date)
                      and tou.st_sklad_1 = 12
                    Group by t.Root_Org1) t4
          on org.Org_Id = t4.root_org1 --t1.Root_Org1
      -- дилеры
        left Join (Select t.Root_Org2, nvl(count(*), 0) cnt_dil
                     from (Select Substr(sys_connect_By_Path(vot.Org_Id, '\'),
                                         2,
                                         7) Root_Org2,
                                  vot.org_id,
                                  vot.org_reltype
                             from t_org_relations vot
                           Connect By Prior vot.Org_Id = vot.Org_Pid
                            Start with vot.Org_Id in (2001272,
                                                      2001280,
                                                      2001270,
                                                      2001454,
                                                      2001455,
                                                      2001433,
                                                      2001279)) t,
                          t_organizations org
                    where t.org_id = org.org_id
                      and (security_pkg.check_has_all_right(l_gsm_right,
                                                            security_pkg.get_orgs_right(org.org_id)) = 1 or
                          security_pkg.check_has_all_right(l_adsl_right,
                                                            security_pkg.get_orgs_right(org.org_id)) = 1)
                      and t.org_reltype in (1004, 999)
                      and is_org_usi(t.org_id) = 0
                      and trunc(org.regdate) < trunc(l_date)
                    Group by t.Root_Org2) t2
          on org.Org_Id = t2.Root_Org2
      -- точки продаж
        left Join (Select t.Root_Org3, nvl(count(*), 0) cnt_TP
                     from (Select Substr(sys_connect_By_Path(vot.Org_Id, '\'),
                                         2,
                                         7) Root_Org3,
                                  vot.org_id,
                                  vot.org_reltype
                             from t_org_relations vot
                           Connect By Prior vot.Org_Id = vot.Org_Pid
                            Start with vot.Org_Id in (2001272,
                                                      2001280,
                                                      2001270,
                                                      2001454,
                                                      2001455,
                                                      2001433,
                                                      2001279)) t,
                          t_organizations org
                    where t.org_id = org.org_id
                      and (security_pkg.check_has_all_right(l_gsm_right,
                                                            security_pkg.get_orgs_right(org.org_id)) = 1 or
                          security_pkg.check_has_all_right(l_adsl_right,
                                                            security_pkg.get_orgs_right(org.org_id)) = 1)
                      and t.org_reltype = 1001
                      and is_org_usi(t.org_id) = 0
                      and trunc(org.regdate) < trunc(l_date)
                    Group by t.Root_Org3) t3
          on org.Org_Id = t3.Root_Org3
      -- ФЭС
        join t_organizations org2
          on org.org_id = org2.org_id
       order by org2.prefix;
  
    return res;
  
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
    
  end Get_Sells;

  -----------------------------------------------------------------------------------------
  -- статистика продаж по единым картам оплаты

  function Get_Sells_EKO(pi_date    in date,
                         po_err_num out pls_integer,
                         po_err_msg out varchar2) return sys_refcursor is
  
    res         sys_refcursor;
    l_eko_right num_tab;
    l_date      date;
  
  begin
  
    if pi_date is null then
      l_date := sysdate;
    else
      l_date := pi_date;
    end if;
  
    select tp.pr_right_id bulk collect
      into l_eko_right -- продажа eko
      from t_perm_rights tp
     where tp.pr_prm_id = 2003;
  
    open res for
      Select org2.prefix PREFIX,
             nvl(t2.cnt_dil, 0) DILER,
             nvl(t1.cnt_eko, 0) EKO,
             nvl(t1.cnt_eko, 0) /
             (trunc(sysdate - 1) - nvl(t1.sale_date, trunc(sysdate))) IN_DAY,
             nvl(t3.cnt_tp, 0) SALE_POINT
        from (Select *
                from t_org_relations vot
               where vot.Org_Id in (2001272,
                                    2001280,
                                    2001270,
                                    2001454,
                                    2001455,
                                    2001433,
                                    2001279)) org
      -- дилеры ЕКО
        left Join (Select t.Root_Org2, nvl(count(*), 0) cnt_dil
                     from t_organizations org,
                          (Select Substr(sys_connect_By_Path(vot.Org_Id, '\'),
                                         2,
                                         7) Root_Org2,
                                  vot.org_id,
                                  vot.org_reltype
                             from t_org_relations vot
                           Connect By Prior vot.Org_Id = vot.Org_Pid
                            Start with vot.Org_Id in (2001272,
                                                      2001280,
                                                      2001270,
                                                      2001454,
                                                      2001455,
                                                      2001433,
                                                      2001279)) t
                    where t.org_id = org.org_id
                      and security_pkg.check_has_all_right(l_eko_right,
                                                           security_pkg.get_orgs_right(org.org_id)) = 1
                      and t.org_reltype in (1004, 999)
                      and is_org_usi(t.org_id) = 0
                      and trunc(org.regdate) < trunc(l_date)
                    Group by t.Root_Org2) t2
          on org.Org_Id = t2.Root_Org2
      -- проданные карты оплаты
        left Join (Select t.Root_Org1,
                          nvl(count(*), 0) cnt_eko,
                          nvl(min(trunc(tto.op_date)), trunc(sysdate - 1)) sale_date
                     from (Select Substr(sys_connect_By_Path(vot.Org_Id, '\'),
                                         2,
                                         7) Root_Org1,
                                  vot.org_id
                             from t_org_relations vot
                           Connect By Prior vot.Org_Id = vot.Org_Pid
                            Start with vot.Org_Id in (2001272,
                                                      2001280,
                                                      2001270,
                                                      2001454,
                                                      2001455,
                                                      2001433,
                                                      2001279)) t
                     Join t_tmc_operation_units tou
                       on tou.owner_id_0 = t.org_id
                     join t_tmc tt
                       on tou.tmc_id = tt.tmc_id
                      and tt.tmc_type = c_tmc_payd_card_id
                     join t_tmc_operations tto
                       on tou.op_id = tto.op_id
                      and tto.op_type = 22
                    where tou.st_sklad_1 = 12
                      and trunc(tto.op_date) < trunc(l_date)
                    Group by t.Root_Org1) t1
          on org.Org_Id = t1.Root_Org1
      -- точки продаж ЕКО
        left Join (Select t.Root_Org3, nvl(count(*), 0) cnt_TP
                     from t_organizations org,
                          (Select Substr(sys_connect_By_Path(vot.Org_Id, '\'),
                                         2,
                                         7) Root_Org3,
                                  vot.org_id,
                                  vot.org_reltype
                             from t_org_relations vot
                           Connect By Prior vot.Org_Id = vot.Org_Pid
                            Start with vot.Org_Id in (2001272,
                                                      2001280,
                                                      2001270,
                                                      2001454,
                                                      2001455,
                                                      2001433,
                                                      2001279)) t
                    where t.org_id = org.org_id
                      and security_pkg.check_has_all_right(l_eko_right,
                                                           security_pkg.get_orgs_right(org.org_id)) = 1
                      and t.org_reltype = 1001
                      and is_org_usi(t.org_id) = 0
                      and trunc(org.regdate) < trunc(l_date)
                    Group by t.Root_Org3) t3
          on org.Org_Id = t3.Root_Org3
      -- ФЭС
        join t_organizations org2
          on org.org_id = org2.org_id
       order by org2.prefix;
  
    return res;
  
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
    
  end Get_Sells_EKO;

  -----------------------------------------------------------------------------------------
  -- статистика по абонентскому обслуживанию (GSM)

  function Get_Ab_Serv(pi_date    in date,
                       po_err_num out pls_integer,
                       po_err_msg out varchar2) return sys_refcursor is
  
    res             sys_refcursor;
    l_ab_serv_right num_tab;
    l_date          date;
  
  begin
  
    if pi_date is null then
      l_date := sysdate;
    else
      l_date := pi_date;
    end if;
  
    select tp.pr_right_id bulk collect
      into l_ab_serv_right --абонентское обслуживание
      from t_perm_rights tp
     where tp.pr_prm_id = 4000;
  
    open res for
      Select org2.prefix PREFIX,
             nvl(t2.cnt_dil, 0) DILER,
             nvl(t1.cnt_op, 0) OP_COUNT
        from (Select *
                from t_org_relations vot
               where vot.Org_Id in (2001272,
                                    2001280,
                                    2001270,
                                    2001454,
                                    2001455,
                                    2001433,
                                    2001279)) org
      -- дилеры, работающие по оказанию абонентского обслуживания
        left Join (Select t.Root_Org2, nvl(count(*), 0) cnt_dil
                     from t_organizations org,
                          (Select Substr(sys_connect_By_Path(vot.Org_Id, '\'),
                                         2,
                                         7) Root_Org2,
                                  vot.org_id,
                                  vot.org_reltype
                             from t_org_relations vot
                           Connect By Prior vot.Org_Id = vot.Org_Pid
                            Start with vot.Org_Id in (2001272,
                                                      2001280,
                                                      2001270,
                                                      2001454,
                                                      2001455,
                                                      2001433,
                                                      2001279)) t
                    where t.org_id = org.org_id
                      and security_pkg.check_has_all_right(l_ab_serv_right,
                                                           security_pkg.get_orgs_right(org.org_id)) = 1
                      and t.org_reltype in (1004, 999)
                      and is_org_usi(t.org_id) = 0
                      and trunc(org.regdate) < trunc(l_date)
                    Group by t.Root_Org2) t2
          on org.Org_Id = t2.Root_Org2
      -- кол-во опаераций по абонентскому обслуживанию
        left Join (Select t.Root_Org1, nvl(count(*), 0) cnt_op
                     from t_ab_service tas,
                          (Select Substr(sys_connect_By_Path(vot.Org_Id, '\'),
                                         2,
                                         7) Root_Org1,
                                  vot.org_id
                             from t_org_relations vot
                           Connect By Prior vot.Org_Id = vot.Org_Pid
                            Start with vot.Org_Id in (2001272,
                                                      2001280,
                                                      2001270,
                                                      2001454,
                                                      2001455,
                                                      2001433,
                                                      2001279)) t
                    where t.org_id = tas.abs_org_id
                      and tas.abs_req_status = 2
                      and trunc(tas.abs_reg_time) < trunc(l_date)
                    Group by t.Root_Org1) t1
          on org.Org_Id = t1.Root_Org1
      -- ФЭС
        join t_organizations org2
          on org.org_id = org2.org_id
       order by org2.prefix;
  
    return res;
  
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
    
  end Get_Ab_Serv;

  -----------------------------------------------------------------------------------------
  -- статистика по операциям абонентского обслуживания

  function Get_Stat_Ab_Serv(pi_date    in date,
                            po_err_num out pls_integer,
                            po_err_msg out varchar2) return sys_refcursor is
  
    res    sys_refcursor;
    l_date date;
  
  begin
  
    if pi_date is null then
      l_date := sysdate;
    else
      l_date := pi_date;
    end if;
  
    open res for
      select das.type_req OP_NAME, count(*) AMOUNT
        from t_ab_service tas
        left join t_dic_ab_service das
          on tas.abs_req_type = das.id_req
       where tas.abs_req_status = 2
         and trunc(tas.abs_reg_time) < trunc(l_date)
         and tas.abs_org_id in
             (Select t.org_id
                from (Select Substr(sys_connect_By_Path(vot.Org_Id, '\'),
                                    2,
                                    7) Root_Org2,
                             vot.org_id,
                             vot.org_reltype
                        from t_org_relations vot
                      Connect By Prior vot.Org_Id = vot.Org_Pid
                       Start with vot.Org_Id in (2001272,
                                                 2001280,
                                                 2001270,
                                                 2001454,
                                                 2001455,
                                                 2001433,
                                                 2001279)) t
               where is_org_usi(t.org_id) = 0)
       group by das.type_req
       order by das.type_req;
  
    return res;
  
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
    
  end Get_Stat_Ab_Serv;
  --------------------------------------------------------------
  -- Отчёт "количество подключений в разрезе ТУЭСов"
  -- начальная дата включительно, конечная - не включительно (это необязательные параметры)
  function Get_Podklush_TUES(pi_date_beg  in date,
                             pi_date_end  in date,
                             pi_worker_id in number,
                             po_err_num   out pls_integer,
                             po_err_msg   out varchar2) return sys_refcursor is
  
    res sys_refcursor;
  
  begin
  
    open res for
      select nvl(tt.name, 'Прочие') TUES, count(ta.ab_id) CNT
        from t_abonent ta
        left join t_tues_calc_center tcc
          on ta.cc_id = tcc.id_cc
        left join t_tues tt
          on tcc.id_tues = tt.id
       where (pi_date_beg is null or
             trunc(ta.change_status_date - 2 / 24) >= trunc(pi_date_beg))
         and (pi_date_end is null or
             trunc(ta.change_status_date - 2 / 24) < trunc(pi_date_end))
         and ta.ab_status in (104, 105) -- одобрено АСРом
         and ta.is_deleted = 0
       group by tt.id, tt.name
       order by tt.name asc;
  
    return res;
  
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
    
  end Get_Podklush_TUES;
  --------------------------------------------------------------
  -- Перевызов. Отчет переехал в пакет REPORT_PERIOD. (16.03.2010)
  function Get_Ob_Sald_Ved(pi_org_pid   in number,
                           pi_date_beg  in date, -- включаем
                           pi_date_end  in date, -- не включаем
                           pi_worker_id in number,
                           po_err_num   out pls_integer,
                           po_err_msg   out varchar2) return sys_refcursor is
    res      sys_refcursor;
    l_prm_id number;
  begin
    res := report_period.Get_Ob_Sald_Ved(pi_org_pid,
                                         pi_date_beg, -- включаем
                                         pi_date_end, -- не включаем
                                         pi_worker_id,
                                         l_prm_id,
                                         po_err_num,
                                         po_err_msg);
    return res;
  end Get_Ob_Sald_Ved;

  --------------------------------------------------------------
  -- Перевызов. Отчет переехал в пакет REPORT_PERIOD. (16.03.2010)

  function Get_Agent_Poruchenie_Svod(pi_org_pid   in number,
                                     pi_date_beg  in date, -- включаем
                                     pi_date_end  in date, -- не включаем
                                     pi_worker_id in number,
                                     po_err_num   out pls_integer,
                                     po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  
  begin
    res := Report_Period.Get_Agent_Poruchenie_Svod(pi_org_pid,
                                                   pi_date_beg, -- включаем
                                                   pi_date_end, -- не включаем
                                                   pi_worker_id,
                                                   po_err_num,
                                                   po_err_msg);
    return res;
  end Get_Agent_Poruchenie_Svod;

  -----------------------------------------------------------------------------
  ---------------------- Несвязанные номера телефонов -------------------------
  function get_report_unrelated_numbers( --pi_region_id in number,
                                        pi_org_id       in array_num_2,
                                        pi_block        in number,
                                        pi_org_relation in num_tab,
                                        pi_worker_id    in number,
                                        po_err_num      out pls_integer,
                                        po_err_msg      out varchar2)
    return sys_refcursor is
    res             sys_refcursor;
    User_Orgs       num_tab;
    l_region_tab    num_tab;
    l_region_tab_in num_tab;
  begin
  
    logging_pkg.info('pi_org_id := ' || get_str_by_array_num_2(pi_org_id) || '; ' ||
                     'pi_block := ' || pi_block || '; ' ||
                     'pi_org_relation := ' ||
                     get_str_by_num_tab(pi_org_relation) || '; ' ||
                     'pi_worker_id := ' || pi_worker_id || '; ',
                     'get_report_unrelated_numbers');
  
    if (not Security_pkg.Check_User_Right_str('EISSD.TMC_REPORTS.UNRELATED_CALLSIGN',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
    
      return null;
    end if;
  
    User_Orgs := get_orgs_tab_for_multiset(pi_orgs         => pi_org_id,
                                           Pi_worker_id    => pi_worker_id,
                                           pi_block        => pi_block,
                                           pi_org_relation => pi_org_relation);
    -- Получаем регионы пришедших организаций
    select distinct o.region_id bulk collect
      into l_region_tab_in
      from t_organizations o
     where o.org_id in (select t.number_1 from table(pi_org_id) t)
       and o.region_id > 0;
    if l_region_tab_in.count = 0 then
      -- Определяем, не МРФ ли пришли и достаем регионы данных МРФ
      select distinct dr.reg_id bulk collect
        into l_region_tab_in
        from t_organizations o
        join t_dic_mrf dm
          on dm.org_id = o.org_id
        join t_dic_region dr
          on dr.mrf_id = dm.id
       where o.org_id in (select t.number_1 from table(pi_org_id) t);
    end if;
  
    select distinct o.region_id bulk collect
      into l_region_tab
      from table(User_Orgs) t
      join t_organizations o
        on o.org_id = t.column_value
       and o.region_id > 0;
    if l_region_tab_in.count <> 0 then
      l_region_tab := intersects(l_region_tab_in, l_region_tab);
    end if;
  
    open res for
    -- e/komissarov 11/12/2009 нужно брать значения из справочника
      select c.federal_callsign federal_callsign,
             cc.callsign_city city_callsign,
             d_c.name color,
             t.name_comm name_comm,
             o.org_id,
             o.org_name,
             max(o_pid.org_id) keep(dense_rank last order by nvl(rtm_pid.is_org_rtm, 1)) org_id_pid,
             max(o_pid.org_name) keep(dense_rank last order by nvl(rtm_pid.is_org_rtm, 1)) org_name_pid
        from t_callsign c
        left join t_callsign_city cc
          on cc.tmc_id = c.callsign_city_id
        left join t_commutators t
          on c.id_comm = t.id_comm
        left join t_dic_sim_color d_c
          on c.color = d_c.id
        left join t_tmc tt
          on c.tmc_id = tt.tmc_id
        left join t_organizations o
          on o.org_id = tt.org_id
        left join mv_org_tree mv
          on mv.org_id = o.org_id
         and mv.org_reltype in (1001, 1002, 1003, 1004, 1007, 1008, 999)
        left join t_organizations o_pid
          on mv.org_pid = o_pid.org_id
        left join t_org_is_rtmob rtm_pid
          on rtm_pid.org_id = o_pid.org_id
       where c.is_related = 0
         and c.region_id in (select /*+ PRECOMPUTE_SUBQUERY */
                              column_value
                               from table(l_region_tab))
         and tt.is_deleted = 0
       group by c.federal_callsign,
                cc.callsign_city,
                d_c.name,
                t.name_comm,
                o.org_id,
                o.org_name
       order by c.federal_callsign;
  
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
    
  end get_report_unrelated_numbers;

  -----------------------------------------------------------------------------
  -------------------------- отчет по активным ЕКО ----------------------------
  function get_report_active_paycard(pi_org_id    in t_organizations.org_id%type,
                                     pi_date_from in date,
                                     pi_date_to   in date,
                                     pi_child_inc in number,
                                     pi_worker_id in number,
                                     po_err_num   out pls_integer,
                                     po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
  
    if (not Security_pkg.Check_User_Right_str('EISSD.TMC_REPORT.ACTIVE_TMC',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      /*open res for
      select 1 from dual where 1 <> 1;*/
      return null;
    end if;
  
    open res for
      select tc.card_number,
             tc.card_ident,
             ts.org_id,
             org.org_name,
             tch.card_change_date - 2 / 24 card_change_date,
             tch.svc_type_id,
             (tch.pay_amount / 100) pay_amount
        from t_tmc                    t,
             t_tmc_pay_card           tc,
             t_org_tmc_status         ts,
             t_paydcard_status_change tch,
             t_organizations          org,
             mv_org_tree              otr,
             t_dogovor                dog,
             t_dogovor_prm            dpm
       where t.tmc_type = 6
         and t.tmc_id = ts.tmc_id
         and t.is_deleted = 0
         and ts.status = 11
         and ((ts.org_id in
             (SELECT tr.org_id
                  FROM mv_org_tree tr, t_organizations tog
                 where tr.org_id = tog.org_id
                 START WITH tr.org_id = pi_org_id
                CONNECT BY PRIOR tr.org_id = tr.org_pid) and
             pi_child_inc = 1) or
             (ts.org_id = pi_org_id and pi_child_inc = 0))
         and otr.org_id = ts.org_id
         and otr.root_rel_id = dog.org_rel_id
         and dpm.dp_dog_id = dog.dog_id
         and dpm.dp_is_enabled = 1
         and dpm.dp_prm_id = 2003
         and tch.card_change_date - 2 / 24 between pi_date_from and
             pi_date_to
         and org.org_id = ts.org_id
         and tc.tmc_id = t.tmc_id
         and tch.card_active_state = 1601 -- отключен для тестирования
         and tch.tmc_id = t.tmc_id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
  end get_report_active_paycard;
  -----------------------------------------------------------------------------
  -------------------------- Сводный отчет по ЕКО -----------------------------
  function get_report_svod_eko(pi_org_id    in t_organizations.org_id%type,
                               pi_date_from in date,
                               pi_date_to   in date,
                               pi_worker_id in number,
                               po_err_num   out pls_integer,
                               po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
  
    if (not Security_pkg.Check_User_Right_str('EISSD.TMC_REPORTS.PAYD_CARD_SUMMARY',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      /*open res for
      select 1 from dual where 1 <> 1;*/
      return null;
    end if;
  
    open res for
      select org.org_id,
             org.org_name,
             nvl(ccn.card_cnt, 0) sell_card_cnt,
             NVL(acc.card_cnt, 0) card_cnt,
             act.svc_type_id,
             NVL(act.act_cnt, 0) act_cnt,
             (act.act_sum / 100) act_sum
        from t_organizations org
             --,t_org_relations rel
            ,
             mv_org_tree   otr,
             t_dogovor     dog,
             t_dogovor_prm dpm,
             (           
             -- количество проданных карт
                          with tab as (select trr.*, grp.card_cnt
                                         from (SELECT tr.org_id,
                                                      Connect_By_Root(tr.org_id) porg_id
                                                 FROM mv_org_tree tr
                                               CONNECT BY PRIOR tr.org_id =
                                                           tr.org_pid) trr,
                                              (select orr.org_id,
                                                      count(tc.tmc_id) card_cnt
                                                 from t_tmc                 tmc,
                                                      t_tmc_pay_card        tc,
                                                      t_tmc_operation_units tou,
                                                      t_tmc_operations      top,
                                                      t_org_relations       orr
                                                where tmc.tmc_id = tou.tmc_id
                                                  and tmc.tmc_type = 6
                                                  and tmc.tmc_id = tc.tmc_id
                                                  and tou.op_id = top.op_id
                                                  and tou.owner_id_0 =
                                                      orr.org_id
                                                  and top.op_type = 22
                                                  and top.op_date - 2 / 24 between
                                                      pi_date_from and
                                                      pi_date_to
                                                group by orr.org_id) grp
                                        where trr.org_id = grp.org_id(+))
               select t.porg_id org_id, sum(t.card_cnt) card_cnt
                 from tab t
                Group by t.porg_id) ccn,(
               -- Количество активированных карт
               with tab as (select trr.*, grp.card_cnt
                              from (SELECT tr.org_id,
                                           Connect_By_Root(tr.org_id) porg_id
                                      FROM mv_org_tree tr
                                    --START WITH tr.org_pid = 0
                                    CONNECT BY PRIOR tr.org_id = tr.org_pid) trr,
                                   (select orr.org_id,
                                           count(tc.tmc_id) card_cnt
                                      from t_tmc                    t,
                                           t_tmc_pay_card           tc,
                                           t_org_tmc_status         ts,
                                           t_paydcard_status_change tch,
                                           t_org_relations          orr
                                     where t.tmc_type = 6
                                       and t.tmc_id = ts.tmc_id
                                       and ts.status = 11
                                       and orr.org_id = ts.org_id
                                       and tc.tmc_id = t.tmc_id
                                       and tch.tmc_id = t.tmc_id
                                       and tch.card_active_state = 1601
                                       and tch.card_change_date - 2 / 24 between
                                           pi_date_from and pi_date_to
                                     group by orr.org_id) grp
                             where trr.org_id = grp.org_id(+))
               select t.porg_id org_id, sum(t.card_cnt) card_cnt
                 from tab t
                Group by t.porg_id) acc,(
               -- Количество активаций
               with tab as (select trr.*,
                                   grp.svc_type_id,
                                   grp.act_cnt,
                                   grp.act_sum
                              from (SELECT tr.org_id,
                                           Connect_By_Root(tr.org_id) qwe
                                      FROM mv_org_tree tr
                                    --START WITH tr.org_id = 0
                                    CONNECT BY PRIOR tr.org_id = tr.org_pid) trr,
                                   (select ts.org_id,
                                           tch.svc_type_id,
                                           count(tc.tmc_id) as act_cnt,
                                           sum(tch.pay_amount) as act_sum
                                      from t_tmc                    t,
                                           t_tmc_pay_card           tc,
                                           t_org_tmc_status         ts,
                                           t_paydcard_status_change tch
                                     where t.tmc_type = 6
                                       and t.tmc_id = ts.tmc_id
                                       and ts.status = 11
                                       and tc.tmc_id = t.tmc_id
                                       and tch.tmc_id = t.tmc_id
                                       and tch.card_active_state = 1601
                                       and tch.card_change_date - 2 / 24 between
                                           pi_date_from and pi_date_to
                                     group by ts.org_id, tch.svc_type_id) grp
                             where trr.org_id = grp.org_id(+))
               select t.qwe as org_id,
                      t.svc_type_id,
                      sum(t.act_cnt) as act_cnt,
                      sum(t.act_sum) as act_sum
                 from tab t
                Group by t.qwe, t.svc_type_id) act
                where
               --rel.org_pid = pi_org_id
                otr.org_pid = pi_org_id
               
            and org.org_id = otr.org_id
            and dog.org_rel_id = otr.root_rel_id
            and dpm.dp_dog_id = dog.dog_id
            and dpm.dp_is_enabled = 1
            and dpm.dp_prm_id = 2003
               --and org.org_id = rel.org_id
            and org.org_id = ccn.org_id(+)
            and org.org_id = act.org_id(+)
            and org.org_id = acc.org_id(+);
  
  
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
  end get_report_svod_eko;

  --------------------------------------------------------------------
  function get_report_agent_paycards(pi_org_id    in t_organizations.org_id%type,
                                     pi_date_from in date,
                                     pi_date_to   in date,
                                     pi_worker_id in number,
                                     po_err_num   out pls_integer,
                                     po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    if (not Security_pkg.Check_User_Right_str('EISSD.TMC_REPORT.ACTIVE_TMC',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      /*open res for
      select 1 from dual where 1 <> 1;*/
      return null;
    end if;
  
    open res for
      select tab.card_full_cost, tab.card_cnt, tab.row_type, tab.data_type
        from (select card_full_cost,
                     count(tmc_id) CARD_CNT,
                     1 row_type,
                     'row' data_type
                from (select top.op_id,
                             top.org_id,
                             top.op_dog_id,
                             tou.tmc_id,
                             crd.card_number,
                             crd.card_full_cost
                        from (select *
                                from t_tmc_operations top
                               where top.op_type = 20
                              minus
                              select *
                                from t_tmc_operations top
                               where top.op_type = 21) top,
                             t_tmc_operation_units tou,
                             t_tmc tmc,
                             t_tmc_pay_card crd,
                             (SELECT tr.org_id
                                FROM mv_org_tree tr, t_organizations tog
                               where tr.org_id = tog.org_id
                               START WITH tr.org_id = pi_org_id
                              CONNECT BY PRIOR tr.org_id = tr.org_pid) orl
                       where tou.op_id = top.op_id
                         and top.org_id = orl.org_id
                         and tmc.tmc_id = tou.tmc_id
                         and tmc.tmc_type = 6 --8
                         and crd.tmc_id = tmc.tmc_id
                         and top.op_date < pi_date_from)
               group by card_full_cost
              union
              select sum(card_full_cost) card_full_cost,
                     count(tmc_id) CARD_CNT,
                     1 row_type,
                     'sum' data_type
                from (select top.op_id,
                             top.org_id,
                             top.op_dog_id,
                             tou.tmc_id,
                             crd.card_number,
                             crd.card_full_cost
                        from (select *
                                from t_tmc_operations top
                               where top.op_type = 20
                              minus
                              select *
                                from t_tmc_operations top
                               where top.op_type = 21) top,
                             t_tmc_operation_units tou,
                             t_tmc tmc,
                             t_tmc_pay_card crd,
                             (SELECT tr.org_id
                                FROM mv_org_tree tr, t_organizations tog
                               where tr.org_id = tog.org_id
                               START WITH tr.org_id = pi_org_id
                              CONNECT BY PRIOR tr.org_id = tr.org_pid) orl
                       where tou.op_id = top.op_id
                         and top.org_id = orl.org_id
                         and tmc.tmc_id = tou.tmc_id
                         and tmc.tmc_type = 6 --8
                         and crd.tmc_id = tmc.tmc_id
                         and top.op_date < pi_date_from)
              union
              select card_full_cost,
                     count(tmc_id) CARD_CNT,
                     2 row_type,
                     'row' data_type
                from (select top.op_id,
                             top.org_id,
                             top.op_dog_id,
                             tou.tmc_id,
                             crd.card_number,
                             crd.card_full_cost
                        from (select *
                                from t_tmc_operations top
                               where top.op_type = 20
                              minus
                              select *
                                from t_tmc_operations top
                               where top.op_type = 21) top,
                             t_tmc_operation_units tou,
                             t_tmc tmc,
                             t_tmc_pay_card crd,
                             (SELECT tr.org_id
                                FROM mv_org_tree tr, t_organizations tog
                               where tr.org_id = tog.org_id
                               START WITH tr.org_id = pi_org_id
                              CONNECT BY PRIOR tr.org_id = tr.org_pid) orl
                       where tou.op_id = top.op_id
                         and top.org_id = orl.org_id
                         and tmc.tmc_id = tou.tmc_id
                         and tmc.tmc_type = 6 -- 8
                         and crd.tmc_id = tmc.tmc_id
                         and top.op_date between pi_date_from and pi_date_to)
               group by card_full_cost
              union
              select sum(card_full_cost) card_full_cost,
                     count(tmc_id) CARD_CNT,
                     2 row_type,
                     'sum' data_type
                from (select top.op_id,
                             top.org_id,
                             top.op_dog_id,
                             tou.tmc_id,
                             crd.card_number,
                             crd.card_full_cost
                        from (select *
                                from t_tmc_operations top
                               where top.op_type = 20
                              minus
                              select *
                                from t_tmc_operations top
                               where top.op_type = 21) top,
                             t_tmc_operation_units tou,
                             t_tmc tmc,
                             t_tmc_pay_card crd,
                             (SELECT tr.org_id
                                FROM mv_org_tree tr, t_organizations tog
                               where tr.org_id = tog.org_id
                               START WITH tr.org_id = pi_org_id
                              CONNECT BY PRIOR tr.org_id = tr.org_pid) orl
                       where tou.op_id = top.op_id
                         and top.org_id = orl.org_id
                         and tmc.tmc_id = tou.tmc_id
                         and tmc.tmc_type = 6 -- 8
                         and crd.tmc_id = tmc.tmc_id
                         and top.op_date between pi_date_from and pi_date_to)
              union
              select card_full_cost,
                     count(tmc_id) CARD_CNT,
                     3 row_type,
                     'row' data_type
                from (select top.op_id,
                             top.org_id,
                             top.op_dog_id,
                             tou.tmc_id,
                             crd.card_number,
                             crd.card_full_cost
                        from (select *
                                from t_tmc_operations top
                               where top.op_type = 22
                              minus
                              select *
                                from t_tmc_operations top
                               where top.op_type = 21) top,
                             t_tmc_operation_units tou,
                             t_tmc tmc,
                             t_tmc_pay_card crd,
                             (SELECT tr.org_id
                                FROM mv_org_tree tr, t_organizations tog
                               where tr.org_id = tog.org_id
                               START WITH tr.org_id = pi_org_id
                              CONNECT BY PRIOR tr.org_id = tr.org_pid) orl
                       where tou.op_id = top.op_id
                         and top.org_id = orl.org_id
                         and tmc.tmc_id = tou.tmc_id
                         and tmc.tmc_type = 6 --8
                         and crd.tmc_id = tmc.tmc_id
                         and top.op_date between pi_date_from and pi_date_to)
               group by card_full_cost
              union
              select sum(card_full_cost) card_full_cost,
                     count(tmc_id) CARD_CNT,
                     3 row_type,
                     'sum' data_type
                from (select top.op_id,
                             top.org_id,
                             top.op_dog_id,
                             tou.tmc_id,
                             crd.card_number,
                             crd.card_full_cost
                        from (select *
                                from t_tmc_operations top
                               where top.op_type = 22
                              minus
                              select *
                                from t_tmc_operations top
                               where top.op_type = 21) top,
                             t_tmc_operation_units tou,
                             t_tmc tmc,
                             t_tmc_pay_card crd,
                             (SELECT tr.org_id
                                FROM mv_org_tree tr, t_organizations tog
                               where tr.org_id = tog.org_id
                               START WITH tr.org_id = pi_org_id
                              CONNECT BY PRIOR tr.org_id = tr.org_pid) orl
                       where tou.op_id = top.op_id
                         and top.org_id = orl.org_id
                         and tmc.tmc_id = tou.tmc_id
                         and tmc.tmc_type = 6 -- 8
                         and crd.tmc_id = tmc.tmc_id
                         and top.op_date between pi_date_from and pi_date_to)
              
              union
              select card_full_cost,
                     count(tmc_id) CARD_CNT,
                     4 row_type,
                     'row' data_type
                from (select top.op_id,
                             top.org_id,
                             top.op_dog_id,
                             tou.tmc_id,
                             crd.card_number,
                             crd.card_full_cost
                        from (select *
                                from t_tmc_operations top
                               where top.op_type = 20
                              minus
                              select *
                                from t_tmc_operations top
                               where top.op_type = 21) top,
                             t_tmc_operation_units tou,
                             t_tmc tmc,
                             t_tmc_pay_card crd,
                             (SELECT tr.org_id
                                FROM mv_org_tree tr, t_organizations tog
                               where tr.org_id = tog.org_id
                               START WITH tr.org_id = pi_org_id
                              CONNECT BY PRIOR tr.org_id = tr.org_pid) orl
                       where tou.op_id = top.op_id
                         and top.org_id = orl.org_id
                         and tmc.tmc_id = tou.tmc_id
                         and tmc.tmc_type = 6 --8
                         and crd.tmc_id = tmc.tmc_id
                         and top.op_date > pi_date_to)
               group by card_full_cost
              union
              select sum(card_full_cost) card_full_cost,
                     count(tmc_id) CARD_CNT,
                     4 row_type,
                     'sum' data_type
                from (select top.op_id,
                             top.org_id,
                             top.op_dog_id,
                             tou.tmc_id,
                             crd.card_number,
                             crd.card_full_cost
                        from (select *
                                from t_tmc_operations top
                               where top.op_type = 20
                              minus
                              select *
                                from t_tmc_operations top
                               where top.op_type = 21) top,
                             t_tmc_operation_units tou,
                             t_tmc tmc,
                             t_tmc_pay_card crd,
                             (SELECT tr.org_id
                                FROM mv_org_tree tr, t_organizations tog
                               where tr.org_id = tog.org_id
                               START WITH tr.org_id = pi_org_id
                              CONNECT BY PRIOR tr.org_id = tr.org_pid) orl
                       where tou.op_id = top.op_id
                         and top.org_id = orl.org_id
                         and tmc.tmc_id = tou.tmc_id
                         and tmc.tmc_type = 6 -- 8
                         and crd.tmc_id = tmc.tmc_id
                         and top.op_date > pi_date_to)) tab
       order by ROW_TYPE, DATA_TYPE, card_full_cost;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
  end get_report_agent_paycards;
  ----------------------------------------------------------------------------
  function Get_act_sverki_acc(pi_org_id   in number,
                              pi_org_pid  in number,
                              pi_dog_id   in number,
                              pi_date_beg in date,
                              pi_date_end in date) return sys_refcursor is
    res       sys_refcursor;
    l_org_id  number := pi_org_id;
    l_org_pid number := pi_org_pid;
    l_dog_id  number := pi_dog_id;
  begin
  
    if (pi_org_id is null or pi_org_pid is null) then
      orgs.Get_Orgs_By_Dog(l_dog_id, l_org_id, l_org_pid);
    end if;
  
    open res for
      select 'Входящее сальдо' doc_type,
             pi_org_id org_id,
             pi_date_beg date_pay,
             null bank_doc_num,
             null cash_doc_num,
             (case
               when acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5001) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5004) +
                   -- 66809 Чтобы совпадало с шахматкой, т.к. там учтены все счета
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5006) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5007) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5008) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5009) < 0 then
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5001) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5004) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5006) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5007) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5008) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5009)
               else
                0
             end) debet,
             (case
               when acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5001) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5004) +
                   -- 66809 Чтобы совпадало с шахматкой, т.к. там учтены все счета
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5006) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5007) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5008) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5009) > 0 then
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5001) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5004) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5006) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5007) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5008) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5009)
               else
                0
             end) kredit
        from dual
      
      union all
      select dv.dv_name as doc_type,
             tor.org_id,
             t.pay_date date_pay,
             null bank_doc_num,
             null cash_doc_num,
             (case
               when t.tr_type = 6000 then
                0
               else
                abs(t.amount)
             end) debet,
             (case
               when t.tr_type = 6000 then
                abs(t.amount)
               else
                0
             end) kredit
        from t_acc_pay t
        join t_accounts ta
          on ta.acc_id = t.acc_id
        join t_acc_owner tao
          on tao.owner_id = ta.acc_owner_id
        join t_dogovor td
          on td.dog_id = tao.owner_ctx_id
         and td.dog_id = pi_dog_id
        join t_org_relations tor
          on tor.id = td.org_rel_id
        join t_organizations torg
          on torg.org_id = tor.org_id
         and torg.org_id = pi_org_id
        join t_dic_values dv
          on dv.dv_id = t.tr_type
       where t.tr_type in (6000, 6007, 6019, 6020, 6021, 6033)
         and t.pay_date between pi_date_beg and pi_date_end
      
      union
      select 'Исходящее сальдо' doc_type,
             pi_org_id org_id,
             pi_date_end date_pay,
             null bank_doc_num,
             null cash_doc_num,
             (case
               when acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_end,
                                                  5001) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_end,
                                                  5004) +
                   -- 66809 Чтобы совпадало с шахматкой, т.к. там учтены все счета
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5006) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5007) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5008) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5009) < 0 then
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_end, 5001) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_end, 5004) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5006) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5007) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5008) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5009)
               else
                0
             end) debet,
             (case
               when acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_end,
                                                  5001) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_end,
                                                  5004) +
                   -- 66809 Чтобы совпадало с шахматкой, т.к. там учтены все счета
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5006) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5007) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5008) +
                    acc_operations.Get_acc_Saldo2(pi_dog_id,
                                                  pi_date_beg,
                                                  5009) > 0 then
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_end, 5001) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_end, 5004) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5006) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5007) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5008) +
                acc_operations.Get_acc_Saldo2(pi_dog_id, pi_date_beg, 5009)
               else
                0
             end) kredit
        from dual
       order by date_pay;
    return res;
  end;
  ---------------------------------------------------------------------------------
  function get_supernet_report(pi_org_id    in t_organizations.org_id%type,
                               pi_date_from in date,
                               pi_date_to   in date,
                               pi_worker_id in number,
                               po_err_num   out pls_integer,
                               po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select du.svc_adsl adsl_num,
             du.svc_gsm phone,
             du.date_on action_date,
             'Подключение' action,
             (select p.person_firstname || ' ' || p.person_middlename || ' ' ||
                     p.person_lastname
                from t_person p, t_users u
               where u.usr_id = du.worker_id_on
                 and p.person_id = u.usr_person_id) worker_fio
        from t_dujet_users du
       where ( /*not (du.worker_id_on is null)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      and*/
              (trunc(du.date_on) >= trunc(pi_date_from) and
              trunc(du.date_on) < trunc(pi_date_to)))
      union
      select du.svc_adsl adsl_num,
             du.svc_gsm phone,
             du.date_off action_date,
             'Отключение' action,
             (select p.person_firstname || ' ' || p.person_middlename || ' ' ||
                     p.person_lastname
                from t_person p, t_users u
               where u.usr_id = du.worker_id_off
                 and p.person_id = u.usr_person_id) worker_fio
        from t_dujet_users du
       where ( /*not (du.worker_id_off is null)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      and */
              (trunc(du.date_off) >= trunc(pi_date_from) and
              trunc(du.date_off) < trunc(pi_date_to)))
       order by action_date;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
  end get_supernet_report;

  ------------------------------------------------------------------------------------
  /*51299 - общий отчет по активным продавцам*/
  function getReportSellerActiveFull(pi_org_id          in t_organizations.org_id%type,
                                     pi_date_begin      in date,
                                     pi_date_end        in date,
                                     pi_sale_type       in number, --тип продажи (1 - GSM , 2 - PSTN)
                                     pi_is_staff        in number,
                                     pi_parents_include in number, --1 - с подчиненными организациями
                                     pi_worker_id       in number,
                                     po_err_num         out number,
                                     po_err_msg         out varchar2)
    return sys_refcursor is
    orgs_tab num_tab;
    res      sys_refcursor;
  begin
  
    if (not Security_pkg.Check_User_Right_str('EISSD.SELLER_ACTIVE.VIEW_REPORT',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;
  
    if (pi_parents_include = 1) then
      select t.org_id bulk collect
        into orgs_tab
        from mv_org_tree t
      connect by prior t.org_id = t.org_pid
       start with t.org_id = pi_org_id;
      --orgs_tab := orgs.Get_Orgs(pi_worker_id, pi_org_id, 0, 1, 1, 0, 0, 1);
    else
    
      Select pi_org_id bulk collect into orgs_tab from dual;
    end if;
  
    if (pi_sale_type = 1) then
      open res for
        Select max(p.person_lastname || ' ' || p.person_firstname || ' ' ||
                   p.person_middlename || ' (' || sa.sa_emp_num || ')') seller_active_fio,
               max(sa.sa_emp_num) sa_emp_num,
               max(nvl(sa.sa_is_staff, is_org_usi(su.su_org_id))) is_staff,
               count(*) cnt,
               sum(ab.ab_cost) sum_cost,
               -- 58777
               /*max(*/
               tsu.person_lastname || ' ' || tsu.person_firstname || ' ' ||
               tsu.person_middlename || ' (' || su.su_emp_num || ')' /*)*/ supervisor_fio
          from t_abonent ab
          join t_seller_active sa
            on ab.seller_active_id = sa.sa_id
          join t_person p
            on sa.sa_person_id = p.person_id
        -- 58777
          join t_seller_active_rel sar
            on sar.sa_id = sa.sa_id
           and ab.ab_reg_date >= sar.date_from
           and ab.ab_reg_date < nvl(sar.date_to, sysdate)
          join t_supervisor su
            on su.su_id = sar.su_id
          join t_person tsu
            on tsu.person_id = su.su_person_id
         where su.su_org_id in (Select * from TABLE(orgs_tab))
           and (pi_is_staff is null or
               nvl(sa.sa_is_staff, is_org_usi(su.su_org_id)) = pi_is_staff)
           and trunc(ab.ab_Dog_Date) >= pi_date_begin
           and trunc(ab.ab_Dog_Date) <= pi_date_end
        --and ab.AB_STATUS_EX in (104, 105)
         group by sa.sa_id,
                  tsu.person_lastname || ' ' || tsu.person_firstname || ' ' ||
                  tsu.person_middlename || ' (' || su.su_emp_num || ')';
    elsif (pi_sale_type = 2) then
      open res for
        select max(p.person_lastname || ' ' || p.person_firstname || ' ' ||
                   p.person_middlename || ' (' || sa.sa_emp_num || ')') seller_active_fio,
               max(sa.sa_emp_num) sa_emp_num,
               max(nvl(sa.sa_is_staff, is_org_usi(su.su_org_id))) is_staff,
               sum(ss.cnt) cnt,
               nvl(max(ss.advance_sum + ss.once_cost), 0) sum_cost,
               -- 58777
               /*max(*/
               tsu.person_lastname || ' ' || tsu.person_firstname || ' ' ||
               tsu.person_middlename || ' (' || su.su_emp_num || ')' /*)*/ supervisor_fio
          from tr_request cc
          join t_seller_active sa
            on cc.seller_id = sa.sa_id
          join t_person p
            on p.person_id = sa.sa_person_id
        -- 58777
          join t_seller_active_rel sar
            on sar.sa_id = sa.sa_id
           and cc.date_create >= sar.date_from
           and cc.date_create < nvl(sar.date_to, sysdate)
          join t_supervisor su
            on su.su_id = sar.su_id
          join t_person tsu
            on tsu.person_id = su.su_person_id
          join (Select max(cu.request_id) request_id,
                       sum(op.fee) advance_sum,
                       sum(op.cost) once_cost,
                       count(*) cnt
                  from tr_request_service cu
                  left join tr_product_option op
                    on op.service_id = cu.id
                 group by cu.request_id) ss
            on ss.request_id = cc.id
         where su.su_org_id in (Select * from TABLE(orgs_tab))
           and (pi_is_staff is null or
               nvl(sa.sa_is_staff, is_org_usi(su.su_org_id)) = pi_is_staff)
           and trunc(cc.date_create /*dogovor_date*/) >= pi_date_begin
           and trunc(cc.date_create /*dogovor_date*/) <= pi_date_end
         group by sa.sa_id,
                  tsu.person_lastname || ' ' || tsu.person_firstname || ' ' ||
                  tsu.person_middlename || ' (' || su.su_emp_num || ')';
    end if;
    return res;
  EXCEPTION
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm;
      return null;
  end getReportSellerActiveFull;

  ---------------------------------------------------------------------------------------
  -- Рутовая организация для отчетов
  ---------------------------------------------------------------------------------------
  function GetRootOrg(pi_org_id in number) return number is
    res number;
  begin
    select max(orgR.org_id)
      into res
      from mv_org_tree     tree,
           t_organizations orgR,
           t_dic_region    dr,
           t_dic_mrf       dm,
           t_dic_mrf       dm1
     where tree.org_id = pi_org_id
       and dr.org_id(+) = tree.org_id
       and dm.org_id(+) = tree.org_pid
       and dm1.org_id(+) = tree.org_id
       and (case is_org_usi(tree.org_id)
             when 0 then
              decode(tree.org_reltype,
                     1004,
                     decode(tree.org_pid,
                            0,
                            0,
                            decode(dr.org_id,
                                   null,
                                   decode(dm.org_id,
                                          null,
                                          tree.org_id,
                                          tree.org_pid),
                                   tree.org_pid)),
                     999,
                     decode(tree.org_pid,
                            0,
                            0,
                            decode(dr.org_id,
                                   null,
                                   decode(dm.org_id,
                                          null,
                                          tree.org_id,
                                          tree.org_pid),
                                   tree.org_pid)),
                     tree.root_org_id)
             when 1 then
              decode(dr.org_id,
                     null,
                     decode(tree.org_id,
                            2001825,
                            tree.org_id,
                            decode(dm1.org_id,
                                   null,
                                   decode(tree.org_id,
                                          0,
                                          tree.org_id,
                                          tree.org_pid),
                                   tree.org_id)),
                     tree.org_id)
           end) = orgR.Org_Id;
    return res;
  end GetRootOrg;
end REPORT;
/
