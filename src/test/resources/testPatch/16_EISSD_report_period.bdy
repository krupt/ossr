CREATE OR REPLACE PACKAGE BODY REPORT_PERIOD is
  c_package constant varchar2(30) := 'REPORT_PERIOD.';
  -------------------------------------------------------------------------------
  procedure fixActivAbonent(pi_id_repper     in t_report_period.id%type,
                            pi_id_repper_cur in t_report_period.id%type) is
  begin
    insert into t_report_activ
      select ab.root_org_id,
             t.tar_id tar_id_1,
             0 priz,
             Sum(Case
                   when ta.amount_pay >= 30 and trep.premia_schema <> 12 then
                    1
                   when ta.amount_pay >= 100 and trep.premia_schema = 12 then
                    1
                   else
                    0
                 End) kol,
             t.id_repper,
             trep.id,
             1
        from t_report_real_conn t,
             t_ab_pay_activity  ta,
             t_abonent          ab,
             t_report_period    trep
       where trep.id = pi_id_repper_cur
         and t.id_repper = pi_id_repper
         and ta.abonent_id = t.id_conn
         and ta.asr_id > 0
         and trep.dates = trunc(ta.date_begin)
         and trunc(trep.datepo) = trunc(ta.date_end)
         and ab.ab_id = t.id_conn
         and t.ci = 1
         and trep.is_actual = 1
       group by ab.root_org_id, trep.id, t.id_repper, t.tar_id;
  end fixActivAbonent;
  -------------------------------------------------------------------------------
  function isNotEmtyConnection(pi_id_org in t_report_connection.id_org%type,
                               pi_datepo in t_report_period.datepo%type)
    return number is
    res   number := 0;
    l_kol number;
  begin
    begin
      select sum(kol)
        into l_kol
        from (select count(*) kol
                from t_abonent t
               where t.is_deleted = 0
                 and t.ab_status in (104, 105)
                 and t.root_org_id = pi_id_org
                 and t.change_status_date is not null
                 and t.change_status_date < pi_datepo + 1
                 and not exists (select /* INDEX(trep IDX_ID_CONN_REAL_CI) */
                       null
                        from t_report_real_conn trep
                       where trep.id_conn = t.ab_id
                         and trep.ci = 1));
      if (l_kol > 0) then
        res := 1;
      end if;
    exception
      when NO_DATA_FOUND then
        l_kol := 0;
    end;
    begin
      select count(rc.id_conn)
        into l_kol
        from t_report_period rp, t_report_real_conn rc
       where rp.id_org = pi_id_org
         and rp.id = rc.id_repper
         and rp.is_actual = 1
         and rp.dates >= ADD_MONTHS(trunc(pi_datepo, 'MONTH'), -5);
      if (l_kol > 0 and res = 0) then
        res := 1;
      end if;
    exception
      when NO_DATA_FOUND then
        null;
    end;
    return res;
  exception
    when NO_DATA_FOUND then
      return 0;
  end isNotEmtyConnection;
  -------------------------------------------------------------------------------
  function GetOrgReportPeriod(pi_datepo in t_report_period.datepo%type)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select sel.org_id, org.org_name
        from (select distinct t.org_id org_id
                from t_abonent t
               where t.is_deleted = 0
                 and t.ab_dog_date <= pi_datepo
                 and t.ab_id not in (select trep.id_conn
                                       from t_report_real_conn trep
                                      where trep.ci = 1)) sel,
             t_organizations org
       where sel.org_id = org.org_id
       order by org.org_name;
    return res;
  end GetOrgReportPeriod;
  -------------------------------------------------------------------------------
  function FixReportPeriod(pi_dates      in t_report_period.dates%type,
                           pi_datepo     in t_report_period.datepo%type,
                           pi_id_org     in t_report_connection.id_org%type,
                           pi_worker_id  in T_USERS.USR_ID%type,
                           pi_dog_id     in t_dogovor.dog_id%type,
                           pi_rep_filial in number, --1-признак того что отчет по филиалу(РТМ)
                           po_err_num    out pls_integer,
                           po_err_msg           out t_Err_Msg,
                           pi_with_blocked_orgs NUMBER := 0) return number is
    l_repper      number;
    l_dates       date;
    l_datepo      date;
    l_counter     number;
    l_repper_calc number;
    l_plan        number;
    --l_plan_percent  number;
    l_count_act_ps  number;
    l_count_act_mis number;
    l_count         number;
    err_msg         varchar2(2000);
    l_dog_class     number := 0;
    l_sale_equip    number;
  begin
    --Помечаю как не актуальный уже закрытый ранее период. Neveroff 29.03.2010
    update t_report_period t
       set t.is_actual = 0
     where trunc(t.dates) = pi_dates
       and Trunc(t.datepo) = pi_datepo
       and t.id_org = pi_id_org
       and t.dog_id = pi_dog_id;
    select max(t.dog_class_id)
      into l_dog_class
      from t_dogovor t
     where t.dog_id = pi_dog_id;
    savepoint sp_begin;
    --открываю его по-новой...
    insert into t_report_period
      (dates,
       datepo,
       datereg,
       id_org,
       premia_schema,
       pr_nds,
       dog_id,
       is_actual,
       plan,
       status)
    values
      (pi_dates,
       pi_datepo,
       sysdate,
       pi_id_org,
       (Select Max(dog.premia_schema)
          from t_dogovor dog
         Where dog.dog_id = pi_dog_id),
       (Select Decode(NVL(Max(dog.with_nds), 0), 0, 0, 0.18)
          from t_dogovor dog
         Where dog.dog_id = pi_dog_id),
       pi_dog_id,
       1,
       l_plan,
       --отчет формируется
       1)
    returning ID into l_repper;
    if nvl(pi_rep_filial, 0) = 0 then
      --если действуют ставки с учетом траффиковой активности
      --в отчет войдут только подключения, активированные в отчетном месяце
      if stavka_pkg.Is_With_Traffic_Report(pi_dates,
                                           pi_id_org,
                                           pi_dog_id,
                                           po_err_num,
                                           err_msg) = 0 then

        insert into t_report_real_conn
          (id_conn, id_repper, ci, tar_id)
          select t.ab_id, l_repper, 1, ts.tar_id
            from t_abonent t
            Join t_tmc_operations o
              on o.Op_Id = t.id_op
            Join t_tmc_operation_units tou
              on tou.op_id = o.op_id
            join t_tmc_sim ts
              on ts.tmc_id = tou.tmc_id
           where t.is_deleted = 0
             and t.ab_status in (104, 105)
             and Nvl(tou.error_id, 0) = 0
             and ((l_dog_class <> 8 and t.root_org_id = pi_id_org) or
                 l_dog_class = 8 and pi_id_org = t.org_id)
             and t.change_status_date is not null
             and t.change_status_date - 2 / 24 < pi_datepo + 1
             and t.change_status_date - 2 / 24 > = pi_dates
             and o.op_dog_id = pi_dog_id;
      else
        insert into t_report_real_conn
          (id_conn, id_repper, ci, tar_id)
          select t.ab_id, l_repper, 1, ts.tar_id
            from t_abonent t
            Join t_tmc_operations o
              on o.Op_Id = t.id_op
            Join t_tmc_operation_units tou
              on tou.op_id = o.op_id
            join t_tmc_sim ts
              on ts.tmc_id = tou.tmc_id
            join t_abonent_activated aa
              on aa.abonent_id = t.ab_id
           where t.is_deleted = 0
             and t.ab_status in (104, 105)
             and Nvl(tou.error_id, 0) = 0
             and ((l_dog_class <> 8 and t.root_org_id = pi_id_org) or
                 l_dog_class = 8 and pi_id_org = t.org_id)
             and t.change_status_date is not null
             and t.change_status_date - 2 / 24 < pi_datepo + 1
             and ((aa.data_activated - 2 / 24 between pi_dates and
                 add_months(pi_dates, 1) - 1 / 24 / 60 / 60 and
                 t.change_status_date >=
                 to_date('01.08.2012', 'dd.mm.yyyy')) or
                 (aa.data_activated - 2 / 24 < pi_dates and
                 t.change_status_date - 2 / 24 between pi_dates and
                 add_months(pi_dates, 1) - 1 / 24 / 60 / 60))
             and t.ab_id not in
                 (select trep.id_conn
                    from t_report_real_conn trep
                    join t_report_period rp
                      on trep.id_repper = rp.id
                     and rp.is_actual = 1
                    left join t_dic_region r
                      on r.org_id = rp.id_org
                   where trep.ci = 1
                     and ( /*r.reg_id is null */
                          rp.id_org not in (2001270,
                                            2001272,
                                            2001279,
                                            2001280,
                                            2001433,
                                            2001455,
                                            2001454,
                                            2004855,
                                            2004866,
                                            2004884)))
             and o.op_dog_id = pi_dog_id;
      end if;
    else
      select count(*)
        into l_count
        from t_dogovor_prm dp
       where dp.dp_prm_id = 2000
         and dp.dp_is_enabled = 1
         and dp.dp_dog_id = pi_dog_id;
      if l_count > 0 then
        if stavka_pkg.Is_With_Traffic_Report(pi_dates,
                                             pi_id_org,
                                             pi_dog_id,
                                             po_err_num,
                                             err_msg) = 0 then

          insert into t_report_real_conn
            (id_conn, id_repper, ci, tar_id)
            select t.ab_id, l_repper, 1, ts.tar_id
              from t_abonent t
              join t_tmc_sim ts
                on ts.tmc_id = t.ab_tmc_id
             where t.is_deleted = 0
               and t.ab_status in (104, 105)
               and t.org_id in
                   (select tor.org_id
                      from mv_org_tree tor
                      join t_org_is_rtmob rt
                        on rt.org_id = tor.org_id
                     where rt.is_org_rtm = 0 and (tor.dog_id is null or tor.dog_id = pi_dog_id)
                    connect by prior tor.org_id = tor.org_pid
                     start with tor.org_id in (pi_id_org))
               and t.change_status_date is not null
               and t.change_status_date - 2 / 24 < pi_datepo + 1
               and t.change_status_date - 2 / 24 > = pi_dates;
        else
          insert into t_report_real_conn
            (id_conn, id_repper, ci, tar_id)
            select t.ab_id, l_repper, 1, ts.tar_id
              from t_abonent t
              join t_tmc_sim ts
                on ts.tmc_id = t.ab_tmc_id
              join t_abonent_activated aa
                on aa.abonent_id = t.ab_id
             where t.is_deleted = 0
               and t.ab_status in (104, 105)
               and t.change_status_date is not null
               and t.change_status_date - 2 / 24 < pi_datepo + 1
               and ((aa.data_activated - 2 / 24 between pi_dates and
                   add_months(pi_dates, 1) - 1 / 24 / 60 / 60 and
                   t.change_status_date >=
                   to_date('01.08.2012', 'dd.mm.yyyy')) or
                   (aa.data_activated - 2 / 24 < pi_dates and
                   t.change_status_date - 2 / 24 between pi_dates and
                   add_months(pi_dates, 1) - 1 / 24 / 60 / 60))
               and t.org_id in
                   (select tor.org_id
                      from mv_org_tree tor
                      join t_org_is_rtmob rt
                        on rt.org_id = tor.org_id
                     where rt.is_org_rtm = 0 and (tor.dog_id is null or tor.dog_id = pi_dog_id)
                    connect by prior tor.org_id = tor.org_pid
                     start with tor.org_id in (pi_id_org));
        end if;
      end if;
    end if;
    -- 31212 Абонентское обслуживание 02.06.2011
    select count(*)
      into l_count
      from t_dogovor_prm dp
     where dp.dp_prm_id = 4000
       and dp.dp_is_enabled = 1
       and dp.dp_dog_id = pi_dog_id;
    if l_count > 0 then
      -- Заполнение t_report_real_conn
      insert into t_report_real_conn
        (id_conn, id_repper, ci, tar_id)
        select distinct ab.id, l_repper, 3, ab.req_type
          from t_ab_service_remote ab
          join mv_org_tree org
            on ab.org_id = org.org_id
          left join t_service_option_remote_cost c
            on c.id_ab_remote = ab.id
           and c.state_id = 0
          left join t_ab_service_option cc
            on cc.id_ab_remote = ab.id
           and nvl(c.state_id, 0) = 0
         where org.root_org_id = pi_id_org
           and ab.reg_time is not null
           and (ab.req_type = 101 or c.id_ab_remote is not null or
               cc.id_ab_remote is not null)
           and ab.reg_time between pi_dates + 2 / 24 and
               pi_datepo + 2 / 24 + 1 - 1 / 24 / 60 / 60
           and ab.id not in (select trep.id_conn
                               from t_report_real_conn trep
                               join t_report_period rp
                                 on trep.id_repper = rp.id
                                and rp.is_actual = 1
                              where trep.ci = 3)
        /*and ab.req_status = 10002*/
        ;
    end if;
    --48025 -адсл
    select count(*)
      into l_count
      from t_dogovor_prm dp
     where dp.dp_prm_id in (2001, 2002, 2007, 2008, 2009, 2010, 8205, 2500)
       and dp.dp_is_enabled = 1
       and dp.dp_dog_id = pi_dog_id;
    if l_count > 0 then
      -- Заполнение t_report_real_conn
      insert into t_report_real_conn
        (id_conn, id_repper, ci, tar_id)
        select distinct rs.id, l_repper, 2, null
          from tr_request r
          join tr_request_service rs
            on rs.request_id = r.id
         where r.org_id in (select /*+ PRECOMPUTE_SUBQUERY */
                             tor.org_id
                              from t_org_relations tor
                              where tor.dog_id is null or tor.dog_id = pi_dog_id
                            connect by prior tor.org_id = tor.org_pid
                             start with tor.org_id = pi_id_org)
           and rs.state_id = 7 /*o.date_exec_sys*/
           and rs.date_change_state between trunc(pi_dates) + 2 / 24 and
               add_months(trunc(pi_dates), 1) - 1 / 24 / 60 / 60 + 2 / 24
           and rs.id not in
               (select rrc.id_conn
                  from t_report_real_conn rrc
                  join t_report_period rp
                    on rp.id = rrc.id_repper
                 where rrc.ci = 2
                   and rp.is_actual = 1
                      --поскольку айдишники usl_id могут  пересекаться со старым функционалом и выпадать из-за этого
                   and rp.dates >= to_date('01.01.2012', 'dd.mm.yyyy'));
    end if;
    IF pi_with_blocked_orgs = 0 THEN
    select count(*)
      into l_count
      from t_dogovor d
       where d.dog_id = pi_dog_id
         and d.is_enabled = 1
       and d.dog_class_id in (10, 11, 12);
    ELSE
       SELECT COUNT(*)
       INTO l_count
       FROM t_dogovor d
       WHERE d.dog_id = pi_dog_id
             AND d.dog_class_id IN (10, 11, 12);
    END IF;
    if l_count > 0 then
      l_sale_equip := 1;
      -- Заполнение t_report_real_conn
      insert into t_report_real_conn
        (id_conn, id_repper, ci)
        select distinct t.id, l_repper, 5
          from t_request_delivery t
         where t.exec_org_id in
               (select /*+ PRECOMPUTE_SUBQUERY */
                 tor.org_id
                  from t_org_relations tor
                  where tor.dog_id is null or tor.dog_id = pi_dog_id
                connect by prior tor.org_id = tor.org_pid
                 start with tor.org_id = pi_id_org)
           and t.final_date between trunc(pi_dates) + 2 / 24 and
               add_months(trunc(pi_dates), 1) - 1 / 24 / 60 / 60 + 2 / 24
           and t.state_id = 10211 --выполнена
        ;
    end if;
    l_counter := 5;

    for i in 1 .. l_counter loop
      l_dates  := ADD_MONTHS(pi_dates, -1 * i);
      l_datepo := LAST_DAY(ADD_MONTHS(pi_datepo, -1 * i));
      begin
        select tre.id
          into l_repper_calc
          from t_report_period tre
         where tre.dates = l_dates
           and tre.datepo = l_datepo
           and tre.is_actual = 1
           and tre.id_org = pi_id_org
           and tre.dog_id = pi_dog_id;
        fixActivAbonent(l_repper_calc, l_repper);
      exception
        when NO_DATA_FOUND then
          null;
      end;
    end loop;
    --
    update t_report_period rp set rp.status = 3 where rp.id = l_repper; --Сформирован не подтверждённый отчёт
    begin
      select sum(t.ps), sum(t.mis)
        into l_count_act_ps, l_count_act_mis
        from (select case
                       when ac.asr_id = -1 then
                        1
                       else
                        0
                     end mis,
                     case
                       when ac.asr_id > 0 then
                        1
                       else
                        0
                     end ps
                from t_report_period rp
                join t_report_real_conn rc
                  on rc.id_repper = rp.id
                 and rc.ci = 1
                join t_ab_pay_activity ac
                  on ac.abonent_id = rc.id_conn
                 and trunc(ac.date_begin) = trunc(rp.dates)
               where rp.id = l_repper) t;
      if l_count_act_ps > 0 then
        update t_report_period rp set rp.status = 3 where rp.id = l_repper; --Сформирован не подтверждённый отчёт
        update t_report_period_request rq
           set rq.status = 3
         where (rq.dates, rq.org_id, rq.dog_id) =
               (select t.dates, t.id_org, t.dog_id
                  from t_report_period t
                 where t.id = l_repper); --Сформирован не подтверждённый отчёт
      elsif l_count_act_mis > 0 then
        update t_report_period rp set rp.status = 2 where rp.id = l_repper; --Сформирован прогнозный отчёт
        update t_report_period_request rq
           set rq.status = 2
         where (rq.dates, rq.org_id, rq.dog_id) =
               (select t.dates, t.id_org, t.dog_id
                  from t_report_period t
                 where t.id = l_repper); --Сформирован прогнозный отчёт
      else
        update t_report_period rp set rp.status = 3 where rp.id = l_repper; --Сформирован не подтверждённый отчёт
        update t_report_period_request rq
           set rq.status = 3
         where (rq.dates, rq.org_id, rq.dog_id) =
               (select t.dates, t.id_org, t.dog_id
                  from t_report_period t
                 where t.id = l_repper); --Сформирован не подтверждённый отчёт
      end if;
    end;
    select count(*)
      into l_count
      from t_dogovor t
     where t.dog_class_id = 8 --гпх
       and t.dog_id = pi_dog_id;
    if l_count = 0 then
      select count(*)
        into l_count
        from t_report_real_conn rc
        join t_report_period t
          on rc.id_repper = t.id
       where t.is_actual = 1
         and t.id_org = pi_id_org
         and t.dog_id = pi_dog_id
         and t.dates > add_months(pi_dates, -12)
         and rownum = 1;
      if l_count = 0 then
        select count(*)
          into l_count
          from t_dogovor_prm dp
         where dp.dp_prm_id in (4001) --СКП,ott
           and dp.dp_is_enabled = 1
           and dp.dp_dog_id = pi_dog_id;
      end if;
    end if;
    if l_count > 0 or l_sale_equip = 1 then
      report_period.generate_stat(l_repper, 777);
      report_period.save_data_sf(l_repper,
                                 Null,
                                 l_datepo,
                                 777,
                                 po_err_num,
                                 po_err_msg);
    else
      logging_pkg.info('Report ('||pi_dates||','||pi_datepo||','||pi_id_org||','||pi_worker_id||','||pi_dog_id||' ) rollbacked ', c_package || 'FixReportPeriod');
      rollback to sp_begin;
      return - 1;
    end if;
    return l_repper;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, c_package || 'FixReportPeriod');
      return - 1;
  end FixReportPeriod;
  -------------------------------------------------------------------------------
  function GetReportPeriodList(pi_id_org       in array_num_2,
                               pi_block        in number,
                               pi_org_relation in num_tab,
                               pi_id_kurator   in t_organizations.org_id%type,
                               pi_dates        in t_report_period.dates%type,
                               pi_datepo       in t_report_period.datepo%type,
                               pi_dog_class    in num_tab,
                               pi_worker_id    in t_users.usr_id%type,
                               po_err_num      out pls_integer,
                               po_err_msg      out t_Err_Msg)
    return sys_refcursor is
    res       sys_refcursor;
    User_Orgs num_tab;
    --Kurators_Orgs num_tab;
  begin
    --если организации не переданы ограничит просто организациями пользователя
    User_Orgs := get_orgs_tab_for_multiset(pi_orgs         => pi_id_org,
                                           Pi_worker_id    => pi_worker_id,
                                           pi_block        => pi_block,
                                           pi_org_relation => pi_org_relation);


   /* Kurators_Orgs := get_orgs_tab_for_multiset(
                                           pi_orgs     => array_num_2(REC_NUM_2(pi_id_kurator,1)),
                                           Pi_worker_id    => pi_worker_id,
                                           pi_block        => pi_block,
                                           pi_org_relation => num_tab(1001));     */



    open res for
      select org_dil.org_name ORG,
             org_kur.org_name KUR,
             rp.dates,
             rp.datepo,
             (select count(rc.id_conn)
                from t_report_real_conn rc
               where rc.id_repper = rp.id
                 and rc.ci = 1) KOL_GSM,
             (nvl((select sum(a.ab_paid)
                    from t_report_real_conn rrc, t_abonent a
                   where rrc.id_repper = rp.id
                     and rrc.id_conn = a.ab_id
                     and rrc.ci = 1),
                  0)) MONEY,
             rp.id rp_id,
             org_dil.org_id ORG_ID,
             org_kur.org_id KUR_ID,
             dog.dog_id,
             dog.dog_number,
             dic.id id_status,
             dic.name name_status,
             dog.dog_class_id dog_class,
             (select count(rc.ID_CONN)
                from t_report_real_conn rc
                left join tr_request_service rs
                  on rs.id = rc.ID_CONN
               where rc.id_repper = rp.id
                 and rc.ci = 2
                 and rs.product_category = 1) KOL_SHPD,
             (select count(rc.ID_CONN)
                from t_report_real_conn rc
                left join tr_request_service rs
                  on rs.id = rc.ID_CONN
               where rc.id_repper = rp.id
                 and rc.ci = 2
                 and rs.product_category = 5) KOL_IPTV,
             (select count(rc.ID_CONN)
                from t_report_real_conn rc
                left join tr_request_service rs
                  on rs.id = rc.ID_CONN
               where rc.id_repper = rp.id
                 and rc.ci = 2
                 and rs.product_category = 2) KOL_PSTN
        from t_report_period     rp,
             t_organizations     org_dil,
             t_organizations     org_kur,
             mv_org_tree         ro,
             t_dogovor           dog,
             t_dic_report_status dic
       where org_dil.org_id = rp.id_org
         and rp.status = dic.id(+)
         and dog.dog_id = rp.dog_id
         and rp.is_actual = 1
            --and (pi_id_org is null or rp.id_org = pi_id_org)
         and rp.id_org in (select * from table(User_Orgs))
         and (pi_dates is null or rp.dates >= pi_dates)
         and (pi_datepo is null or rp.datepo <= pi_datepo)
         and ro.org_id = rp.id_org
         --and (ro.dog_id is null or ro.org_pid in (select * from table(Kurators_Orgs)))
         and ro.root_reltype in (1004, 1003, 1007, 1008, 999)
         and dog.dog_class_id in (select * from table(pi_dog_class))
         and (pi_id_kurator is null or ro.org_pid = pi_id_kurator
             or ro.root_org_id = pi_id_kurator)
            /*and (rp.id_org in
            (select * from TABLE(get_user_orgs_tab(pi_worker_id))))*/
         and org_kur.org_id = ro.org_pid
         and (ro.dog_id is null or rp.dog_id = ro.dog_id)
       order by dates, datepo, KUR, ORG;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end GetReportPeriodList;
  -------------------------------------
  function GetReportPeriod(pi_id_rp     in t_report_period.id%type,
                           Pi_Perm      in number,
                           pi_worker_id in t_users.usr_id%type,
                           po_err_num   out pls_integer,
                           po_err_msg   out t_Err_Msg,
                           pi_id_org    in t_organizations.org_id%type := null,
                           pi_month     in date := null) return sys_refcursor is
    res       sys_refcursor;
    l_id_rp   number;
    l_count   number;
    l_org_tab num_tab := num_tab();
  begin
    if (pi_id_rp is not null) then
      l_id_rp := pi_id_rp;
    else
      begin
        select rp.id
          into l_id_rp
          from t_report_period rp
         where rp.id_org = pi_id_org
           and trunc(rp.dates, 'MONTH') = trunc(pi_month, 'MONTH')
           and rp.is_actual = 1;
      exception
        when others then
          null;
      end;
    end if;
    select count(*)
      into l_count
      from t_report_real_conn rc
     where rc.id_repper = l_id_rp;

    if (l_count = 0) then
      l_org_tab := GET_USER_ORGS_TAB(777);
      open res for
        select org_dil.org_name ORG,
               org_kur.org_name KUR,
               rp.dates,
               rp.datepo,
               null TAR,
               0 KOL,
               0 MONEY,
               d.premia_schema,
               d.dog_number,
               sf.number_sf num_account,
               rp.num_zaiav_opl,
               rp.dt_zaiav_opl,
               acc_operations.Get_Lic_Acc_Id_By_Org_Dog(rp.id_org,
                                                        rp.dog_id) Lic_acc_id,
               rp.id_document,
               rp.status id_status,
               (Select max(rph.date_change)
                  from t_report_period_hst rph
                 where rph.id_repper = rp.Id) date_change,
               sf.adoption_date,
               sf.payment_date,
               (select min(drp.dog_type) keep(dense_rank last order by drp.date_dog_type)
                  from t_dogovor_report_form drp
                 where drp.dog_id = d.dog_id
                   and rp.dates >= drp.date_dog_type) dog_type,
               4 product_cat
          from t_report_period rp,
               t_organizations org_dil,
               t_organizations org_kur,
               mv_org_tree ro,
               t_dogovor       d,
               t_report_sf     sf
         where org_dil.org_id = rp.id_org
           and sf.id_repper /*(+)*/
               = rp.id --20.09 зад.21759
           and rp.is_actual = 1
           and pi_id_rp = rp.id
           and ro.org_id = rp.id_org
           and ro.root_reltype in (1003, 1004, 1007, 1008, 999)
           and (rp.id_org in (Select /*+ PRECOMPUTE_SUBQUERY */
                               *
                                from Table(l_org_tab)))
           and org_kur.org_id = ro.org_pid
           and d.org_rel_id = ro.id
           and d.dog_id = rp.dog_id;
    else
      l_org_tab := GET_USER_ORGS_TAB(pi_worker_id);
      open res for
        select org_dil.org_name ORG,
               org_kur.org_name KUR,
               rp.dates,
               rp.datepo,
               tar.title TAR,
               count(rc.id_conn) KOL,
               sum(a.ab_paid) MONEY,
               d.premia_schema,
               d.dog_number,
               sf.number_sf num_account,
               rp.num_zaiav_opl,
               rp.dt_zaiav_opl,
               acc_operations.Get_Lic_Acc_Id_By_Org_Dog(rp.id_org,
                                                        rp.dog_id) Lic_acc_id,
               rp.id_document,
               rp.status id_status,
               (Select max(rph.date_change)
                  from t_report_period_hst rph
                 where rph.id_repper = rp.Id) date_change,
               sf.adoption_date,
               sf.payment_date,
               (select min(drp.dog_type) keep(dense_rank last order by drp.date_dog_type)
                  from t_dogovor_report_form drp
                 where drp.dog_id = d.dog_id
                   and rp.dates >= drp.date_dog_type) dog_type,
               4 product_cat
          from t_report_period rp
          left join t_report_real_conn rc
            on rc.id_repper = rp.id
           and rc.ci = 1
          left join t_abonent a
            on a.ab_id = rc.id_conn
          join t_organizations org_dil
            on org_dil.org_id = rp.id_org
          join mv_org_tree ro
            on ro.org_id = rp.id_org
          join t_organizations org_kur
            on org_kur.org_id = ro.org_pid
          left join t_tarif_by_at_id tar
            on tar.at_id = rc.tar_id
          join t_dogovor d
            on d.dog_id = rp.dog_id
          left join t_report_sf sf
            on sf.id_repper = rp.id
         where rp.is_actual = 1
           and l_id_rp = rp.id
           and ro.root_reltype in (1003, 1004, 1007, 1008, 999)
           and (rp.id_org in (Select /*+ PRECOMPUTE_SUBQUERY */
                               *
                                from Table(l_org_tab)))
           and d.org_rel_id = ro.id
         group by org_dil.org_name,
                  org_kur.org_name,
                  rp.dates,
                  rp.datepo,
                  tar.title,
                  d.premia_schema,
                  d.dog_number,
                  sf.number_sf,
                  rp.num_zaiav_opl,
                  rp.dt_zaiav_opl,
                  acc_operations.Get_Lic_Acc_Id_By_Org_Dog(rp.id_org,
                                                           rp.dog_id),
                  rp.id_document,
                  rp.status,
                  sf.adoption_date,
                  sf.payment_date,
                  rp.Id,
                  d.dog_id
        union
        select ORG,
               KUR,
               dates,
               datepo,
               TAR,
               KOL,
               0 MONEY,
               premia_schema,
               dog_number,
               num_account,
               num_zaiav_opl,
               dt_zaiav_opl,
               acc_operations.Get_Lic_Acc_Id_By_Org_Dog(id_org, dog_id) Lic_acc_id,
               id_document,
               id_status,
               (Select max(rph.date_change)
                  from t_report_period_hst rph
                 where rph.id_repper = Id) date_change,
               adoption_date,
               payment_date,
               (select min(drp.dog_type) keep(dense_rank last order by drp.date_dog_type)
                  from t_dogovor_report_form drp
                 where drp.dog_id = dog_id
                   and dates >= drp.date_dog_type) dog_type,
               product_cat
          from (select org_dil.org_name ORG,
                       org_kur.org_name KUR,
                       rp.dates,
                       rp.datepo,
                       nvl(tar.title, tar_mrf.title) TAR,
                       count(rrc.id_conn) KOL,
                       d.premia_schema,
                       d.dog_number,
                       sf.number_sf num_account,
                       rp.num_zaiav_opl,
                       rp.dt_zaiav_opl,
                       rp.id_document,
                       rp.status id_status,
                       sf.adoption_date,
                       sf.payment_date,
                       rs.product_category product_cat,
                       rp.id,
                       rp.id_org,
                       rp.dog_id
                  from t_report_period rp
                  join t_report_real_conn rrc
                    on rrc.id_repper = rp.id
                  join tr_request_service rs
                    on rs.id = rrc.id_conn
                  left join tr_service_product sp
                    on sp.service_id = rs.id
                   and sp.product_type = 1
                  left join tr_request_product pr
                    on pr.id = sp.product_id
                   and pr.type_product = 1
                  join t_organizations org_dil
                    on org_dil.org_id = rp.id_org
                  join mv_org_tree ro
                    on ro.org_id = rp.id_org
                  join t_organizations org_kur
                    on org_kur.org_id = ro.org_pid
                  left join CONTRACT_T_TARIFF_VERSION tar
                    on tar.id = pr.tar_id
                   and pr.type_tariff = 1
                  left join t_mrf_tariff tar_mrf
                    on tar_mrf.id = pr.tar_id
                   and pr.type_tariff = 3
                  join t_dogovor d
                    on d.org_rel_id = ro.id
                   and d.dog_id = rp.dog_id
                  join t_report_sf sf
                    on sf.id_repper = rp.id
                 where rp.is_actual = 1
                   and l_id_rp = rp.id
                   and ro.root_reltype in (1003, 1004, 1007, 1008, 999)
                   and (rp.id_org in (Select /*+ PRECOMPUTE_SUBQUERY */
                                       *
                                        from Table(l_org_tab)))
                   and rrc.ci = 2
                 group by org_dil.org_name,
                          org_kur.org_name,
                          rp.dates,
                          rp.datepo,
                          nvl(tar.title, tar_mrf.title),
                          d.premia_schema,
                          d.dog_number,
                          sf.number_sf,
                          rp.num_zaiav_opl,
                          rp.dt_zaiav_opl,
                          rp.id_document,
                          rp.status,
                          sf.adoption_date,
                          sf.payment_date,
                          rs.product_category,
                          rp.id,
                          rp.id_org,
                          rp.dog_id)
        --and nvl(Pi_Perm, 0) in (2001, 2002, 2007, 2008, 2009)
         order by dates, datepo, KUR, ORG, TAR;
    end if;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end GetReportPeriod;
  -------------------------------------------------------------------------------
  procedure FixPeriod(par_date             date := sysdate,
                      Pi_Org_Id            Number := null,
                      pi_with_blocked_orgs NUMBER := 0) is
  begin
    FixPeriod(par_date, Pi_Org_Id, 1, pi_with_blocked_orgs);
  end;
  -------------------------------------------------------------------------------
  procedure FixPeriod(par_date         date := sysdate,
                      Pi_Org_Id        Number := null,
                      pi_include_child     number,
                      pi_with_blocked_orgs NUMBER := 0) is
    l_date    date := sysdate;
    l_dates   date;
    l_datepo  date;
    l_res     number;
    l_err_num pls_integer;
    l_err_msg t_Err_Msg;
    l_cnt     number := 0;
  begin

    if (Trunc(par_date) = Trunc(sysDate) or par_date is Null) then
      l_date := ADD_MONTHS(SysDate, -1);
    Else
      l_date := par_date;
    end if;

    l_dates  := TRUNC(l_date, 'MONTH');
    l_datepo := LAST_DAY(l_dates);
    if pi_include_child = 1 then
      for i in (select distinct vorg.ORG_ID, vorg.ID, vorg.dog_id
                  from v_org_tree_all vorg
                 Where vorg.ORG_RELTYPE in (1003, 1004, 1007, 1008, 999)
                   and vorg.dog_id is Not Null
                Connect by prior vorg.ORG_ID = vorg.ORG_PID
                 Start with vorg.ORG_ID = NVL(Pi_Org_Id, 1)) loop
        logging_pkg.info(Pi_Org_Id || '(' || i.org_id || ') Start: ' ||
                         sysdate,
                         c_package || 'FixPeriod');
        l_res := FixReportPeriod(l_dates,
                                 l_datepo,
                                 i.org_id,
                                 -1,
                                 i.dog_id,
                                 0,
                                 l_err_num,
                                 l_err_msg,
                                 pi_with_blocked_orgs);
        If l_err_num is Not Null then
          dbms_output.put_line('org_id: ' || i.org_id || ' -- ' ||
                               l_err_msg);
          l_err_num := null;
        End If;
        l_cnt := l_cnt + 1;
        logging_pkg.info(Pi_Org_Id || '(' || i.org_id || ','||i.dog_id ||') Stop : ' ||
                         sysdate,
                         c_package || 'FixPeriod');
      end loop;
    else
      for i in (select distinct vorg.ORG_ID, vorg.ID, vorg.dog_id
                  from v_org_tree_all vorg
                 Where vorg.ORG_RELTYPE in (1003, 1004, 1007, 1008, 999)
                   and vorg.dog_id is Not Null
                   and vorg.ORG_ID = NVL(Pi_Org_Id, 0)) loop
        logging_pkg.info(Pi_Org_Id || '(' || i.org_id || ') Start: ' ||
                         sysdate,
                         c_package || 'FixPeriod');
        l_res := FixReportPeriod(l_dates,
                                 l_datepo,
                                 i.org_id,
                                 -1,
                                 i.dog_id,
                                 1,
                                 l_err_num,
                                 l_err_msg,
                                 pi_with_blocked_orgs);
        If l_err_num is Not Null then
          dbms_output.put_line('org_id: ' || i.org_id || ' -- ' ||
                               l_err_msg);
          l_err_num := null;
        End If;
        l_cnt := l_cnt + 1;
        logging_pkg.info(Pi_Org_Id || '(' || i.org_id ||','||i.dog_id || ') Stop : ' ||
                         sysdate,
                         c_package || 'FixPeriod');
      end loop;
    end if;
    logging_pkg.info('cnt=' || l_cnt, c_package || 'FixPeriod');
  end FixPeriod;
  -------------------------------------------------------------------------------
  function SegReport(pi_rep_id    in number,
                     pi_worker_id in number,
                     po_err_num   out pls_integer,
                     po_err_msg   out t_Err_Msg) return sys_refcursor is
    l_cur   sys_refcursor;
    l_count number := 10;
  begin
    open l_cur for
      select bon.seg_id,
             bon.seg_name,
             bon.at_id,
             bon.title,
             bon.count,
             bon.stavka,
             (bon.count * bon.stavka) bonus
        from (select tab.seg_id,
                     s.seg_name,
                     tab.at_id,
                     tar.title,
                     tab.cou count,
                     (case
                       when cou >= l_count then
                        s.seg_up_cost
                       else
                        s.seg_base_cost
                     end) stavka
                from (select s.seg_id, at.at_id, count(a.ab_id) cou
                        from t_abonent          a,
                             t_report_real_conn t,
                             t_tmc              t,
                             t_tmc_sim          ts,
                             t_abstract_tar     at,
                             t_tar_segm         s
                       where a.ab_tmc_id = t.tmc_id
                         and t.tmc_id = ts.tmc_id
                         and ts.tar_id = at.at_id
                         and s.seg_id = at.at_seg_id
                         and a.ab_id = t.id_conn
                         and t.id_repper = pi_rep_id
                       group by s.seg_id, at.at_id
                       order by seg_id) tab,
                     t_tariff2 tar,
                     t_tar_segm s
               where tab.seg_id = s.seg_id
                 and get_last_tar_id_by_at_id(tab.at_id) = tar.id
               order by s.seg_id) bon;
    return l_cur;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end SegReport;
  -------------------------------------------------------------------------------
  function RepLastMonth(pi_rep_id in number) return number is
    l_count number;
  begin
    select count(*)
      into l_count
      from t_Report_Period    t,
           t_Report_Period    tt,
           t_report_real_conn c,
           t_abonent          a,
           t_ab_pay_activity  pa
     where t.id_org = tt.id_org
       and t.is_actual = 1
       and tt.dates = ADD_MONTHS(t.dates, -1)
       and t.id = pi_rep_id
       and c.id_repper = tt.id
       and c.id_conn = a.ab_id
       and pa.abonent_id = a.ab_id
       and pa.amount_pay >= 30;
    return l_count;
  end RepLastMonth;
  -------------------------------------------------------------------------------
  function Get_Connections(pi_rp_id     in number,
                           pi_worker_id in T_USERS.USR_ID%type,
                           po_err_num   out pls_integer,
                           po_err_msg   out t_Err_Msg,
                           pi_id_org    in t_organizations.org_id%type := null,
                           pi_month     in date := null) return sys_refcursor is
  begin
    return Get_Connections(pi_rp_id,
                           0,
                           pi_worker_id,
                           po_err_num,
                           po_err_msg,
                           pi_id_org,
                           pi_month);
  end;
  -------------------------------------------------------------------------------
  function Get_Connections(pi_rp_id     in number,
                           pi_type_date in number, --0-по дате подключения,1-по дате активации
                           pi_worker_id in T_USERS.USR_ID%type,
                           po_err_num   out pls_integer,
                           po_err_msg   out t_Err_Msg,
                           pi_id_org    in t_organizations.org_id%type := null,
                           pi_month     in date := null)
    return sys_refcursor is
    res      sys_refcursor;
    l_rp_id  number;
    l_date   date;
    l_org_id number;
    -- Добавлен учет договора, т.к. есть ситуация, когда у оорганизации
    -- 2 активных договора (например 2004717), а надо, чтобы подключения
    -- подцеплялись только по одному из них (задача 69856)
    l_dog_id number;
    l_is_fes number;
    l_org_tab num_tab;
  begin
    if (pi_rp_id is not null) then
      l_rp_id := pi_rp_id;
      select trunc(rp.dates),
             rp.id_org,
             rp.dog_id,
             case
               when rp.id_org = 2004855 then
                1
               else
                r.reg_id
             end
        into l_date, l_org_id, l_dog_id, l_is_fes
        from t_report_period rp
        left join t_dic_region r
          on r.org_id = rp.id_org
       where rp.id = l_rp_id
         and rp.is_actual = 1;
    else
      l_org_id := pi_id_org;
      l_date   := trunc(pi_month, 'MONTH');
      begin
        select rp.id
          into l_rp_id
          from t_report_period rp
         where rp.id_org = pi_id_org
           and rp.is_actual = 1
           and trunc(rp.dates, 'MONTH') = trunc(pi_month, 'MONTH');
      exception
        when others then
          null;
      end;
    end if;
  
    select tor.org_id bulk collect
      into l_org_tab
      from t_org_relations tor
      join t_org_is_rtmob rt
        on rt.org_id = tor.org_id
     where rt.is_org_rtm = 0
   connect by prior tor.org_id = tor.org_pid
     start with tor.org_id in (l_org_id);
  
    -- берем список подключений
    if pi_type_date = 0 then
      open res for
        select A.AB_ID ABONENT_ID,
               A.CLIENT_ID,
               a.CLIENT_TYPE,
               a.PERSON_LASTNAME,
               a.PERSON_FIRSTNAME,
               a.PERSON_MIDDLENAME,
               a.JUR_NAME,
               A.AB_REG_DATE - 2 / 24 REG_DATE,
               A.AB_MOD_DATE - 2 / 24 MOD_DATE,
               decode(A.AB_STATUS, 105, 105, a.ab_status) STATUS,
               A.AB_PAID PAID,
               A.AB_PAID "COST",
               A.ERR_CODE,
               A.ERR_MSG,
               OOO.ORG_NAME WHICH_ORG_AB_NAME,
               (select CONCAT(DV1.DV_NAME,
                              CONCAT(' ',
                                     CONCAT(DV3.DV_NAME,
                                            CONCAT('  ', DV2.DV_NAME))))
                  from T_DIC_VALUES     DV1,
                       T_DIC_VALUES     DV2,
                       T_DIC_VALUES     DV3,
                       t_tarif_by_at_id TAR
                 where tar.at_id = a.tar_id
                   and tar.type_vdvd_id = dv1.dv_id
                   and tar.tariff_type = dv2.dv_id
                   and tar.pay_type = dv3.dv_id) CONN_NAME,
               (select /*CONCAT(*/
                distinct TAR.TITLE /*, CONCAT(' / ', TREG.NAME))*/
                  from t_tarif_by_at_id TAR --, t_dic_region_data TREG, t_dic_mvno_region dmr
                 where tar.at_id = a.tar_id /*
                                                                                   and tar.at_region_id = dmr.id
                                                                                   and dmr.REG_ID = treg.reg_id*/
                ) /*null*/ TAR_NAME,
               A.IMSI IMSI,
               A.CALLSIGN CS,
               null CITY_NUM,
               null SIM_COLOR,
               A.AB_DOG_DATE DOG_DATE,
               A.IS_BAD,
               A.ID_BAD AB_ID_BAD,
               cc.cc_id,
               NVL(CC.CC_NAME, '') CC_NAME,
               A.CHANGE_STATUS_DATE - 2 / 24 CHANGE_STATUS_DATE,
               '' DOG_NUMBER,
               A.ARCHIVE_MARK,
               A.USER_ID USER_ID,
               null worker,
               null seller,
               A.IS_COMMON,
               a.client_category,
               -- 33436 olia_serg
               A.AB_PAID_ESPO, -- Cумма с оплатой через агента
               A.AB_PAID_ESPP, -- Cумма с оплатой через ЕСПП
               (case
                 when a.data_activated is null or
                      a.data_activated - 2 / 24 >= add_months(l_date, 1) then
                  0
                 else
                  1
               end) is_activated,
               trunc(a.data_activated - 2 / 24) as data_activated,
               a.ab_status_ex,
               a.model_equip,
               a.is_resident,
               a.reg_deadline,
               out_account,
               CREDIT_LIMIT,
               ALARM_LIMIT
          from T_ORGANIZATIONS OOO,
               T_CALC_CENTER CC,
               (select AB.AB_ID,
                       null ID_BAD,
                       AB.AB_REG_DATE,
                       AB.AB_MOD_DATE,
                       decode(AB.AB_STATUS, 105, 105, AB.Ab_Status) as AB_STATUS,
                       AB.AB_PAID,
                       AB.AB_COST,
                       AB.ERR_CODE,
                       AB.ERR_MSG,
                       AB.ORG_ID,
                       AB.CHANGE_STATUS_DATE,
                       AB.CC_ID,
                       AB.AB_DOG_DATE,
                       0 IS_BAD,
                       SIM.TAR_ID TAR_ID,
                       AB.ROOT_ORG_ID,
                       AB.ARCHIVE_MARK,
                       SIM.SIM_IMSI IMSI,
                       SIM.SIM_CALLSIGN CALLSIGN,
                       AB.USER_ID,
                       AB.IS_COMMON,
                       -- 33436 olia_serg
                       (case nvl(AB.With_Ab_Pay, 0)
                         when 0 then
                          AB.AB_PAID -- Cумма с оплатой через агента
                         when 1 then
                          0 -- Cумма с оплатой через ЕСПП
                       end) AB_PAID_ESPO,
                       (case nvl(AB.With_Ab_Pay, 0)
                         when 0 then
                          null -- Cумма с оплатой через агента
                         when 1 then
                          AB.AB_PAID -- Cумма с оплатой через ЕСПП
                         when -1 then
                          AB.AB_PAID -- Cумма с оплатой через ЕСПП
                       end) AB_PAID_ESPP,
                       aa.data_activated,
                       cl.client_id,
                       cl.client_type,
                       p.person_lastname,
                       p.person_firstname,
                       p.person_middlename,
                       j.jur_name,
                       j.jur_category client_category,
                       ab.ab_status_ex,
                       nvl(mu.usb_model, mp.name_model) model_equip,
                       cl.is_resident,
                       cl.reg_deadline,
                       ab.out_account,
                       ab.CREDIT_LIMIT,
                       ab.ALARM_LIMIT
                  from T_ABONENT AB
                  join t_clients cl
                    on cl.client_id = ab.client_id
                  left join t_person p
                    on cl.client_type = 'P'
                   and cl.fullinfo_id = p.person_id
                  left join t_juristic j
                    on cl.client_type = 'J'
                   and cl.fullinfo_id = j.juristic_id
                  join t_tmc_sim sim
                    on ab.ab_tmc_id = sim.tmc_id
                  left join t_abonent_activated aa
                    on aa.abonent_id = ab.ab_id
                  left join t_tmc_modem_usb usb
                    on usb.tmc_id = ab.equipment_tmc_id
                  left join t_modem_model_usb mu
                    on mu.id = usb.usb_model
                  left join t_tmc_phone allo
                    on allo.tmc_id = ab.equipment_tmc_id
                  left join t_model_phone mp
                    on mp.model_id = allo.model_id
                  where ab.is_deleted = 0
                   and ab.ab_status in (104, 105)
                   and ab.change_status_date between l_date + 2 / 24 and
                       trunc(add_months(l_date, 1), 'mm') - 1 / 24 / 60 / 60 +
                       2 / 24
                   and ((ab.root_org_id = l_org_id and
                       ab.dog_id = l_dog_id and l_is_fes is null) or
                       ab.org_id in (select * from table(l_org_tab)))) A
         where a.org_id = ooo.org_id
           and a.cc_id = cc.cc_id(+)
         order by DOG_DATE asc;
    else
      open res for
        select A.AB_ID ABONENT_ID,
               A.CLIENT_ID,
               a.CLIENT_TYPE,
               a.PERSON_LASTNAME,
               a.PERSON_FIRSTNAME,
               a.PERSON_MIDDLENAME,
               a.JUR_NAME,
               A.AB_REG_DATE - 2 / 24 REG_DATE,
               A.AB_MOD_DATE - 2 / 24 MOD_DATE,
               decode(A.AB_STATUS, 105, 105, a.ab_status) STATUS,
               A.AB_PAID PAID,
               A.AB_PAID "COST",
               A.ERR_CODE,
               A.ERR_MSG,
               OOO.ORG_NAME WHICH_ORG_AB_NAME,
               (select CONCAT(DV1.DV_NAME,
                              CONCAT(' ',
                                     CONCAT(DV3.DV_NAME,
                                            CONCAT('  ', DV2.DV_NAME))))
                  from T_DIC_VALUES     DV1,
                       T_DIC_VALUES     DV2,
                       T_DIC_VALUES     DV3,
                       t_tarif_by_at_id TAR
                 where tar.at_id = a.tar_id
                   and tar.type_vdvd_id = dv1.dv_id
                   and tar.tariff_type = dv2.dv_id
                   and tar.pay_type = dv3.dv_id) CONN_NAME,
               (select /*CONCAT(*/
                distinct TAR.TITLE /*, CONCAT(' / ', TREG.NAME))*/
                  from t_tarif_by_at_id TAR /*, t_dic_region_data TREG, t_dic_mvno_region dmr*/
                 where tar.at_id = a.tar_id /*
                                                                                   and tar.at_region_id = dmr.id
                                                                                   and dmr.REG_ID = treg.reg_id*/
                ) TAR_NAME,
               A.IMSI IMSI,
               A.CALLSIGN CS,
               null CITY_NUM,
               null SIM_COLOR,
               A.AB_DOG_DATE DOG_DATE,
               A.IS_BAD,
               A.ID_BAD AB_ID_BAD,
               cc.cc_id,
               NVL(CC.CC_NAME, '') CC_NAME,
               A.CHANGE_STATUS_DATE - 2 / 24 CHANGE_STATUS_DATE,
               '' DOG_NUMBER,
               A.ARCHIVE_MARK,
               A.USER_ID USER_ID,
               null worker,
               null seller,
               A.IS_COMMON,
               a.client_category,
               -- 33436 olia_serg
               A.AB_PAID_ESPO, -- Cумма с оплатой через агента
               A.AB_PAID_ESPP, -- Cумма с оплатой через ЕСПП
               (case
                 when a.data_activated is null or
                      a.data_activated - 2 / 24 >= add_months(l_date, 1) then
                  0
                 else
                  1
               end) is_activated,
               trunc(a.data_activated - 2 / 24) as data_activated,
               a.ab_status_ex,
               a.model_equip,
               a.is_resident,
               a.reg_deadline,
               out_account,
               CREDIT_LIMIT,
               ALARM_LIMIT
          from T_ORGANIZATIONS OOO,
               T_CALC_CENTER CC,
               (select AB.AB_ID,
                       null ID_BAD,
                       AB.AB_REG_DATE,
                       AB.AB_MOD_DATE,
                       decode(AB.AB_STATUS, 105, 104, AB.Ab_Status) as AB_STATUS,
                       AB.AB_PAID,
                       AB.AB_COST,
                       AB.ERR_CODE,
                       AB.ERR_MSG,
                       AB.ORG_ID,
                       AB.CHANGE_STATUS_DATE,
                       AB.CC_ID,
                       AB.AB_DOG_DATE,
                       0 IS_BAD,
                       SIM.TAR_ID TAR_ID,
                       AB.ROOT_ORG_ID,
                       AB.ARCHIVE_MARK,
                       SIM.SIM_IMSI IMSI,
                       SIM.SIM_CALLSIGN CALLSIGN,
                       AB.USER_ID,
                       AB.IS_COMMON,
                       -- 33436 olia_serg
                       (case nvl(AB.With_Ab_Pay, 0)
                         when 0 then
                          AB.AB_PAID -- Cумма с оплатой через агента
                         when 1 then
                          0 -- Cумма с оплатой через ЕСПП
                       end) AB_PAID_ESPO,
                       (case nvl(AB.With_Ab_Pay, 0)
                         when 0 then
                          null -- Cумма с оплатой через агента
                         when 1 then
                          AB.AB_PAID -- Cумма с оплатой через ЕСПП
                         when -1 then
                          AB.AB_PAID -- Cумма с оплатой через ЕСПП
                       end) AB_PAID_ESPP,
                       aa.data_activated,
                       cl.client_id,
                       cl.client_type,
                       p.person_lastname,
                       p.person_firstname,
                       p.person_middlename,
                       j.jur_name,
                       j.jur_category client_category,
                       ab.ab_status_ex,
                       nvl(mu.usb_model, mp.name_model) model_equip,
                       cl.is_resident,
                       cl.reg_deadline,
                       ab.out_account,
                       ab.CREDIT_LIMIT,
                       ab.ALARM_LIMIT
                  from T_ABONENT AB
                  join t_clients cl
                    on cl.client_id = ab.client_id
                  left join t_person p
                    on cl.client_type = 'P'
                   and cl.fullinfo_id = p.person_id
                  left join t_juristic j
                    on cl.client_type = 'J'
                   and cl.fullinfo_id = j.juristic_id
                  join t_report_real_conn rpc
                    on ab.ab_id = rpc.id_conn
                   and rpc.id_repper = l_rp_id
                   and rpc.ci = 1
                  join t_tmc_sim sim
                    on ab.ab_tmc_id = sim.tmc_id
                  left join t_abonent_activated aa
                    on aa.abonent_id = ab.ab_id
                  left join t_tmc_modem_usb usb
                    on usb.tmc_id = ab.equipment_tmc_id
                  left join t_modem_model_usb mu
                    on mu.id = usb.usb_model
                  left join t_tmc_phone allo
                    on allo.tmc_id = ab.equipment_tmc_id
                  left join t_model_phone mp
                    on mp.model_id = allo.model_id
                 where ab.is_deleted = 0
                   and ab.ab_status in (104, 105)
                   and (l_is_fes > 0 or ab.dog_id = l_dog_id)) A
         where a.org_id = ooo.org_id
           and a.cc_id = cc.cc_id(+)
         order by DOG_DATE asc;
    end if;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Connections;
  ----------------------------------------------------------------------------
  function Get_Connections_Stat(pi_rp_id     in number,
                                pi_worker_id in T_USERS.USR_ID%type,
                                po_err_num   out pls_integer,
                                po_err_msg   out t_Err_Msg,
                                pi_id_org    in t_organizations.org_id%type := null,
                                pi_month     in date := null)
    return sys_refcursor is
  begin
    return Get_Connections_Stat(pi_rp_id,
                                0,
                                pi_worker_id,
                                po_err_num,
                                po_err_msg,
                                pi_id_org,
                                pi_month);
  end;
  ----------------------------------------------------------------------------
  function Get_Connections_Stat(pi_rp_id     in number,
                                pi_type_date in number, --0-по дате подключения,1-по дате активации
                                pi_worker_id in T_USERS.USR_ID%type,
                                po_err_num   out pls_integer,
                                po_err_msg   out t_Err_Msg,
                                pi_id_org    in t_organizations.org_id%type := null,
                                pi_month     in date := null)
    return sys_refcursor is
    res      sys_refcursor;
    l_rp_id  number;
    l_org_id number;
    l_dates  date;
    -- Добавлен учет договора, т.к. есть ситуация, когда у оорганизации
    -- 2 активных договора (например 2004717), а надо, чтобы подключения
    -- подцеплялись только по одному из них (задача 69856)
    l_dog_id number;
    l_is_fes number;
  begin
    if (pi_rp_id is not null) then
      l_rp_id := pi_rp_id;
      select t.id_org,
             t.dates,
             t.dog_id,
             case
               when t.id_org = 2004855 then
                1
               else
                r.reg_id
             end
        into l_org_id, l_dates, l_dog_id, l_is_fes
        from t_report_period t
        left join t_dic_region r
          on r.org_id = t.id_org
       where t.id = pi_rp_id
         and t.is_actual = 1;
    else
      l_org_id := pi_id_org;
      l_dates  := trunc(pi_month, 'MONTH');
      begin
        select rp.id
          into l_rp_id
          from t_report_period rp
         where rp.id_org = pi_id_org
           and rp.is_actual = 1
           and rp.dates = trunc(pi_month, 'MONTH');
      exception
        when others then
          null;
      end;
    end if;
    if pi_type_date = 0 then
      open res for
        select distinct A.AB_STATUS STATUS,
                        sum(A.AB_PAID) PAID,
                        count(*) "COUNT",
                        sum(a.with_ab_pay) count_with_ab_pay,
                        -- 33436 olia_serg
                        sum(A.AB_PAID_ESPO) AB_PAID_ESPO, -- Cумма с оплатой через агента
                        sum(A.AB_PAID_ESPP) AB_PAID_ESPP -- Cумма с оплатой через ЕСПП
          from /*T_TMC_OPERATIONS O,
                                                               T_TMC_OPERATION_UNITS OU,
                                                               T_CLIENTS CL,*/
               (select AB1.AB_ID,
                       AB1.CLIENT_ID,
                       ab1.ab_status,
                       AB1.AB_PAID,
                       AB1.AB_TMC_ID,
                       AB1.ORG_ID,
                       AB1.Id_Op,
                       AB1.CHANGE_STATUS_DATE,
                       -- 33436 olia_serg
                       (case nvl(AB1.With_Ab_Pay, 0)
                         when 0 then
                          AB1.AB_PAID -- Cумма с оплатой через агента
                         when 1 then
                          0 -- Cумма с оплатой через ЕСПП
                       end) AB_PAID_ESPO,
                       (case nvl(AB1.With_Ab_Pay, 0)
                         when 0 then
                          null -- Cумма с оплатой через агента
                         when 1 then
                          decode(nvl(AB1.AB_PAID, 0), 0, null, AB1.AB_PAID) -- Cумма с оплатой через ЕСПП
                         when -1 then
                          decode(nvl(AB1.AB_PAID, 0), 0, null, AB1.AB_PAID) -- Cумма с оплатой через ЕСПП
                       end) AB_PAID_ESPP,
                       nvl(AB1.with_ab_pay, 0) with_ab_pay,
                       ab1.dog_id
                  from T_ABONENT AB1
                 where AB1.IS_DELETED = 0
                   and AB1.AB_STATUS in (104, 105)
                   and ab1.change_status_date - 2 / 24 between l_dates and
                       trunc(add_months(l_dates, 1), 'mm') -
                       1 / 24 / 60 / 60
                   and ((ab1.root_org_id = l_org_id and l_is_fes is null) or
                       ab1.org_id in
                       (select tor.org_id
                           from t_org_relations tor
                           join t_org_is_rtmob rt
                             on rt.org_id = tor.org_id
                          where rt.is_org_rtm = 0
                         connect by prior tor.org_id = tor.org_pid
                          start with tor.org_id in (l_org_id)))) A
         where /*(A.ID_OP is null or O.OP_TYPE = 22)
                                   and O.OP_ID = A.ID_OP
                                   and OU.OP_ID = O.OP_ID
                                   and CL.CLIENT_ID = A.CLIENT_ID
                                   and ou.tmc_id = A.AB_TMC_ID
                                   and*/
         A.ORG_ID is not null
         and not (A.ORG_ID = -1)
        /*and OU.OWNER_ID_0 is not null
        and not (OU.OWNER_ID_0 = -1)*/
         and (nvl(l_is_fes, 0) > 0 or /* o.op_dog_id*/
         a.dog_id = l_dog_id)
         group by A.AB_STATUS
         order by STATUS asc;
    else
      open res for
        select distinct A.AB_STATUS STATUS,
                        sum(A.AB_PAID) PAID,
                        count(*) "COUNT",
                        sum(a.with_ab_pay) count_with_ab_pay,
                        -- 33436 olia_serg
                        sum(A.AB_PAID_ESPO) AB_PAID_ESPO, -- Cумма с оплатой через агента
                        sum(A.AB_PAID_ESPP) AB_PAID_ESPP -- Cумма с оплатой через ЕСПП
          from /*T_TMC_OPERATIONS O,
                                                               T_TMC_OPERATION_UNITS OU,
                                                               T_CLIENTS CL,*/
               (select AB1.AB_ID,
                       AB1.CLIENT_ID,
                       ab1.ab_status,
                       AB1.AB_PAID,
                       AB1.AB_TMC_ID,
                       AB1.ORG_ID,
                       AB1.CHANGE_STATUS_DATE,
                       AB1.ID_OP,
                       -- 33436 olia_serg
                       (case nvl(AB1.With_Ab_Pay, 0)
                         when 0 then
                          AB1.AB_PAID -- Cумма с оплатой через агента
                         when 1 then
                          0 -- Cумма с оплатой через ЕСПП
                       end) AB_PAID_ESPO,
                       (case nvl(AB1.With_Ab_Pay, 0)
                         when 0 then
                          null -- Cумма с оплатой через агента
                         when 1 then
                          AB1.AB_PAID -- Cумма с оплатой через ЕСПП
                         when -1 then
                          AB1.AB_PAID -- Cумма с оплатой через ЕСПП
                       end) AB_PAID_ESPP,
                       nvl(AB1.with_ab_pay, 0) with_ab_pay,
                       ab1.dog_id
                  from T_ABONENT AB1, t_report_real_conn rpc
                 where rpc.id_repper = l_rp_id
                   and rpc.id_conn = ab1.ab_id
                   and rpc.ci = 1
                   and AB1.AB_STATUS in (104, 105)
                   and AB1.IS_DELETED = 0) A
         where /*(A.ID_OP is null or O.OP_TYPE = 22)
                                   and O.OP_ID = A.ID_OP
                                   and OU.OP_ID = O.OP_ID
                                   and CL.CLIENT_ID = A.CLIENT_ID
                                   and ou.tmc_id = A.AB_TMC_ID
                                   and*/
         A.ORG_ID is not null
         and not (A.ORG_ID = -1)
        /*and OU.OWNER_ID_0 is not null
        and not (OU.OWNER_ID_0 = -1)*/
         and (nvl(l_is_fes, 0) > 0 or /*o.op_dog_id*/
         a.dog_id = l_dog_id)
         group by A.AB_STATUS
         order by STATUS asc;
    end if;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Connections_Stat;
  -----------------------------------------------------------------------------------
  -- 43379 Аналог Get_Connections_Stat для ADSL
  -----------------------------------------------------------------------------------
  function Get_ADSL_Connections_Stat(pi_rp_id     in number,
                                     pi_worker_id in T_USERS.USR_ID%type,
                                     po_err_num   out pls_integer,
                                     po_err_msg   out t_Err_Msg,
                                     pi_id_org    in t_organizations.org_id%type := null,
                                     pi_month     in date := null)
    return sys_refcursor is
    res     sys_refcursor;
    l_rp_id number;
  begin
    if (pi_rp_id is not null) then
      l_rp_id := pi_rp_id;
    else
      begin
        select rp.id
          into l_rp_id
          from t_report_period rp
         where rp.id_org = pi_id_org
           and rp.is_actual = 1
           and trunc(rp.dates, 'MONTH') = trunc(pi_month, 'MONTH');
      exception
        when others then
          null;
      end;
    end if;
    OPEN RES FOR
      SELECT DISTINCT STATUS,
                      SUM(A.PAID) PAID,
                      COUNT(*) "COUNT",
                      SUM(A.AB_ESPO) count_espo,
                      SUM(A.AB_ESPP) count_espp,
                      SUM(A.AB_PAID_ESPO) AB_PAID_ESPO,
                      SUM(A.AB_PAID_ESPP) AB_PAID_ESPP,
                      SUM(A.ADVANCE_COST) ADVANCE_COST,
                      SUM(A.AB_advance_ESPO) AB_advance_ESPO,
                      SUM(A.AB_advance_ESPP) AB_advance_ESPP
        FROM (SELECT 0 STATUS,
                     nvl(TRPR.PRICE_BASE_FEE, 0) ADVANCE_COST,
                     nvl(TRPR.PRICE_BASE_COST, 0) PAID,
                     /*(CASE nvl(u.with_ab_paid, 0)
                       WHEN 0 THEN
                        nvl(u.abon_sum_without_param, 0)
                       WHEN 1 THEN
                        0
                     END)*/
                     nvl(TRPR.PRICE_BASE_FEE, 0) AB_advance_ESPO,
                     /*(CASE nvl(u.with_ab_paid, 0)
                       WHEN 0 THEN
                        0
                       WHEN 1 THEN
                        nvl(u.abon_sum_without_param, 0)
                     END)*/
                     0 AB_advance_ESPP,
                     nvl(TRPR.PRICE_BASE_COST, 0) AB_PAID_ESPO,
                     /*(CASE nvl(u.with_ab_paid, 0)
                       WHEN 0 THEN
                        0
                       WHEN 1 THEN
                        nvl(s.cost, 0)
                     END)*/
                     0 AB_PAID_ESPP,
                     /*(CASE nvl(u.with_ab_paid, 0)
                       WHEN 0 THEN
                        1
                       WHEN 1 THEN
                        0
                     END)*/
                     1 AB_ESPO,
                     /*(CASE nvl(u.with_ab_paid, 0)
                       WHEN 0 THEN
                        0
                       WHEN 1 THEN
                        1
                       WHEN 2 THEN
                        1
                     END)*/
                     0 AB_ESPP
                from T_REPORT_REAL_CONN RC
                join tr_request_service rs
                  on rs.id = rc.id_conn
                 and rc.ci = 2
                /*left join tr_product_option s
                  on s.service_id = rc.id_conn*/
        LEFT JOIN TR_REQUEST_PRICE TRPR ON TRPR.SERVICE_ID = RS.ID AND TRPR.IS_ACTUAL = 1
               WHERE RC.ID_REPPER = L_RP_ID) A
       GROUP BY A.STATUS
       ORDER BY STATUS asc;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_ADSL_Connections_Stat;
  ----------------------------------------------------------------------------
  function GetReportDetails(pi_rp_id     in number,
                            pi_worker_id in T_USERS.USR_ID%type,
                            po_err_num   out pls_integer,
                            po_err_msg   out t_Err_Msg,
                            pi_id_org    in t_organizations.org_id%type := null,
                            pi_month     in date := null)
    return sys_refcursor is
    res     sys_refcursor;
    l_rp_id number;
  begin
    if (pi_rp_id is not null) then
      l_rp_id := pi_rp_id;
    else
      begin
        select rp.id
          into l_rp_id
          from t_report_period rp
         where rp.id_org = pi_id_org
           and rp.is_actual = 1
           and trunc(rp.dates, 'MONTH') = trunc(pi_month, 'MONTH');
      exception
        when others then
          null;
      end;
    end if;
    open res for
      select org_dil.org_name ORG,
             org_dil.org_full_name ORG_FULL,
             org_kur.org_name KUR,
             rp.dates,
             rp.datepo,
             org_dil.org_id ORG_ID,
             org_kur.org_id KUR_ID,
             d.dog_id,
             d.dog_number,
             d.dog_date,
             d.premia_schema,
             d.with_nds,
             extract(MONTH from rp.dates) mon,
             extract(YEAR from rp.dates) yea,
             d.v_lice,
             d.na_osnovanii,
             org_dil.org_inn,
             org_dil.org_kpp,
             d.conn_type,
             rp.num_account,
             rp.num_zaiav_opl,
             rp.DT_ZAIAV_OPL,
             (select min(drp.dog_type) keep(dense_rank last order by drp.date_dog_type)
                from t_dogovor_report_form drp
               where drp.dog_id = d.dog_id
                 and rp.dates >= drp.date_dog_type) dog_type,
             ro.root_reltype org_reltype
        from t_report_period rp,
             t_organizations org_dil,
             t_organizations org_kur,
             mv_org_tree ro,
             t_dogovor       d
       where org_dil.org_id = rp.id_org
         and rp.is_actual = 1
         and l_rp_id = rp.id
         and rp.dog_id = d.dog_id
         and ro.org_id = rp.id_org
         and ro.root_reltype in (1003, 1004, 1007, 1008, 999)
         and (rp.id_org in
             (select * from TABLE(get_user_orgs_tab(pi_worker_id))))
         and org_kur.org_id = ro.org_pid;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end GetReportDetails;
  ----------------------------------------------------------------------------
  function GetReportPremiaMoney(pi_rp_id     in number,
                                pi_worker_id in T_USERS.USR_ID%type,
                                pi_flag_corr in number,
                                po_err_num   out pls_integer,
                                po_err_msg   out t_Err_Msg,
                                pi_id_org    in t_organizations.org_id%type := null,
                                pi_month     in date := null)
    return sys_refcursor is
    res sys_refcursor;
  begin
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end GetReportPremiaMoney;
  ----------------------------------------------------------------------------
  --43449 Активность абонентов(полный)
  ----------------------------------------------------------------------------
  function Abonent_Activity_count(pi_org_id       in array_num_2,
                                  pi_block        in number,
                                  pi_org_relation in num_tab,
                                  pi_period_beg   in date,
                                  pi_period_end   in date,
                                  pi_date         in date,
                                  pi_type_date    in number, --0-один месяц активности и период подключения; 1-по выбранному периоду активности и месяцу подключения
                                  pi_type         in number, --1-отчет завязан на одобренных АСР;2-по дате активности;3-отчет завязан на отчете агента
                                  pi_worker_id    in T_USERS.USR_ID%type,
                                  po_err_num      out pls_integer,
                                  po_err_msg      out varchar2) return number is
    res     number;
    org_tab num_tab := num_tab();
  begin
    org_tab := get_orgs_tab_for_multiset(pi_orgs         => pi_org_id,
                                         Pi_worker_id    => pi_worker_id,
                                         pi_block        => pi_block,
                                         pi_org_relation => pi_org_relation,
                                         pi_is_rtmob     => 1);
    select count(*)
      into res
      from T_ABONENT ab
      left join t_ab_pay_activity apa
        on apa.abonent_id = ab.ab_id
       and apa.asr_id > 0
       and trunc(apa.date_begin) = trunc(pi_date)
       and nvl(apa.amount_pay, 0) > 0
     where ab.org_id in (select /*+ PRECOMPUTE_SUBQUERY */
                          *
                           from table(org_tab))
       and (ab.change_status_date between
           nvl(pi_period_beg, Add_Months(pi_date, -6)) and
           nvl(add_months(pi_period_end, 1), add_months(pi_date, 1)))
       and ab.is_deleted = 0
       and ab.ab_status in (104, 105);

    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  ----------------------------------------------------------------------------
  --43449 Активность абонентов(полный)
  ----------------------------------------------------------------------------
  function Abonent_Activity(pi_org_id       in array_num_2,
                            pi_block        in number,
                            pi_org_relation in num_tab,
                            pi_period_beg   in date,
                            pi_period_end   in date,
                            pi_date         in date,
                            pi_type_date    in number, --0-один месяц активности и период подключения; 1-по выбранному периоду активности и месяцу подключения
                            pi_type         in number, --1-отчет завязан на одобренных АСР;2-по дате активности;3-отчет завязан на отчете агента
                            pi_worker_id    in T_USERS.USR_ID%type,
                            pi_is_full      in number,
                            pi_num_page     in number, -- номер страницы
                            pi_count_req    in number, -- кол-во записей на странице
                            pi_column       in number, -- Номер колонки для сортировки
                            pi_sorting      in number, -- 0-по возрастанияю, 1-по убыванию
                            po_all_count    out number, -- выводим общее кол-во записей, подходящих под условия
                            po_full_rep     out sys_refcursor,
                            po_summ         out sys_refcursor,
                            po_err_num      out pls_integer,
                            po_err_msg      out varchar2)
    return sys_refcursor is
    res            sys_refcursor;
    rights_value   varchar2(100);
    org_tab        num_tab := num_tab();
    l_col_request  request_Order_Tab;
    l_col_list     abonent_activ_list_tab;
    l_col_count    rep_period_count_tab;
    l_order_asc    number; -- по возрастанияю
    l_order_desc   number; -- по убыванию
    l_max_num_page number;
    l_num_page     number;
    l_column       number;
    l_org_tab      num_tab;
  begin
    logging_pkg.debug('pi_org_id: ' ||
                      get_str_by_array_num_2(pi_org_id, 5) || 'pi_block: ' ||
                      pi_block || chr(10) || 'pi_period_beg' ||
                      pi_period_beg || chr(10) || 'pi_period_end' ||
                      pi_period_end || chr(10) || 'pi_date' || pi_date ||
                      chr(10) || 'pi_type_date' || pi_type_date || chr(10) ||
                      'pi_type' || pi_type,
                      'Abonent_Activity');
    open po_summ for
      select null as date_m,
             null as amount_pay,
             null as amount_place,
             null as ab_paid
        from dual
       where 1 = 0;
    -- Если параметры не переданы -- сортирует по второму полю, в по возростанию.
    l_column := nvl(pi_column, 2);
    --
    If nvl(pi_sorting, 0) = 0 then
      l_order_asc := l_column;
    else
      l_order_desc := l_column;
    end If;
    --
    if (pi_is_full = 1) then
      rights_value := 'EISSD.TMC_REPORTS.ABONENT_ACTIVITY_FULL';
    else
      rights_value := 'EISSD.REPORT.ABONENT_ACTIVITY_SHORT';
    end if;

    select t.number_1 bulk collect into l_org_tab from table(pi_org_id) t;

    if (not Security_pkg.Check_Rights_Orgs_str(rights_value,
                                               l_org_tab,
                                               pi_worker_id,
                                               po_err_num,
                                               po_err_msg)) then
      po_full_rep := null;
      return null;
    end if;
    org_tab := get_orgs_tab_for_multiset(pi_orgs         => pi_org_id,
                                         Pi_worker_id    => pi_worker_id,
                                         pi_block        => 1,
                                         pi_org_relation => pi_org_relation,
                                         pi_is_rtmob     => 1);
    -- статистика по 1.подключенным абонентам
    if pi_type = 1 then

      open res for
        select trunc(ab.change_status_date - 2 / 24, 'mm') months,
               count(distinct ab.ab_id) count_all,
               count(distinct apa.abonent_id) count_act,
               sum(apa.amount_pay) sum_amount
          from T_ABONENT ab
          left join t_ab_pay_activity apa
            on apa.abonent_id = ab.ab_id
           and apa.asr_id > 0
           and trunc(apa.date_begin) = trunc(pi_date)
           and nvl(apa.amount_pay, 0) > 0
         where ab.org_id in (select /*+ PRECOMPUTE_SUBQUERY */
                              *
                               from table(org_tab))
           and (ab.change_status_date - 2 / 24 between
               nvl(pi_period_beg, Add_Months(pi_date, -6)) and
               nvl(add_months(pi_period_end, 1) - 1 / 24 / 60 / 60,
                    add_months(pi_date, 1) - 1 / 24 / 60 / 60))
           and ab.is_deleted = 0
           and ab.ab_status in (104, 105)
         group by trunc(ab.change_status_date - 2 / 24, 'mm')
         order by trunc(ab.change_status_date - 2 / 24, 'mm');

    elsif pi_type = 2 then
      --статистика по 2.активным абонентам
      open res for
        select months,
               sum(count_all) count_all,
               sum(count_act) count_act,
               0 sum_amount
          from (select trunc(aa.data_activated - 2 / 24, 'mm') months,
                       0 count_all,
                       count(distinct ab.ab_id) count_act
                  from t_abonent ab
                  join t_abonent_activated aa
                    on aa.abonent_id = ab.ab_id
                 where ab.org_id in (select /*+ PRECOMPUTE_SUBQUERY */
                                      *
                                       from table(org_tab))
                   and ab.ab_status in (104, 105)
                   and ab.is_deleted = 0
                   and (aa.data_activated between
                       nvl(pi_period_beg, Add_Months(pi_date, -6)) + 2 / 24 and
                       nvl(add_months(pi_period_end, 1) - 1 / 24 / 60 / 60,
                            add_months(pi_date, 1) - 1 / 24 / 60 / 60) +
                       2 / 24)
                 group by trunc(aa.data_activated - 2 / 24, 'mm')
                union
                select trunc(ab.change_status_date - 2 / 24, 'mm') months,
                       count(distinct ab.ab_id) count_all,
                       0 count_act
                  from T_ABONENT ab
                 where ab.org_id in (select /*+ PRECOMPUTE_SUBQUERY */
                                      *
                                       from table(org_tab))
                   and (ab.change_status_date between
                       nvl(pi_period_beg, Add_Months(pi_date, -6)) + 2 / 24 and
                       nvl(add_months(pi_period_end, 1) - 1 / 24 / 60 / 60,
                            add_months(pi_date, 1) - 1 / 24 / 60 / 60) +
                       2 / 24)
                   and ab.is_deleted = 0
                   and ab.ab_status in (104, 105)
                 group by trunc(ab.change_status_date - 2 / 24, 'mm')) tt
         group by months
         order by months;
    elsif pi_type = 3 then
      --статистика по 3.активным абонентам
      select rep_period_count_type(months, count_all, count_act, sum_amount) bulk collect
        into l_col_count
      -- open res for
        from (select months,
                     sum(count_all) count_all,
                     sum(count_act) count_act,
                     0 sum_amount
                from (with o_tab as(select /*+ PRECOMPUTE_SUBQUERY */
                                          column_value org_id
                                           from table(org_tab))
                      select dates months,
                             0 count_all,
                             count(distinct ab_id) count_act
                        from (select max(rp.dates) dates, ab_id
                                from t_report_real_conn rc
                                join t_report_period rp
                                  on rp.id = rc.id_repper
                                 and rc.ci = 1
                                join t_abonent ab
                                  on ab.ab_id = rc.id_conn
                                join o_tab org
                                  on ab.org_id = org.org_id
                               where ab.ab_status in (104, 105)
                                 and ab.is_deleted = 0
                                 and rp.is_actual = 1
                                 and rp.id_org in (select org_id from o_tab)
                                 and (pi_type_date = 0 and
                                     rp.dates between pi_period_beg and
                                     add_months(pi_period_end, 1) -
                                     1 / 24 / 60 / 60)
                               group by ab.ab_id)
                       group by dates
                      union
                      select dates months,
                             0 count_all,
                             count(distinct ab_id) count_act
                        from (select max(rp.dates) dates, ab.ab_id
                                from t_report_real_conn rc
                                join t_report_period rp
                                  on rp.id = rc.id_repper
                                 and rc.ci = 1
                                join t_abonent ab
                                  on ab.ab_id = rc.id_conn
                                join (select /*+ PRECOMPUTE_SUBQUERY */
                                      column_value org_id
                                       from table(org_tab)) org
                                  on ab.org_id = org.org_id
                               where ab.ab_status in (104, 105)
                                 and ab.is_deleted = 0
                                 and rp.is_actual = 1
                                 and rp.id_org in (select org_id from o_tab)
                                 and (pi_type_date = 1 and
                                     rp.dates between pi_date and
                                     add_months(pi_date, 1) -
                                     1 / 24 / 60 / 60)
                               group by ab.ab_id)
                       group by dates
                      union
                      select trunc(ab.change_status_date - 2 / 24, 'mm') months,
                             count(distinct ab.ab_id) count_all,
                             0 count_act
                        from T_ABONENT ab
                       where ab.org_id in
                             (select /*+ PRECOMPUTE_SUBQUERY */
                               *
                                from table(org_tab))
                         and (pi_type_date = 0 and ab.change_status_date between
                             pi_period_beg + 2 / 24 and
                             add_months(pi_period_end, 1) - 1 / 24 / 60 / 60 +
                             2 / 24)
                         and ab.is_deleted = 0
                         and ab.ab_status in (104, 105)
                       group by trunc(ab.change_status_date - 2 / 24, 'mm')
                      union
                      select trunc(ab.change_status_date - 2 / 24, 'mm') months,
                             count(distinct ab.ab_id) count_all,
                             0 count_act
                        from T_ABONENT ab
                       where ab.org_id in
                             (select /*+ PRECOMPUTE_SUBQUERY */
                               *
                                from table(org_tab))
                         and (pi_type_date = 1 and
                             ab.change_status_date between pi_date + 2 / 24 and
                             add_months(pi_date, 1) - 1 / 24 / 60 / 60 +
                             2 / 24)
                         and ab.is_deleted = 0
                         and ab.ab_status in (104, 105)
                       group by trunc(ab.change_status_date - 2 / 24, 'mm')) tt
               group by months
               order by months);
      open res for
        select months, count_all, count_act, sum_amount
          from table(l_col_count) col;

    end if;
    -- подробная информация по активности абонентов
    if (not Security_pkg.Check_Rights_Orgs_str(rights_value,
                                               l_org_tab,
                                               pi_worker_id,
                                               po_err_num,
                                               po_err_msg/*,
                                               1 = 1,
                                               1 = 1*/)) then
      po_full_rep := null;
      open po_summ for
        select null as date_m,
               null as amount_pay,
               null as amount_place,
               null as ab_paid
          from dual
         where 1 = 0;
    else
      if pi_type = 1 then
        -- сформируем сортированный список
        select request_Order_Type(ab_id, rownum) bulk collect
          into l_col_request
          from (select ab.ab_id AB_ID
                  from T_ABONENT ab
                  join t_clients ca
                    on ca.client_id = ab.client_id
                  left join t_person pa
                    on pa.person_id = ca.fullinfo_id
                   and ca.client_type = 'P'
                  left join t_juristic j
                    on j.juristic_id = ca.fullinfo_id
                   and ca.client_type = 'J'
                  left join T_AB_PAY_ACTIVITY apac
                    on ab.ab_id = apac.abonent_id
                   and trunc(apac.date_begin) = trunc(pi_date)
                   and apac.asr_id > 0
                  join T_ORGANIZATIONS org
                    on ab.org_id = org.org_id
                  left join t_organizations orgR
                    on orgR.Org_Id = ab.root_org_id --rel.org_pid
                  left join t_dic_region reg1
                    on reg1.reg_id = org.region_id
                  left join t_dic_mrf mrf
                    on mrf.id = reg1.mrf_id
                   and ab.root_org_id in (0, 1)
                  join t_tmc_sim ts
                    on ts.tmc_id = ab.ab_tmc_id
                  left join t_tarif_by_at_id tar
                    on ts.tar_id = tar.AT_ID
                  left join t_users u
                    on ab.worker_id = u.usr_id
                  left join t_person pu
                    on u.usr_person_id = pu.person_id
                  left join t_users sel_u
                    on sel_u.usr_id = ab.user_id
                  left join t_person sel_p
                    on sel_p.person_id = sel_u.usr_person_id
                 where ab.org_id in (select /*+ PRECOMPUTE_SUBQUERY */
                                      *
                                       from table(org_tab))
                   and (ab.change_status_date between
                       nvl(pi_period_beg, Add_Months(pi_date, -6)) + 2 / 24 and
                       nvl(add_months(pi_period_end, 1) - 1 / 24 / 60 / 60,
                            add_months(pi_date, 1) - 1 / 24 / 60 / 60) +
                       2 / 24)
                   and ab.is_deleted = 0
                   and ab.ab_status in (104, 105)
                 order by decode(l_order_asc,
                                 null,
                                 null,
                                 3,
                                 (case
                                   when mrf.name_mrf is null then
                                    orgR.Org_Name
                                   else
                                    'МРФ ' || mrf.name_mrf
                                 end),
                                 5,
                                 org.org_name,
                                 6,
                                 NVL(TO_CHAR(ts.sim_callsign), ''),
                                 7,
                                 NVL(TO_CHAR(ts.sim_imsi), ''),
                                 12,
                                 tar.title,
                                 14,
                                 (case
                                   when ca.client_type = 'P' then
                                    trim(pa.person_lastname) || ' ' || trim(pa.person_firstname) || ' ' ||
                                    trim(pa.person_middlename)
                                   else
                                    j.jur_name
                                 end),
                                 15,
                                 trim(sel_p.person_lastname) || ' ' ||
                                 trim(sel_p.person_firstname) || ' ' ||
                                 trim(sel_p.person_middlename),
                                 null) asc,
                          decode(l_order_asc,
                                 9,
                                 NVL(apac.amount_pay, 0),
                                 10,
                                 NVL(apac.amount_place, 0),
                                 null) asc,
                          decode(l_order_asc,
                                 8,
                                 to_date(ab.change_status_date, 'dd.mm.yyyy'),
                                 null) asc,
                          decode(l_order_desc,
                                 null,
                                 null,
                                 3,
                                 (case
                                   when mrf.name_mrf is null then
                                    orgR.Org_Name
                                   else
                                    'МРФ ' || mrf.name_mrf
                                 end),
                                 5,
                                 org.org_name,
                                 6,
                                 NVL(TO_CHAR(ts.sim_callsign), ''),
                                 7,
                                 NVL(TO_CHAR(ts.sim_imsi), ''),
                                 12,
                                 tar.title,
                                 14,
                                 (case
                                   when ca.client_type = 'P' then
                                    trim(pa.person_lastname) || ' ' || trim(pa.person_firstname) || ' ' ||
                                    trim(pa.person_middlename)
                                   else
                                    j.jur_name
                                 end),
                                 15,
                                 trim(sel_p.person_lastname) || ' ' ||
                                 trim(sel_p.person_firstname) || ' ' ||
                                 trim(sel_p.person_middlename),
                                 null) desc,
                          decode(l_order_desc,
                                 9,
                                 NVL(apac.amount_pay, 0),
                                 10,
                                 NVL(apac.amount_place, 0),
                                 null) desc,
                          decode(l_order_desc,
                                 8,
                                 to_date(ab.change_status_date, 'dd.mm.yyyy'),
                                 null) desc);

        po_all_count := l_col_request.count;

        l_max_num_page := round(po_all_count / nvl(pi_count_req, 1));

        if (pi_num_page > l_max_num_page and l_max_num_page <> 0) then
          l_num_page := l_max_num_page + 1;
        else
          l_num_page := nvl(pi_num_page, 1);
        end if;

        open po_full_rep for
          select distinct ab.ab_id AB_ID,
                          orgr.org_id ROOT_ORG_ID,
                          orgR.Org_Name ROOT_ORG_NAME,
                          org.org_id ORG_ID,
                          org.org_name ORG_NAME,
                          NVL(TO_CHAR(ts.sim_callsign), '') CALLSIGN,
                          NVL(TO_CHAR(ts.sim_imsi), '') IMSI,
                          trunc(ab.change_status_date - 2 / 24) DATE_ASR,
                          NVL(apac.amount_pay, 0) AMOUNT_PAY,
                          NVL(apac.amount_place, 0) amount_place,
                          ts.tar_id,
                          tar.title tar_name,
                          trim(pu.person_lastname) || ' ' ||
                          trim(pu.person_firstname) || ' ' ||
                          trim(pu.person_middlename) person,
                          (case
                            when ca.client_type = 'P' then
                             trim(pa.person_lastname) || ' ' ||
                             trim(pa.person_firstname) || ' ' ||
                             trim(pa.person_middlename)
                            else
                             j.jur_name
                          end) abonent,
                          trim(sel_p.person_lastname) || ' ' ||
                          trim(sel_p.person_firstname) || ' ' ||
                          trim(sel_p.person_middlename) seller,
                          sel_u.employee_number seller_emp_num,
                          null data_activated,
                          rn
            from (Select request_id, rn
                    from table(l_col_request)
                   where rn between
                         (l_num_page - 1) * nvl(pi_count_req, po_all_count) + 1 and
                         (l_num_page) * nvl(pi_count_req, po_all_count)) col_rc
            join T_ABONENT ab
              on ab.ab_id = col_rc.request_id
            join t_clients ca
              on ca.client_id = ab.client_id
            left join t_person pa
              on pa.person_id = ca.fullinfo_id
             and ca.client_type = 'P'
            left join t_juristic j
              on j.juristic_id = ca.fullinfo_id
             and ca.client_type = 'J'
            left join T_AB_PAY_ACTIVITY apac
              on ab.ab_id = apac.abonent_id
             and trunc(apac.date_begin) = trunc(pi_date)
             and apac.asr_id > 0
            join T_ORGANIZATIONS org
              on ab.org_id = org.org_id
            left join t_organizations orgR
              on orgR.org_id = org.ROOT_ORG_ID2
            join t_tmc_sim ts
              on ts.tmc_id = ab.ab_tmc_id
            left join t_tarif_by_at_id tar
              on ts.tar_id = tar.AT_ID
            left join t_users u
              on ab.worker_id = u.usr_id
            left join t_person pu
              on u.usr_person_id = pu.person_id
            left join t_users sel_u
              on sel_u.usr_id = ab.user_id
            left join t_person sel_p
              on sel_p.person_id = sel_u.usr_person_id
           where 1 = 1
           order by rn;

        if (pi_is_full = 1) then
          -- для полной версии сформируем также суммарную строку
          open po_summ for
            select null date_m,
                   sum(NVL(apac.amount_pay, 0)) amount_pay,
                   sum(NVL(apac.amount_place, 0)) amount_place,
                   0 ab_paid
              from (Select request_id, rn from table(l_col_request)) col_rc
              join T_ABONENT ab
                on ab.ab_id = col_rc.request_id
              join t_clients ca
                on ca.client_id = ab.client_id
              left join t_person pa
                on pa.person_id = ca.fullinfo_id
              left join T_AB_PAY_ACTIVITY apac
                on ab.ab_id = apac.abonent_id
               and trunc(apac.date_begin) = trunc(pi_date)
               and apac.asr_id > 0
              join T_ORGANIZATIONS org
                on ab.org_id = org.org_id
              left join t_organizations orgR
                on orgR.Org_Id = ab.root_org_id --rel.org_pid
              left join t_dic_region reg1
                on reg1.reg_id = org.region_id
              left join t_dic_mrf mrf
                on mrf.id = reg1.mrf_id
               and ab.root_org_id in (0, 1)
              join t_tmc_sim ts
                on ts.tmc_id = ab.ab_tmc_id
              left join t_tarif_by_at_id tar
                on ts.tar_id = tar.AT_ID
              left join t_users u
                on ab.worker_id = u.usr_id
              left join t_person pu
                on u.usr_person_id = pu.person_id
              left join t_users sel_u
                on sel_u.usr_id = ab.user_id
              left join t_person sel_p
                on sel_p.person_id = sel_u.usr_person_id
             where 1 = 1;
        end if;

      elsif pi_type = 2 then
        -- сформируем сортированный список
        select request_Order_Type(ab_id, rownum) bulk collect
          into l_col_request
          from (select ab.ab_id AB_ID
                   from T_ABONENT ab
                   join t_abonent_activated aa
                     on aa.abonent_id = ab.ab_id
                   join t_clients ca
                     on ca.client_id = ab.client_id
                   left join t_person pa
                     on pa.person_id = ca.fullinfo_id
                    and ca.client_type = 'P'
                   left join t_juristic j
                     on j.juristic_id = ca.fullinfo_id
                    and ca.client_type = 'J'
                   left join T_AB_PAY_ACTIVITY apac
                     on ab.ab_id = apac.abonent_id
                    and trunc(apac.date_begin) = trunc(pi_date)
                    and apac.asr_id > 0
                   join T_ORGANIZATIONS org
                     on ab.org_id = org.org_id
                   left join t_organizations orgR
                     on orgR.Org_Id = ab.root_org_id --rel.org_pid
                   left join t_dic_region reg1
                     on reg1.reg_id = org.region_id
                   left join t_dic_mrf mrf
                     on mrf.id = reg1.mrf_id
                    and ab.root_org_id in (0, 1)
                   join t_tmc_sim ts
                     on ts.tmc_id = ab.ab_tmc_id
                   left join t_tarif_by_at_id tar
                     on ts.tar_id = tar.AT_ID
                   left join t_users u
                     on ab.worker_id = u.usr_id
                   left join t_person pu
                     on u.usr_person_id = pu.person_id
                   left join t_users sel_u
                     on sel_u.usr_id = ab.user_id
                   left join t_person sel_p
                     on sel_p.person_id = sel_u.usr_person_id
                  where /*aa.data_activated < last_day(trunc(rp.dates)) + 1*/
                  ab.org_id in (select /*+ PRECOMPUTE_SUBQUERY */
                                 *
                                  from table(org_tab))
               and ab.ab_status in (104, 105)
               and ab.is_deleted = 0
                 --and rp.is_actual = 1
                 --and aa.data_activated < last_day(trunc(rp.dates)) + 1
               and (aa.data_activated between
                  nvl(pi_period_beg, Add_Months(pi_date, -6)) + 2 / 24 and
                  nvl(add_months(pi_period_end, 1) - 1 / 24 / 60 / 60,
                       add_months(pi_date, 1) - 1 / 24 / 60 / 60) + 2 / 24)
                  order by decode(l_order_asc,
                                  null,
                                  null,
                                  3,
                                  (case
                                    when mrf.name_mrf is null then
                                     orgR.Org_Name
                                    else
                                     'МРФ ' || mrf.name_mrf
                                  end),
                                  5,
                                  org.org_name,
                                  6,
                                  NVL(TO_CHAR(ts.sim_callsign), ''),
                                  7,
                                  NVL(TO_CHAR(ts.sim_imsi), ''),
                                  12,
                                  tar.title,
                                  14,
                                  (case
                                    when ca.client_type = 'P' then
                                     trim(pa.person_lastname) || ' ' || trim(pa.person_firstname) || ' ' ||
                                     trim(pa.person_middlename)
                                    else
                                     j.jur_name
                                  end),
                                  15,
                                  trim(sel_p.person_lastname) || ' ' ||
                                  trim(sel_p.person_firstname) || ' ' ||
                                  trim(sel_p.person_middlename),
                                  null) asc,
                           decode(l_order_asc,
                                  9,
                                  NVL(apac.amount_pay, 0),
                                  10,
                                  NVL(apac.amount_place, 0),
                                  null) asc,
                           decode(l_order_asc,
                                  8,
                                  to_date(ab.change_status_date, 'dd.mm.yyyy'),
                                  null) asc,
                           decode(l_order_desc,
                                  null,
                                  null,
                                  3,
                                  (case
                                    when mrf.name_mrf is null then
                                     orgR.Org_Name
                                    else
                                     'МРФ ' || mrf.name_mrf
                                  end),
                                  5,
                                  org.org_name,
                                  6,
                                  NVL(TO_CHAR(ts.sim_callsign), ''),
                                  7,
                                  NVL(TO_CHAR(ts.sim_imsi), ''),
                                  12,
                                  tar.title,
                                  14,
                                  (case
                                    when ca.client_type = 'P' then
                                     trim(pa.person_lastname) || ' ' || trim(pa.person_firstname) || ' ' ||
                                     trim(pa.person_middlename)
                                    else
                                     j.jur_name
                                  end),
                                  15,
                                  trim(sel_p.person_lastname) || ' ' ||
                                  trim(sel_p.person_firstname) || ' ' ||
                                  trim(sel_p.person_middlename),
                                  null) desc,
                           decode(l_order_desc,
                                  9,
                                  NVL(apac.amount_pay, 0),
                                  10,
                                  NVL(apac.amount_place, 0),
                                  null) desc,
                           decode(l_order_desc,
                                  8,
                                  to_date(ab.change_status_date, 'dd.mm.yyyy'),
                                  null) desc);

        po_all_count := l_col_request.count;

        l_max_num_page := round(po_all_count / nvl(pi_count_req, 1));

        if (pi_num_page > l_max_num_page and l_max_num_page <> 0) then
          l_num_page := l_max_num_page + 1;
        else
          l_num_page := nvl(pi_num_page, 1);
        end if;

        open po_full_rep for
          select distinct ab.ab_id    AB_ID,
                          orgr.org_id ROOT_ORG_ID,

                          orgR.Org_Name ROOT_ORG_NAME,
                          org.org_id ORG_ID,
                          org.org_name ORG_NAME,
                          NVL(TO_CHAR(ts.sim_callsign), '') CALLSIGN,
                          NVL(TO_CHAR(ts.sim_imsi), '') IMSI,
                          trunc(ab.change_status_date - 2 / 24) DATE_ASR,
                          NVL(apac.amount_pay, 0) AMOUNT_PAY,
                          NVL(apac.amount_place, 0) amount_place,
                          ts.tar_id,
                          tar.title tar_name,
                          trim(pu.person_lastname) || ' ' ||
                          trim(pu.person_firstname) || ' ' ||
                          trim(pu.person_middlename) person,
                          (case
                            when ca.client_type = 'P' then
                             trim(pa.person_lastname) || ' ' ||
                             trim(pa.person_firstname) || ' ' ||
                             trim(pa.person_middlename)
                            else
                             j.jur_name
                          end) abonent,
                          trim(sel_p.person_lastname) || ' ' ||
                          trim(sel_p.person_firstname) || ' ' ||
                          trim(sel_p.person_middlename) seller,
                          sel_u.employee_number seller_emp_num,
                          aa.data_activated - 2 / 24 data_activated,
                          rn
            from (Select request_id, rn
                    from table(l_col_request)
                   where rn between
                         (l_num_page - 1) * nvl(pi_count_req, po_all_count) + 1 and
                         (l_num_page) * nvl(pi_count_req, po_all_count)) col_rc
            join T_ABONENT ab
              on ab.ab_id = col_rc.request_id
            join t_abonent_activated aa
              on aa.abonent_id = ab.ab_id
            join t_clients ca
              on ca.client_id = ab.client_id
            left join t_person pa
              on pa.person_id = ca.fullinfo_id
             and ca.client_type = 'P'
            left join t_juristic j
              on j.juristic_id = ca.fullinfo_id
             and ca.client_type = 'J'
            left join T_AB_PAY_ACTIVITY apac
              on ab.ab_id = apac.abonent_id
             and trunc(apac.date_begin) = trunc(pi_date)
             and apac.asr_id > 0
            join T_ORGANIZATIONS org
              on ab.org_id = org.org_id
            left join t_organizations orgR
              on orgR.org_id = org.ROOT_ORG_ID2
            join t_tmc_sim ts
              on ts.tmc_id = ab.ab_tmc_id
            left join t_tarif_by_at_id tar
              on ts.tar_id = tar.AT_ID
            left join t_users u
              on ab.worker_id = u.usr_id
            left join t_person pu
              on u.usr_person_id = pu.person_id
            left join t_users sel_u
              on sel_u.usr_id = ab.user_id
            left join t_person sel_p
              on sel_p.person_id = sel_u.usr_person_id
           where 1 = 1
           order by rn;

        if (pi_is_full = 1) then
          -- для полной версии сформируем также суммарную строку
          open po_summ for
            select null date_m,
                   sum(NVL(apac.amount_pay, 0)) AMOUNT_PAY,
                   sum(NVL(apac.amount_place, 0)) amount_place,
                   0 ap_paid
              from (Select request_id, rn from table(l_col_request)) col_rc
              join T_ABONENT ab
                on ab.ab_id = col_rc.request_id
              join t_abonent_activated aa
                on aa.abonent_id = ab.ab_id
              join t_clients ca
                on ca.client_id = ab.client_id
              left join t_person pa
                on pa.person_id = ca.fullinfo_id
              left join T_AB_PAY_ACTIVITY apac
                on ab.ab_id = apac.abonent_id
               and trunc(apac.date_begin) = trunc(pi_date)
               and apac.asr_id > 0
              join T_ORGANIZATIONS org
                on ab.org_id = org.org_id
              left join t_organizations orgR
                on orgR.Org_Id = ab.root_org_id --rel.org_pid
              left join t_dic_region reg1
                on reg1.reg_id = org.region_id
              left join t_dic_mrf mrf
                on mrf.id = reg1.mrf_id
               and ab.root_org_id in (0, 1)
              join t_tmc_sim ts
                on ts.tmc_id = ab.ab_tmc_id
              left join t_tarif_by_at_id tar
                on ts.tar_id = tar.AT_ID
              left join t_users u
                on ab.worker_id = u.usr_id
              left join t_person pu
                on u.usr_person_id = pu.person_id
              left join t_users sel_u
                on sel_u.usr_id = ab.user_id
              left join t_person sel_p
                on sel_p.person_id = sel_u.usr_person_id
             where 1 = 1;
        end if;
      elsif pi_type = 3 then
        -- сформируем сортированный список
        select abonent_activ_list_type(ab_id,
                                       root_org_id,
                                       root_org_name,
                                       org_id,
                                       org_name,
                                       callsign,
                                       imsi,
                                       date_asr,
                                       amount_pay,
                                       amount_place,
                                       tar_id,
                                       tar_name,
                                       person,
                                       abonent,
                                       seller,
                                       seller_emp_num,
                                       data_activated,
                                       date_act,
                                       ab_cost,
                                       ab_paid,
                                       dates,
                                       rownum) bulk collect
          into l_col_list
          from (select *
                  from (select AB_ID,
                               ROOT_ORG_ID,
                               ROOT_ORG_NAME,
                               ORG_ID,
                               ORG_NAME,
                               CALLSIGN,
                               IMSI,
                               DATE_ASR,
                               AMOUNT_PAY,
                               sum(amount_place) amount_place, --apac.amount_place
                               tar_id,
                               tar_name,
                               person,
                               abonent,
                               seller,
                               seller_emp_num,
                               data_activated,
                               date_act,
                               ab_cost,
                               ab_paid,
                               max(dates) dates
                          from (with o_tab as(select /*+ PRECOMPUTE_SUBQUERY */
                                          column_value org_id
                                           from table(org_tab))

                          select distinct ab.ab_id AB_ID,
                                                orgr.org_id ROOT_ORG_ID,
                                                orgR.Org_Name ROOT_ORG_NAME,
                                                org.org_id ORG_ID,
                                                org.org_name ORG_NAME,
                                                NVL(TO_CHAR(ts.sim_callsign),
                                                    '') CALLSIGN,
                                                NVL(TO_CHAR(ts.sim_imsi), '') IMSI,
                                                trunc(ab.change_status_date -
                                                      2 / 24) DATE_ASR,
                                                NVL(apac.amount_pay, 0) AMOUNT_PAY,
                                                (case
                                                  when mp.date_begin >= to_date('01.08.2013', 'dd.mm.yyyy') /*perov-av 65009*/
                                                   then
                                                   NVL(mp.amount, 0) / 100
                                                  else
                                                   NVL(mp.amount, 0)
                                                end) amount_place,
                                                ts.tar_id,
                                                tar.title tar_name,
                                                trim(pu.person_lastname) || ' ' ||
                                                trim(pu.person_firstname) || ' ' ||
                                                trim(pu.person_middlename) person,
                                                (case
                                                  when ca.client_type = 'P' then
                                                   trim(pa.person_lastname) || ' ' || trim(pa.person_firstname) || ' ' ||
                                                   trim(pa.person_middlename)
                                                  else
                                                   j.jur_name
                                                end) abonent,
                                                trim(sel_p.person_lastname) || ' ' ||
                                                trim(sel_p.person_firstname) || ' ' ||
                                                trim(sel_p.person_middlename) seller,
                                                sel_u.employee_number seller_emp_num,
                                                aa.data_activated - 2 / 24 data_activated,
                                                (case
                                                  when pi_type_date = 0 then
                                                   apac.date_begin
                                                  else
                                                   tmp_m.dates
                                                -- apac.date_begin
                                                end) /*apac.date_begin*/ date_act,
                                                ab.ab_cost / 100 ab_cost,
                                                ab.ab_paid / 100 ab_paid,
                                                rp.dates
                                  from t_report_period rp
                                  join t_report_real_conn rc
                                    on rp.id = rc.id_repper
                                  join T_ABONENT ab
                                    on rc.id_conn = ab.ab_id
                                   and rc.ci = 1
                                  join o_tab org
                                    on ab.org_id = org.org_id
                                  left join t_abonent_activated aa
                                    on aa.abonent_id = ab.ab_id
                                  join t_clients ca
                                    on ca.client_id = ab.client_id
                                  left join t_person pa
                                    on pa.person_id = ca.fullinfo_id
                                   and ca.client_type = 'P'
                                  left join t_juristic j
                                    on j.juristic_id = ca.fullinfo_id
                                   and ca.client_type = 'J'
                                -- генерим месяцы чтобы по каждому абоненту было одинаковое число записей
                                -- даже если не было активности
                                  join (Select add_months(trunc(case
                                                                 when pi_type_date = 1 then
                                                                  pi_period_beg
                                                                 else
                                                                  pi_date --pi_period_beg
                                                               end,
                                                               'mm'),
                                                         level - 1) dates
                                         from dual
                                       Connect by level <= ((case
                                                    when pi_type_date = 1 then
                                                     months_between(pi_period_end, pi_period_beg)
                                                    else
                                                     months_between(pi_date, pi_date)
                                                  end) + 1)) tmp_m
                                    on 1 = 1
                                  left join T_AB_PAY_ACTIVITY apac
                                    on ab.ab_id = apac.abonent_id
                                   and (( /*pi_type_date = 0  and */
                                        trunc(apac.date_begin) =
                                        trunc(tmp_m.dates))
                                       /*or (pi_type_date = 1  and trunc(apac.date_begin) between pi_period_beg and
                                       pi_period_end)*/
                                       )
                                      /*and (pi_type_date = 0 or trunc(apac.date_begin) between
                                      LEAST(pi_period_beg, pi_date) and pi_period_end)*/ /*=
                                                                                                                                                                                                 trunc(tmp_m.dates))*/
                                   and apac.asr_id > 0
                                /*and ((pi_type_date = 0 and trunc(apac.date_begin) =
                                trunc(pi_date)) or
                                (pi_type_date = 1 and trunc(apac.date_begin) between
                                pi_period_beg and pi_period_end))*/
                                  join T_ORGANIZATIONS org
                                    on ab.org_id = org.org_id
                                  left join t_organizations orgR
                                    on orgR.org_id = org.root_org_id2
                                  join t_tmc_sim ts
                                    on ts.tmc_id = ab.ab_tmc_id
                                  left join t_tarif_by_at_id tar
                                    on ts.tar_id = tar.AT_ID
                                  left join t_users u
                                    on ab.worker_id = u.usr_id
                                  left join t_person pu
                                    on u.usr_person_id = pu.person_id
                                  left join t_users sel_u
                                    on sel_u.usr_id = ab.user_id
                                  left join t_person sel_p
                                    on sel_p.person_id = sel_u.usr_person_id
                                  left join t_mis_pays mp
                                    on mp.ab_id = ab.ab_id
                                   and mp.date_end - mp.date_begin < 32
                                 where ((nvl(pi_type_date, 0) = 0 and
                                       rp.dates between pi_period_beg and
                                       pi_period_end) or
                                       (nvl(pi_type_date, 0) = 1 and
                                       trunc(rp.dates) = trunc(pi_date) /*= trunc(pi_date)*/ /*between pi_period_beg and
                                                                                                                                                                                                     pi_period_end*/
                                       ))
                                       and rp.id_org in (select * from o_tab)
                                      /*and \*((nvl(pi_type_date, 0) = 0 and
                                                                            tmp_m.dates between pi_period_beg and
                                                                            pi_period_end) or*\
                                      (nvl(pi_type_date, 0) = 0 or
                                      trunc(tmp_m.dates) \*= trunc(pi_date)*\between pi_period_beg and
                                      pi_period_end)*/
                                   and ab.ab_status in (104, 105)
                                   and ab.is_deleted = 0
                                   and rp.is_actual = 1) tab
                         group by AB_ID,
                                  ROOT_ORG_ID,
                                  ROOT_ORG_NAME,
                                  ORG_ID,
                                  ORG_NAME,
                                  CALLSIGN,
                                  IMSI,
                                  DATE_ASR,
                                  AMOUNT_PAY,
                                  tar_id,
                                  tar_name,
                                  person,
                                  abonent,
                                  seller,
                                  seller_emp_num,
                                  data_activated,
                                  date_act,
                                  ab_cost,
                                  ab_paid /*,
                                                                                                                                                                                    dates*/
                        )
                 order by decode(l_order_asc,
                                 null,
                                 null,
                                 3,
                                 ROOT_ORG_NAME,
                                 5,
                                 ORG_NAME,
                                 6,
                                 CALLSIGN,
                                 7,
                                 IMSI,
                                 12,
                                 tar_name,
                                 15,
                                 seller,
                                 null) asc,
                          decode(l_order_asc,
                                 9,
                                 AMOUNT_PAY,
                                 10,
                                 amount_place,
                                 19,
                                 ab_paid,
                                 null) asc,
                          decode(l_order_asc,
                                 8,
                                 to_date(DATE_ASR, 'dd.mm.yyyy'),
                                 null) asc,
                          decode(l_order_desc,
                                 null,
                                 null,
                                 3,
                                 ROOT_ORG_NAME,
                                 5,
                                 ORG_NAME,
                                 6,
                                 CALLSIGN,
                                 7,
                                 IMSI,
                                 12,
                                 tar_name,
                                 15,
                                 seller,
                                 null) desc,
                          decode(l_order_desc,
                                 9,
                                 AMOUNT_PAY,
                                 10,
                                 amount_place,
                                 19,
                                 ab_paid,
                                 null) desc,
                          decode(l_order_desc,
                                 8,
                                 to_date(DATE_ASR, 'dd.mm.yyyy'),
                                 null) desc,
                          ab_id);

        po_all_count := l_col_list.count;

        l_max_num_page := round(po_all_count / nvl(pi_count_req, 1));

        if (pi_num_page > l_max_num_page and l_max_num_page <> 0) then
          l_num_page := l_max_num_page + 1;
        else
          l_num_page := nvl(pi_num_page, 1);
        end if;

        if pi_type_date = 1 then
          /*здесь нужно придумать правильный вывод количества строк*/
          /* select sum(count_act) into l_count_act from table(l_col_count);
          po_all_count := l_count_act;*/

          po_all_count := po_all_count /
                          (months_between(pi_period_end, pi_period_beg) + 1);
          select sum(count_act) into po_all_count from table(l_col_count);

          open po_full_rep for
            select *
              from table(l_col_list)
             where rn between
                   (l_num_page - 1) * nvl(pi_count_req, po_all_count) *
                   (months_between(pi_period_end, pi_period_beg) + 1) + 1 and
                   (l_num_page) * nvl(pi_count_req, po_all_count) *
                   (months_between(pi_period_end, pi_period_beg) + 1)
             order by rn;

          /*po_all_count := po_all_count /
          (months_between(pi_period_end, pi_period_beg) + 1);*/
        else
          open po_full_rep for
            select *
              from table(l_col_list)
             where rn between
                   (l_num_page - 1) * nvl(pi_count_req, po_all_count) + 1 and
                   (l_num_page) * nvl(pi_count_req, po_all_count)
             order by rn;
          --     po_all_count:=po_all_count/(months_between(pi_period_end, pi_period_beg) + 1);
        end if;

        if (pi_is_full = 1) then
          -- для полной версии сформируем также суммарную строку
          open po_summ for
            select DECODE(nvl(pi_type_date, 0),
                          0,
                          trunc(pi_date, 'MM'),
                          trunc(date_act, 'MM')) date_m,
                   sum(amount_pay) as amount_pay,
                   sum(amount_place) as amount_place,
                   sum(ab_paid) as ab_paid
              from table(l_col_list)
             group by DECODE(nvl(pi_type_date, 0),
                             0,
                             trunc(pi_date, 'MM'),
                             trunc(date_act, 'MM'));
        end if;
      end if;
    end if;
    return res;
  exception
    when others then
      po_err_num  := SQLCODE;
      po_err_msg  := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      po_full_rep := null;
      return null;
  end Abonent_Activity;
  ----------------------------------------------------------------------------
  -- статистика по абоненсткой активности ADSL-подключений
  function AbonentActivityADSL(pi_org_id     in T_ORGANIZATIONS.ORG_ID%type,
                               pi_is_all_org in number, --1 - по всем орг-циям
                               pi_date_0     in date,
                               pi_date_1     in date,
                               pi_worker_id  in T_USERS.USR_ID%type,
                               pi_is_full    in number,
                               po_err_num    out pls_integer,
                               po_err_msg    out t_Err_Msg,
                               po_full_rep   out sys_refcursor)
    return sys_refcursor is
    res          sys_refcursor;
    rights_value number;
    l_date_beg   number;
  begin
    if (pi_is_full = 1) then
      rights_value := 'EISSD.TMC_REPORTS.ABONENT_ACTIVITY_FULL';
    else
      rights_value := 'EISSD.REPORT.ABONENT_ACTIVITY_SHORT';
    end if;
    if (not Security_pkg.Check_Rights_str(rights_value,
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg,
                                          true)) then
      po_full_rep := null;
      return null;
    end if;
    -- месяц активности
    l_date_beg := to_char(pi_date_0, 'yyyymm');
    -- статистика по активности абонентов
    open res for
      select to_char(trunc(r.date_create, 'month'), 'YYYY-Month') "month",
             count(*) "all",
             sum(case
                   when trp.premia_schema = 12 and bda.acc_money_spravka >= 100 or
                        trp.premia_schema <> 12 and bda.acc_money_spravka >= 30 then
                    1
                   else
                    0
                 end) active
        from tr_request r
        join tr_request_service s
          on s.request_id = r.id
        left join t_bm_data_adsl bda
          on bda.account_id = r.personal_account
         and bda.period = l_date_beg /*дата в формате 200909, 200910 и т.д.*/
        join t_report_real_conn rrc
          on rrc.id_conn = s.id
         and rrc.ci = 2 -- ADSL
        join t_report_period trp
          on trp.id = rrc.id_repper
         and trp.is_actual = 1
       where ((nvl(pi_is_all_org, 0) = 0 and r.org_id = pi_org_id) or
             (nvl(pi_is_all_org, 0) = 1 and
             (r.org_id in
             (select tor.org_id
                   from t_org_relations tor
                 connect by prior tor.org_id = tor.org_pid
                  start with tor.org_id = pi_org_id))))
         and (trunc(r.date_create, 'month') <= trunc(pi_date_0, 'month') and
             trunc(r.date_create, 'month') >
             add_months(trunc(pi_date_0, 'month'), -5) and
             pi_date_1 is null or
             trunc(r.date_create, 'month') = trunc(pi_date_1, 'month'))
       group by trunc(r.date_create, 'month')
       order by trunc(r.date_create, 'month') asc;
    -- подробная информация по активности абонентов
    if (not Security_pkg.Check_Rights_str(rights_value,
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      po_full_rep := null;
    else
      open po_full_rep for
        select r.client_id /*du.id_client*/ ab_id,
               null root_org_id,
               null root_org_name,
               r.org_id /*du.id_org*/ org_id,
               org.org_name org_name,
               sd.usl_number CALLSIGN, --НШД
               sd.main_equipment IMSI,
               trunc(r.date_create) DATE_ASR,
               1 ACTIVE,
               rrc.id_repper RP_ID,
               nvl(bda.acc_money_spravka, 0) AMOUNT_PAY,
               nvl(sum_pay.sum_amount_pay, 0) SUM_AMOUNT_PAY,
               pr.tar_id TAR_ID,
               nvl(tar.title, tar_mrf.title) TAR_NAME,
               nvl(bda.amnt, 0) AMOUNT_PLACE,
               trim(tp.person_lastname) || ' ' || trim(tp.person_firstname) || ' ' ||
               trim(tp.person_middlename) person,
               trim(pa.person_lastname) || ' ' || trim(pa.person_firstname) || ' ' ||
               trim(pa.person_middlename) abonent
          from tr_request_service rs
          join tr_request r
            on r.id = rs.id
          join tr_service_product sp
            on sp.service_id = rs.id
          join tr_request_product pr
            on pr.id = sp.product_id
           and pr.type_product = 1
          left join tr_request_service_detail sd
            on sd.service_id = rs.id
          left join t_clients ca
            on ca.client_id = r.client_id
          left join t_person pa
            on pa.person_id = ca.fullinfo_id
          left join CONTRACT_T_TARIFF_VERSION tar
            on tar.id = pr.tar_id
           and pr.type_tariff = 1
          left join t_mrf_tariff tar_mrf
            on tar_mrf.id = pr.tar_id
           and pr.type_tariff = 3
          left join t_bm_data_adsl bda
            on bda.account_id = r.personal_account
           and bda.period = l_date_beg /*дата в формате 200909, 200910 и т.д.*/
          join t_report_real_conn rrc
            on rrc.id_conn = rs.id
           and rrc.ci = 2 --ADSL
          join t_report_period trp
            on trp.id = rrc.id_repper
           and trp.is_actual = 1
          join t_organizations org
            on org.org_id = r.org_id
          LEFT join (select bd.login,
                            bd.account_id,
                            sum(NVL(bd.acc_money_spravka, 0)) sum_amount_pay
                       from t_bm_data_adsl bd
                      where To_Date(bd.period, 'YYYYMM') between
                            Trunc(pi_date_1, 'mm') and
                            To_Date(l_date_beg, 'YYYYMM')
                      group by bd.login) sum_pay
            on sum_pay.account_id = r.personal_account
          join t_users tu
            on tu.usr_id = r.worker_create
          join t_person tp
            on tp.person_id = tu.usr_person_id
         where ((nvl(pi_is_all_org, 0) = 0 and r.org_id = pi_org_id) or
               (nvl(pi_is_all_org, 0) = 1 and
               (r.org_id in
               (select tor.org_id
                     from t_org_relations tor
                   connect by prior tor.org_id = tor.org_pid
                    start with tor.org_id = pi_org_id))))
           and (trunc(r.date_create, 'month') <= trunc(pi_date_0, 'month') and
               trunc(r.date_create, 'month') >
               add_months(trunc(pi_date_0, 'month'), -5) and
               pi_date_1 is null or
               trunc(r.date_create, 'month') = trunc(pi_date_1, 'month'))
         order by trunc(r.date_create, 'month') asc;
    end if;
    return res;
  exception
    when others then
      po_err_num  := SQLCODE;
      po_err_msg  := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      po_full_rep := null;
      return null;
  end AbonentActivityADSL;
  ----------------------------------------------------------------------------
  /** ФУНКЦИЯ ВОЗВРАЩАЕТ СУММАРНОЕ ВОЗНАГРАЖДЕНИЯ АГЕНТОВ ПО ОРГАНИЗАЦИЯМ ЗА ОПРЕДЕЛЁННЫЙ
  * МЕСЯЦ (ИЛИ ЗА ВСЕ). ВХОДНЫЕ ПАРАМЕТРЫ - КОД КУРИРУЮЩЕЙ ОРГАНИЗАЦИИ, ДАТА (М.Б. NULL)*/
  function GetSumAgentNagr(pi_root_id in t_organizations.root_org_id%type,
                           pi_date    in t_report_stat.d_month%type,
                           po_err_num out pls_integer,
                           po_err_msg out varchar2) return sys_refcursor is
    res      sys_refcursor;
    new_date t_report_stat.d_month%type;
  begin
    -- Приводим дату к первому числу текущего месяца --
    new_date := last_day(add_months(pi_date, -1)) + 1;
    if (pi_root_id > -1) then
      open res for
        select ORG1.org_name P_ORG_NAME,
               ORG2.Org_Name C_ORG_NAME,
               sf.date_s d_month, -- new
               sum(nvl(sf.summa, 0)) NAGR, -- new !!! Берём суммму БЕЗ НДС
               null KOL -- new
          from t_organizations ORG1,
               t_organizations ORG2,
               t_report_sf     SF,
               t_org_relations REL
         where org1.org_id = pi_root_id -- условие для 1 столбца: выбираем наименов. курирующ. организации по её коду --
           and sf.id_org_sel = REL.org_id -- new
           and rel.org_pid = pi_root_id
           and rel.org_reltype in (1004, 999)
           and sf.id_org_sel = org2.org_id -- new
           and (new_date is null or (new_date is not null and date_s /*d_month*/
               = new_date))
         group by ORG1.ORG_NAME, ORG2.ORG_NAME, date_s
         order by P_ORG_NAME, C_ORG_NAME, D_MONTH;
    else
      open res for
        select ORG1.org_name P_ORG_NAME,
               ORG2.Org_Name C_ORG_NAME,
               sf.date_s d_month, --new
               sum(nvl(sf.summa, 0)) NAGR, --new!!! Берём суммму БЕЗ НДС
               null KOL -- new
          from t_organizations ORG1,
               t_organizations ORG2,
               t_report_sf     SF, -- new
               t_org_relations REL
         where sf.id_org_sel = REL.org_id --new
           and org1.org_id = rel.org_pid
           and rel.org_reltype in (1004, 999)
           and sf.id_org_sel = ORG2.ORG_ID -- new
           and (new_date is null or
               (new_date is not null and date_s = new_date))
         group by org1.org_name, ORG2.ORG_NAME, date_s
        union
        select 'Нет' P_ORG_NAME,
               ORG2.Org_Name C_ORG_NAME,
               sf.date_s D_MONTH, --new
               sum(nvl(sf.summa, 0)) NAGR, -- new !!! Берём суммму БЕЗ НДС
               null KOL --new
          from t_organizations ORG2,
               t_report_sf     sf, --new
               t_org_relations REL
         where sf.id_org_sel = org2.org_id --new
           and sf.id_org_sel = rel.org_id -- new
           and REL.org_pid = -1
           and rel.org_reltype in (1004, 999)
           and (new_date is null or
               (new_date is not null and date_s = new_date))
         group by ORG2.Org_Name, date_s
         order by 1, 2, 3;
    end if;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end GetSumAgentNagr;
  ----------------------------------------------------------------------------
  -- Добавление корректировки
  procedure Correction_Add(pi_org_id    in t_organizations.org_id%type,
                           pi_dog_id    in number, -- 14.01.10
                           pi_date      in date,
                           pi_summ      in t_corrections.summ%type,
                           pi_reason    in t_corrections.reason%type,
                           pi_corr_type in t_corrections.corr_type%type,
                           pi_worker_id in T_USERS.USR_ID%type,
                           po_c_id      out t_corrections.c_id%type,
                           po_err_num   out pls_integer,
                           po_err_msg   out t_Err_Msg,
                           pi_perm      in number) is
    l_perm   number;
    l_dog_id number;
    ex_no_perm exception;
  begin
    savepoint sp_Correction_Add;
    /*вставить проверку прав доступа пользователя!!!*/
    -- Перекодировка для web'a
    case pi_perm
      when 5000 then
        l_perm := 2000;
      when 5001 then
        l_perm := 4000;
      when 5002 then
        l_perm := 2001;
      when 5003 then
        l_perm := 2003;
      when 5004 then
        l_perm := 2007;
      when 5005 then
        l_perm := 2008;
      when 5006 then
        l_perm := 2007;
      when 5007 then
        l_perm := 2008;
      when 5008 then
        l_perm := 2009;
      else
        l_perm := pi_perm;
        --- raise ex_no_perm;
    end case;

    if pi_dog_id is null then
      select dog.dog_id
        into l_dog_id
        from t_dogovor dog, t_org_relations rel
       where rel.org_id = pi_org_id
         and rel.id = dog.org_rel_id
         and rel.org_reltype != 1009
         and rownum <= 1;
    else
      l_dog_id := pi_dog_id;
    end if;
    insert into t_corrections t
      (c_date, org_id, summ, reason, dog_id, perm, corr_type)
    values
      (pi_date,
       pi_org_id,
       pi_summ,
       pi_reason,
       l_dog_id,
       l_perm,
       nvl(pi_corr_type, 1))
    returning c_id into po_c_id;
  exception
    when ex_no_perm then
      po_err_msg := 'Не выбрано направление использования.';
      po_err_num := 1;
      rollback to sp_Correction_Add;
    when NO_DATA_FOUND then
      po_err_msg := 'У данной организации договоров не найдено!';
      po_c_id    := 0;
      rollback to sp_Correction_Add;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      rollback to sp_Correction_Add;
  end Correction_Add;
  ----------------------------------------------------------------------------
  -- Изменение корректировки
  Procedure Correction_Change(pi_c_id      in t_corrections.c_id%type,
                              pi_dog_id    in number,
                              pi_org_id    in t_organizations.org_id%type,
                              pi_date      in date,
                              pi_summ      in t_corrections.summ%type,
                              pi_reason    in t_corrections.reason%type,
                              pi_corr_type in t_corrections.corr_type%type,
                              pi_worker_id in T_USERS.USR_ID%type,
                              po_err_num   out pls_integer,
                              po_err_msg   out t_Err_Msg,
                              pi_perm      in number) is
    num      number;
    l_perm   number;
    l_dog_id number;
  begin
    savepoint sp_Correction_Change;
    /*вставить проверку прав доступа пользователя!!!*/
    case pi_perm
      when 5000 then
        l_perm := 2000;
      when 5001 then
        l_perm := 4000;
      when 5002 then
        l_perm := 2001;
      when 5003 then
        l_perm := 2003;
      when 5004 then
        l_perm := 2007;
      when 5005 then
        l_perm := 2008;
      when 5006 then
        l_perm := 2007;
      when 5007 then
        l_perm := 2008;
      when 5008 then
        l_perm := 2009;
      else
        l_perm := pi_perm;
        --- raise ex_no_perm;
    end case;

    if pi_dog_id is null then
      select dog.dog_id
        into l_dog_id
        from t_dogovor dog, t_org_relations rel
       where rel.org_id = pi_org_id
         and rel.id = dog.org_rel_id
         and rownum <= 1;
    else
      l_dog_id := pi_dog_id;
    end if;

    select count(*) into num from t_corrections t where t.c_id = pi_c_id;
    if num > 0 then
      update t_corrections t
         set c_date      = pi_date,
             org_id      = pi_org_id,
             summ        = pi_summ,
             reason      = pi_reason,
             dog_id      = l_dog_id,
             perm        = l_perm,
             t.corr_type = pi_corr_type
       where c_id = pi_c_id;
    else
      report_period.Correction_Add(pi_org_id,
                                   pi_dog_id,
                                   pi_date,
                                   pi_summ,
                                   pi_reason,
                                   pi_corr_type,
                                   pi_worker_id,
                                   num,
                                   po_err_num,
                                   po_err_msg,
                                   pi_perm);
    end if;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      rollback to sp_Correction_Change;
  end Correction_Change;
  ----------------------------------------------------------------------------
  -- Поиск корректировки по организации и месяцу
  function Correction_Find(pi_org_id    in t_organizations.org_id%type,
                           pi_date      in date,
                           pi_worker_id in T_USERS.USR_ID%type,
                           pi_dog_id    in number,
                           pi_perm      in number,
                           po_err_num   out pls_integer,
                           po_err_msg   out t_Err_Msg) return sys_refcursor is
    c_pr_name constant varchar2(65) := c_package || 'Correction_Find';
    res          sys_refcursor;
    l_NDS        number;
    l_dog_number varchar2(32);
    l_perm       number;
    /****************************************/
    function getStrParam return varchar2 is
    begin
      return 'pi_org_id=' || pi_org_id || ';' || 'pi_date=' || pi_date || ';' || 'pi_dog_id=' || pi_dog_id || ';' || 'pi_perm=' || pi_perm || ';' || 'pi_worker_id=' || pi_worker_id;
    end getStrParam;
    /****************************************/
  begin
    savepoint sp_Correction_Find;
    logging_pkg.debug(getStrParam, c_pr_name);
    if pi_dog_id is not null then
      select td.dog_number
        into l_dog_number
        from t_dogovor td
       where td.dog_id = pi_dog_id;
    end if;
    case pi_perm
      when 5000 then
        l_perm := 2000;
      when 5002 then
        l_perm := 2001;
      when 5003 then
        l_perm := 2003;
      when 5001 then
        l_perm := 4000;
      when 5006 then
        l_perm := 2007;
      when 5007 then
        l_perm := 2008;
      else
        l_perm := pi_perm;
    end case;

    Begin
      l_NDS := 0;
      Select max(rp.pr_nds)
        into l_NDS
        from t_Report_Period rp
       Where rp.id_org = pi_org_id
         and rp.is_actual = 1
         and pi_date between rp.dates and rp.datepo
         and (rp.dog_id = pi_dog_id or pi_dog_id is null);
    Exception
      when Others then
        l_NDS := 0;
    End;
    /*вставить проверку прав доступа пользователя!!!*/
    open res for
      select corr.c_id,
             corr.c_date,
             org.org_id,
             org.org_name,
             corr.summ,
             Round(nvl(corr.summ / (1 + nvl(l_NDS, 0)), 0), 2) summ_bezNDS,
             corr.reason,
             td.dog_number,
             decode(corr.perm,
                    2000,
                    5000,
                    2001,
                    5002,
                    4000,
                    5001,
                    2003,
                    5003,
                    2007,
                    5006,
                    2008,
                    5007,
                    corr.perm) perm,
             corr.corr_type
        from t_organizations org, t_corrections corr
        join t_dogovor td
          on td.dog_id = corr.dog_id
       where corr.org_id = org.org_id
         and ((pi_org_id is null) or
             (pi_org_id is not null and corr.org_id = pi_org_id))
         and ((pi_date is null) or
             (pi_date is not null and
             Trunc(corr.c_date, 'mm') = Trunc(pi_date)))
         and (corr.dog_id = pi_dog_id or pi_dog_id is null)
         and (corr.perm = l_perm or l_perm is null)
       order by 1, 2;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      rollback to sp_Correction_Find;
      return null;
  end Correction_Find;
  ----------------------------------------------------------------------------
  -- Поиск корректировки (по ID)
  function Correction_ID_Get(pi_c_id      in t_organizations.org_id%type,
                             pi_worker_id in T_USERS.USR_ID%type,
                             po_err_num   out pls_integer,
                             po_err_msg   out t_Err_Msg) return sys_refcursor is
    res sys_refcursor;
  begin
    /*вставить проверку прав доступа пользователя!!!*/
    open res for
      select corr.c_id,
             corr.c_date,
             org.org_id,
             org.org_name,
             corr.summ,
             corr.reason,
             corr.dog_id,
             (case
               when corr.perm = 2000 then
                5000
               when corr.perm = 2001 then
                5002
               when corr.perm = 4000 then
                5001
               when corr.perm in (2004, 2006) then
                5002
               when corr.perm in (2007) then
                5006
               when corr.perm in (2008) then
                5007
               else
                corr.perm
             end) perm,
             corr.corr_type
        from t_organizations org, t_corrections corr
        join t_dogovor td
          on td.dog_id = corr.dog_id
       where corr.org_id = org.org_id
         and corr.c_id = pi_c_id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Correction_ID_Get;
  ----------------------------------------------------------------------------
  -- Удаление корректировки (по ID)
  procedure Correction_Delete(pi_c_id      in t_organizations.org_id%type,
                              pi_worker_id in T_USERS.USR_ID%type,
                              po_err_num   out pls_integer,
                              po_err_msg   out t_Err_Msg) is
  begin
    savepoint sp_Correction_Delete;
    /*вставить проверку прав доступа пользователя!!!*/
    delete from t_corrections t where t.c_id = pi_c_id;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      rollback to sp_Correction_Delete;
  end Correction_Delete;
  ----------------------------------------------------------------------------------
  -- Ищет вознаграждения по общей схеме по дереву курирующих организаций
  function Get_Obsh_Premia_Recur(pi_org_id in T_ORGANIZATIONS.ORG_ID%type,
                                 pi_tar_id in number) return number is
    l_value  number;
    l_org_id T_ORGANIZATIONS.ORG_ID%type;
  begin
    begin
      select Max(rel.org_pid)
        into l_org_id
        from V_Org_Tree rel
       where rel.org_id = pi_org_id;
    exception
      when no_data_found then
        null;
    end;
    if (l_org_id is null or l_org_id = -1) then
      return null;
    end if;
    select max(nvl(pp.value, 0))
      into l_value
      from t_premia_point pp, t_premia_schema ps
     where ps.id_org = l_org_id
       and ps.id = pp.id_schema
       and ps.ci = 2
       and ((ps.id_tariff = pi_tar_id and pi_tar_id is Not Null) or
           pi_tar_id is Null);
    if (l_value is null) then
      return Get_Obsh_Premia_Recur(l_org_id, pi_tar_id);
    else
      return l_value;
    end if;
  end Get_Obsh_Premia_Recur;
  ----------------------------------------------------------------
  function Get_ADSL_Connections(Pi_rp_id     in number,
                                pi_worker_id in T_USERS.USR_ID%type,
                                po_err_num   out pls_integer,
                                po_err_msg   out t_Err_Msg)
    return sys_refcursor is
    res sys_refcursor;
  begin
    -- находим отчетный период
    open res for
      select distinct rs.id abonent_id,
                      org.org_id org_id,
                      concat(concat(concat(concat(p.person_lastname, ' '),
                                           p.person_firstname),
                                    ' '),
                             p.person_middlename) FIO,
                      ad.addr_city,
                      ad.addr_street,
                      ad.addr_index,
                      ad.addr_building,
                      ad.addr_office addr_flat,
                      sd.usl_number usl_number,
                      /*tcc.dogovor_date*/
                      /*cc.contract_date*/rs.date_change_state - 2 / 24 dog_date,
                      cc.contract_num dog_num,
                      nvl(tar.title, tar_mrf.title) tar_name,
                      (select (listagg(nvl(tar_c.title, mrf_opt.name_option),
                                       ', ') WITHIN
                               GROUP(order by
                                     nvl(tar_c.title, mrf_opt.name_option)))
                         from tr_product_option r_op
                         left join contract_t_tariff_option tar_opt
                           on tar_opt.id = r_op.option_id
                             --and rp.type_tariff = 1
                          and tar_opt.alt_name not in
                              ('discount_doubleplay', 'discount_tripleplay')
                         left join contract_t_tariff_option_cost tar_c
                           on tar_c.option_id = tar_opt.id
                         left join t_mrf_tariff_option mrf_opt
                           on mrf_opt.id = r_op.option_id
                        where r_op.service_id = rs.id
                          and (tar_opt.id is null or rp.type_tariff = 1)
                          and (tar_c.id is null or
                              (nvl(tar_c.tech_id, nvl(rs.tech_id, -1)) =
                              nvl(rs.tech_id, -1) and rp.type_tariff = 1 and
                              rp.type_tariff = 1 and
                              tar_c.tar_ver_id = rp.tar_id))
                          and (mrf_opt.id is null or rp.type_tariff = 2)) option_name,
                      0 nagr_obsh,
                      0 nagr_dil,
                      0 with_nds,
                      TRPR.PRICE_BASE_COST as once_cost,
                      TRPR.PRICE_BASE_FEE as advance_cost,
                      0 as status,
                      org.is_pay_espp,
                      orgp.org_name as fes,
                      pW.Person_Id worker_id,
                      pW.Person_Lastname || ' ' || pW.Person_Firstname || ' ' ||
                      pW.Person_Middlename as worker_name,
                      org.org_name,
                      os.cost AB_PAID_ESPO,
                      0 AB_PAID_ESPP,
                      rs.product_category category_id,
                      (case
                        when mrf.name_mrf is null then
                         orgR.Org_Name
                        else
                         'МРФ ' || mrf.name_mrf
                      end) ROOT_ORG_NAME,
                      adr.addr_city addr_city_install,
                      adr.addr_street addr_street_install,
                      adr.addr_index addr_index_install,
                      adr.addr_building addr_building_install,
                      adr.addr_office addr_flat_install,
                      rs.tech_id
        from t_report_real_conn rrc
        join t_report_period trp
          on trp.id = rrc.id_repper
        join tr_request_service rs
          on rs.id = rrc.id_conn
        join tr_request r
          on r.id = rs.request_id
        join tr_service_product sp
          on sp.service_id = rs.id
        join tr_request_product rp
          on rp.id = sp.product_id
         and rp.type_product = 1
        left join tr_request_service_detail sd
          on sd.service_id = rs.id
        left join tr_contract_request cr
          on cr.request_id = r.id
        left join tr_contract cc
          on cc.id = cr.contract_id
        left join tr_product_onetime_service os
          on os.service_id = rrc.id_conn
        left join CONTRACT_T_TARIFF_VERSION tar
          on tar.id = rp.tar_id
         and rp.type_tariff = 1
        left join t_mrf_tariff tar_mrf
          on tar_mrf.id = rp.tar_id
         and rp.type_tariff = 3
        LEFT JOIN TR_REQUEST_PRICE TRPR ON TRPR.SERVICE_ID = RS.ID AND TRPR.IS_ACTUAL = 1
        left join t_organizations org
          on org.org_id = r.org_id
        left join t_organizations orgP
          on orgP.Org_Id = Get_FES_Bi_Id_Region(org.region_id)
        join t_clients c
          on r.client_id = c.client_id
        join t_person p
          on c.fullinfo_id = p.person_id
        join t_documents d
          on p.doc_id = d.doc_id
        left join t_address ad
          on p.addr_id_reg = ad.addr_id
        join t_report_period rp
          on rrc.id_repper = rp.id
         and rp.is_actual = 1
        left join t_users us
          on us.usr_id = rs.worker_create
        left join t_person Pw
          on Pw.Person_Id = us.usr_person_id
        left join mv_org_tree rel
          on rel.org_id = r.org_id -- tcc.org_id
         and rel.org_pid <> -1
         and rel.root_org_pid <> -1
         and rel.org_reltype not in (1005, 1006)
        left join mv_org_tree rel1
          on rel1.org_id = trp.id_org
         and rel1.org_reltype in (1004, 999)
        left join t_organizations orgR
          on orgR.Org_Id = nvl(rel.root_org_id, rel1.org_id)
        left join t_dic_region reg1
          on reg1.reg_id = org.region_id
        left join t_dic_mrf mrf
          on mrf.id = reg1.mrf_id
         and rel.root_org_id in (0, 1, 2)
        left join t_address adr
          on adr.addr_id = r.address_id
       where rrc.id_repper = Pi_rp_id
         and rrc.ci = 2
       order by category_id, abonent_id; -- только ADSL
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
      return res;
  end Get_ADSL_Connections;
  --------------------------------------------------------
  function GetReportPremiaMoney_Test(pi_rp_id     in number,
                                     pi_worker_id in T_USERS.USR_ID%type,
                                     pi_flag_corr in number,
                                     po_err_num   out pls_integer,
                                     po_err_msg   out varChar2,
                                     pi_id_org    in t_organizations.org_id%type := null,
                                     pi_month     in date := null)
    return sys_refcursor is
    res              sys_refcursor;
    l_rp_id          Number;
    l_month          date;
    l_id_org         number;
    l_SumWNDS_adsl   Number := 0; -- new
    l_SumWNDS_ab_ser Number := 0; -- new
    l_SumWNDS_SKP    Number := 0; -- new
    l_SumWNDS_ott    Number := 0; -- new
    --
    l_SumWNDS_SKP_loyalty Number := 0;
    l_kol_SKP_loyalty     Number := 0;
    l_Sum_adsl            Number := 0; -- new
    ---
    l_SumWNDS Number := 0;
    l_Sum     Number := 0;
    l_kol     Number := 0;
    --дист. продажи
    l_SumWNDS_RemS Number := 0;
    l_Sum_RemS     Number := 0;
    l_kol_RemS     Number := 0;
    --доставка
    l_SumWNDS_deliv1 Number := 0;
    l_Sum_deliv1     Number := 0;
    l_SumWNDS_deliv2 Number := 0;
    l_Sum_deliv2     Number := 0;
    --
    l_kol_ADSL         Number := 0;
    l_kol_ab_ser       Number := 0;
    l_kol_SKP          Number := 0;
    l_kol_ott          Number := 0;
    l_Pr_NDS           Number;
    l_with_corr_gsm    Number;
    l_with_corr_adsl   Number;
    l_with_corr_ab_ser Number;
    l_penalty_gsm      Number;
    l_penalty_adsl     number;
    l_with_corr_ott    number;
    l_penalty_ott      Number;
    --
    l_dog_id   number;
    Po_Formula Varchar2(200);
  begin
    if (not Security_pkg.Check_User_Right_str('EISSD.REPORT.PERIOD.VIEW',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;

    l_month := trunc(pi_month, 'MONTH');
    if (pi_rp_id is not null) then
      l_rp_id := pi_rp_id;
      begin
        select rp.dates, rp.id_org, rp.pr_nds
          into l_month, l_id_org, l_Pr_NDS
          from t_report_period rp
         where rp.id = l_rp_id
           and rp.is_actual = 1;
      exception
        when others then
          null;
      end;
    else
      begin
        select rp.id, nvl(rp.pr_nds, 0)
          into l_rp_id, l_Pr_NDS
          from t_report_period rp
         where rp.id_org = pi_id_org
           and rp.is_actual = 1
           and trunc(rp.dates, 'MONTH') = trunc(pi_month, 'MONTH');
      exception
        when others then
          null;
      end;
    end if;

    select td.dog_id
      into l_dog_id
      from t_report_period trp
      join t_dogovor td
        on td.dog_id = trp.dog_id
     where trp.id = pi_rp_id
       and trp.is_actual = 1;
    --корректировки
    select sum(tc.summ) * NVL(pi_flag_corr, 0)
      into l_with_corr_gsm
      from t_corrections tc
     where tc.c_date = l_month
       and tc.perm = 2000
       and tc.dog_id = l_dog_id
       and tc.corr_type = 1;
    l_with_corr_gsm := NVL(l_with_corr_gsm, 0);

    select sum(tc.summ) * NVL(pi_flag_corr, 0)
      into l_with_corr_adsl
      from t_corrections tc
     where tc.c_date = l_month
       and tc.perm = 2001
       and tc.dog_id = l_dog_id
       and tc.corr_type = 1;
    l_with_corr_adsl := NVL(l_with_corr_adsl, 0);

    select sum(tc.summ) * NVL(pi_flag_corr, 0)
      into l_with_corr_ab_ser
      from t_corrections tc
     where tc.c_date = l_month
       and tc.perm = 4000
       and tc.dog_id = l_dog_id
    /*and tc.corr_type = 1*/
    ;
    l_with_corr_ab_ser := NVL(l_with_corr_ab_ser, 0);

    select sum(tc.summ) * NVL(pi_flag_corr, 0)
      into l_penalty_gsm
      from t_corrections tc
     where tc.c_date = l_month
       and tc.dog_id = l_dog_id
       and tc.corr_type = 2
       and tc.perm = 2000;
    l_penalty_gsm := NVL(l_penalty_gsm, 0);

    select sum(tc.summ) * NVL(pi_flag_corr, 0)
      into l_penalty_adsl
      from t_corrections tc
     where tc.c_date = l_month
       and tc.dog_id = l_dog_id
       and tc.corr_type = 2
       and tc.perm = 2001;
    l_penalty_adsl := NVL(l_penalty_adsl, 0);

    --
    select sum(tc.summ) * NVL(pi_flag_corr, 0)
      into l_with_corr_ott
      from t_corrections tc
     where tc.c_date = l_month
       and tc.perm in (2012, 2011, 5014)
       and tc.dog_id = l_dog_id
       and tc.corr_type = 1;
    l_with_corr_ott := NVL(l_with_corr_ott, 0);

    select sum(tc.summ) * NVL(pi_flag_corr, 0)
      into l_penalty_ott
      from t_corrections tc
     where tc.c_date = l_month
       and tc.dog_id = l_dog_id
       and tc.corr_type = 2
       and tc.perm in (2012, 2011, 5014);
    l_penalty_ott := NVL(l_penalty_ott, 0);

    res := GetAgentPoruchenie_Test(l_rp_id,
                                   pi_worker_id,
                                   4,
                                   Po_Formula,
                                   po_err_num,
                                   po_err_msg);
    for orarec in (select ap.dates,
                          sum(ap.sumwnds) as SumWNDS,
                          sum(ap.kol) as kol
                     from t_report_ap ap
                    where ap.rp_id = l_rp_id
                      and ap.type_schema = 8
                    group by ap.dates) loop
      If orarec.dates = l_month then
        l_SumWNDS_ab_ser := l_SumWNDS_ab_ser +
                            round(NVL(orarec.SumWNDS, 0), 2);
        l_kol_ab_ser     := l_kol_ab_ser + NVL(orarec.kol, 0);
      Else
        l_SumWNDS_ab_ser := l_SumWNDS_ab_ser +
                            round(NVL(orarec.SumWNDS, 0), 2);
      End If;
    end loop;
    --
    for orarec in (select ap.dates,
                          sum(ap.sumwnds) as SumWNDS,
                          sum(ap.sum) as sum
                     from t_report_ap ap
                    where ap.rp_id = l_rp_id
                      and ap.type_schema = 16
                    group by ap.dates) loop
      l_SumWNDS_deliv1 := l_SumWNDS_deliv1 +
                          round(NVL(orarec.SumWNDS, 0), 2);
      l_Sum_deliv1     := l_Sum_deliv1 + round(NVL(orarec.sum, 0), 2);
    end loop;
    for orarec in (select ap.dates,
                          sum(ap.sumwnds) as SumWNDS,
                          sum(ap.sum) as sum
                     from t_report_ap ap
                    where ap.rp_id = l_rp_id
                      and ap.type_schema = 17
                    group by ap.dates) loop
      l_SumWNDS_deliv2 := l_SumWNDS_deliv2 +
                          round(NVL(orarec.SumWNDS, 0), 2);
      l_Sum_deliv2     := l_Sum_deliv2 + round(NVL(orarec.sum, 0), 2);
    end loop;
    for orarec in (select ap.seg_id,
                          sum(ap.sumwnds) as SumWNDS,
                          sum(ap.kol) as kol
                     from t_report_ap ap
                    where ap.rp_id = l_rp_id
                      and ap.type_schema = 14
                    group by ap.seg_id) loop
      if orarec.seg_id is null then
        l_SumWNDS_SKP := l_SumWNDS_SKP + round(NVL(orarec.SumWNDS, 0), 2);
        l_kol_SKP     := l_kol_SKP + NVL(orarec.kol, 0);
      else
        l_SumWNDS_SKP_loyalty := l_SumWNDS_SKP_loyalty +
                                 round(NVL(orarec.SumWNDS, 0), 2);
        l_kol_SKP_loyalty     := l_kol_SKP_loyalty + NVL(orarec.kol, 0);

      end if;
    end loop;
    for orarec in (select ap.seg_id,
                          sum(ap.sumwnds) as SumWNDS,
                          sum(ap.kol) as kol
                     from t_report_ap ap
                    where ap.rp_id = l_rp_id
                      and ap.type_schema = 15
                    group by ap.seg_id) loop
      l_SumWNDS_ott := l_SumWNDS_ott + round(NVL(orarec.SumWNDS, 0), 2);
      l_kol_ott     := l_kol_ott + NVL(orarec.kol, 0);
    end loop;

    for orarec in (select ap.dates,
                          sum(ap.sumwnds) as SumWNDS,
                          sum(ap.sum) as Summ,
                          sum(ap.kol) as kol
                     from t_report_ap ap
                    where ap.rp_id = l_rp_id
                      and ap.type_schema in (4, 12) --12-ознаграждение за монобренд-тоже решили запихать в жсм
                    group by ap.dates) loop
      If orarec.dates = l_month then
        l_SumWNDS := l_SumWNDS + round(NVL(orarec.SumWNDS, 0), 2);
        l_Sum     := l_Sum + round(NVL(orarec.Summ, 0), 2);
        l_kol     := l_kol + NVL(orarec.kol, 0);
      Else
        l_SumWNDS := l_SumWNDS + round(NVL(orarec.SumWNDS, 0), 2);
        l_Sum     := l_Sum + round(NVL(orarec.Summ, 0), 2);
      End If;
    end loop;
    for orarec in (select ap.dates,
                          sum(ap.sumwnds) as SumWNDS,
                          sum(ap.sum) as Summ,
                          sum(ap.kol) as kol
                     from t_report_ap ap
                    where ap.rp_id = l_rp_id
                      and ap.type_schema in (6, 13)
                    group by ap.dates) loop
      If orarec.dates = l_month then
        l_SumWNDS_adsl := NVL(l_SumWNDS_adsl, 0) +
                          round(NVL(orarec.SumWNDS, 0), 2);
        l_Sum_adsl     := NVL(l_Sum_adsl, 0) +
                          round(NVL(orarec.Summ, 0), 2);
        l_kol_Adsl     := NVL(l_kol_Adsl, 0) + NVL(orarec.kol, 0);
      Else
        l_SumWNDS_adsl := NVL(l_SumWNDS_adsl, 0) +
                          round(NVL(orarec.SumWNDS, 0), 2);
        l_Sum_adsl     := NVL(l_Sum_adsl, 0) +
                          round(NVL(orarec.Summ, 0), 2);
      End If;
    end loop;
    for orarec in (select ap.dates,
                          sum(ap.sumwnds) as SumWNDS,
                          sum(ap.sum) as Summ,
                          sum(ap.kol) as kol
                     from t_report_ap ap
                    where ap.rp_id = l_rp_id
                      and ap.type_schema = 10
                    group by ap.dates) loop
      If orarec.dates = l_month then
        l_SumWNDS_RemS := l_SumWNDS_RemS + round(NVL(orarec.SumWNDS, 0), 2);
        l_Sum_RemS     := l_Sum_RemS + round(NVL(orarec.Summ, 0), 2);
        l_kol_RemS     := l_kol_RemS + NVL(orarec.kol, 0);
      Else
        l_SumWNDS_RemS := l_SumWNDS_RemS + round(NVL(orarec.SumWNDS, 0), 2);
        l_Sum_RemS     := l_Sum_RemS + round(NVL(orarec.Summ, 0), 2);
      End If;
    end loop;

    Open res for
      select l_kol kol,
             round(l_Sum, 2) SUM_GSM,
             round(l_SumWNDS, 2) - round(l_Sum, 2) SUM_NDS_GSM,
             round(l_SumWNDS, 2) SUM_WITH_NDS_GSM,
             l_kol_Adsl kol_Adsl,
             round(l_Sum_adsl, 2) SUM_ADSL,
             round(l_SumWNDS_adsl, 2) - round(l_Sum_adsl, 2) SUM_NDS_ADSL,
             round(l_SumWNDS_adsl, 2) SUM_WITH_NDS_ADSL,
             -- Абонентское обслуживание 31260 14.06.2011
             l_kol_ab_ser kol_ab_ser,
             round(l_SumWNDS_ab_ser /
                   Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                   2) SUM_ab_ser,
             round(l_SumWNDS_ab_ser -
                   l_SumWNDS_ab_ser /
                   Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                   2) SUM_NDS_ab_ser,
             round(l_SumWNDS_ab_ser, 2) SUM_WITH_NDS_ab_ser,
             --Дист продажи
             l_kol_RemS kol_RemS,
             round(l_Sum_RemS, 2) SUM_RemS,
             round(l_SumWNDS_RemS, 2) - round(l_Sum_RemS, 2) SUM_NDS_RemS,
             round(l_SumWNDS_RemS, 2) SUM_WITH_NDS_RemS,
             --до 07.2013 корректировки забивались с учетом НДС,после-без НДС
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_with_corr_gsm /
                      Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                      2)
               else
                round(l_with_corr_gsm, 2)
             end) SUM_CORR_GSM, -- без НДС
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_with_corr_gsm -
                      l_with_corr_gsm /
                      Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                      2)
               else
                round(l_with_corr_gsm * (1 + l_Pr_NDS), 2) -
                round(l_with_corr_gsm, 2)
             end) SUM_NDS_CORR_GSM, --НДС
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_with_corr_gsm, 2)
               else
                round(l_with_corr_gsm * (1 + l_Pr_NDS), 2)
             end) SUM_WITH_NDS_CORR_GSM,
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_with_corr_adsl /
                      Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                      2)
               else
                l_with_corr_adsl
             end) SUM_CORR_ADSL, -- без НДС
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_with_corr_adsl -
                      l_with_corr_adsl /
                      Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                      2)
               else
                round(l_with_corr_adsl * (1 + l_Pr_NDS), 2) -
                l_with_corr_adsl
             end) SUM_NDS_CORR_ADSL, --НДС
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_with_corr_adsl, 2)
               else
                round(l_with_corr_adsl * (1 + l_Pr_NDS), 2)
             end) SUM_WITH_NDS_CORR_ADSL,
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_with_corr_ab_ser /
                      Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                      2)
               else
                l_with_corr_ab_ser
             end) SUM_CORR_AB_SER, -- без НДС
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_with_corr_ab_ser -
                      l_with_corr_ab_ser /
                      Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                      2)
               else
                round(l_with_corr_ab_ser * (1 + l_Pr_NDS), 2) -
                l_with_corr_ab_ser
             end) SUM_NDS_CORR_AB_SER, --НДС
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_with_corr_ab_ser, 2)
               else
                round(l_with_corr_ab_ser * (1 + l_Pr_NDS), 2)
             end) SUM_WITH_NDS_CORR_AB_SER,
             --штрафные санкции GSM
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_penalty_gsm /
                      Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                      2)
               else
                round(l_penalty_gsm, 2)
             end) SUM_CORR_PENALTY_gsm, -- без НДС
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_penalty_gsm -
                      l_penalty_gsm /
                      Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                      2)
               else
                round(l_penalty_gsm * (1 + l_Pr_NDS), 2) -
                round(l_penalty_gsm, 2)
             end) SUM_NDS_CORR_PENALTY_gsm, --НДС
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_penalty_gsm, 2)
               else
                round(l_penalty_gsm * (1 + l_Pr_NDS), 2)
             end) SUM_WITH_NDS_CORR_PENALTY_gsm,
             --штрафные санкции ADSL
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_penalty_adsl /
                      Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                      2)
               else
                round(l_penalty_adsl, 2)
             end) SUM_CORR_PENALTY_adsl, -- без НДС
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_penalty_adsl -
                      l_penalty_adsl /
                      Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                      2)
               else
                round(l_penalty_adsl * (1 + l_Pr_NDS), 2) -
                round(l_penalty_adsl, 2)
             end) SUM_NDS_CORR_PENALTY_adsl, --НДС
             (case
               when l_month < to_date('01.07.2013', 'dd.mm.yyyy') then
                round(l_penalty_adsl, 2)
               else
                round(l_penalty_adsl * (1 + l_Pr_NDS), 2)
             end) SUM_WITH_NDS_CORR_PENALTY_adsl,
             -- СКП
             l_kol_SKP kol_SKP,
             round(l_SumWNDS_SKP /
                   Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                   2) SUM_SKP,
             round(l_SumWNDS_SKP -
                   l_SumWNDS_SKP /
                   Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                   2) SUM_NDS_SKP,
             round(l_SumWNDS_SKP, 2) SUM_WITH_NDS_SKP,
             --
             round(l_SumWNDS_SKP_loyalty /
                   Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                   2) SUM_SKP_loyalty,
             round(l_SumWNDS_SKP_loyalty -
                   l_SumWNDS_SKP_loyalty /
                   Decode(nvl(l_Pr_NDS, 0), 0, 1, 1 + l_Pr_NDS),
                   2) SUM_NDS_SKP_loyalty,
             round(l_SumWNDS_SKP_loyalty, 2) SUM_WITH_NDS_SKP_loyalty,
             -- ott
             l_kol_ott kol_ott,
             round(l_SumWNDS_ott / Decode(nvl(0.18, 0), 0, 1, 1 + 0.18), 2) SUM_ott,
             round(l_SumWNDS_ott -
                   l_SumWNDS_ott / Decode(nvl(0.18, 0), 0, 1, 1 + 0.18),
                   2) SUM_NDS_ott,
             round(l_SumWNDS_ott, 2) SUM_WITH_NDS_ott,
             --
             round(l_with_corr_ott, 2) SUM_CORR_ott, -- без НДС
             round(l_with_corr_ott * (1 + 0.18), 2) -
             round(l_with_corr_ott, 2) SUM_NDS_CORR_ott, --НДС
             round(l_with_corr_ott * (1 + 0.18), 2) SUM_WITH_NDS_CORR_ott,
             round(l_penalty_ott, 2) SUM_CORR_PENALTY_ott, -- без НДС
             round(l_penalty_ott * (1 + 0.18), 2) - round(l_penalty_ott, 2) SUM_NDS_CORR_PENALTY_ott, --НДС
             round(l_penalty_ott * (1 + 0.18), 2) SUM_WITH_NDS_CORR_PENALTY_ott,
             l_SumWNDS_deliv1,
             l_Sum_deliv1,
             l_SumWNDS_deliv2,
             l_Sum_deliv2
        from dual;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'generate_stat');
      return null;
  end GetReportPremiaMoney_test;
  ------------------- Номер счет-фактуры для агентов -----------------------------
  procedure NumAgentPoruchenie(pi_rp_id       in number,
                               pi_num_account in varchar2, -- номер счет-фактуры для агентов
                               pi_worker_id   in T_USERS.USR_ID%type,
                               po_err_num     out pls_integer,
                               po_err_msg     out varchar2) is
  begin
    update t_report_sf t
       set t.number_sf = pi_num_account
     where t.id_repper = pi_rp_id;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
  end NumAgentPoruchenie;
  ------------------ Общее кол-во подключений для курирующей орг-ии и ее диллерам ------------------
  function Get_Cur_Connections(pi_dates     in t_report_period.dates%type,
                               pi_datepo    in t_report_period.datepo%type,
                               pi_org_pid   in number, -- курирующая организация
                               pi_worker_id in T_USERS.USR_ID%type,
                               po_err_num   out pls_integer,
                               po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    if (not Security_pkg.Check_Rights_str('EISSD.REPORT.PRINCIPAL.VIEW',
                                          pi_org_pid,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      return null;
    end if;
    open res for
      select distinct ro.org_pid,
                      oo.org_name,
                      to_char(rp.dates, 'mm yyyy') dd,
                      rp.dates,
                      count(rc.id_conn) count
        from t_report_period    rp,
             t_org_relations    ro,
             t_report_real_conn rc,
             t_organizations    o,
             t_organizations    oo
       where rp.id_org = ro.org_id
         and rp.is_actual = 1
         and ro.org_reltype in (1003, 1004, 999)
         and (pi_org_pid is null or
             (ro.org_pid = pi_org_pid or rp.id_org = pi_org_pid))
         and rc.id_repper = rp.id
         and rp.id_org in
             (select * from TABLE(get_user_orgs_tab(pi_worker_id)))
         and ro.org_id = o.org_id
         and ro.org_pid = oo.org_id
         and (pi_dates is null or rp.dates >= pi_dates)
         and (pi_datepo is null or rp.datepo <= pi_datepo)
            --51621
         and rc.ci = 1
       group by ro.org_pid, oo.org_name, rp.dates
       order by oo.org_name, rp.dates;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Cur_Connections;
  ----------------------------- Книга покупок -----------------------------
  function Buying_Book(pi_dates     in t_report_period.dates%type,
                       pi_datepo    in t_report_period.datepo%type,
                       pi_org_pid   in number, -- курирующая организация
                       pi_worker_id in T_USERS.USR_ID%type,
                       po_err_num   out pls_integer,
                       po_err_msg   out varchar2) return sys_refcursor is
    res sys_refcursor;
  begin
    if (not Security_pkg.Check_Rights_str('EISSD.REPORT.PRINCIPAL.VIEW',
                                          pi_org_pid,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      return null;
    end if;
    open res for
      select to_char(rp.datepo, 'DD.MM.YYYY') || ' ' || sf.number_sf dat_num,
             sf.payment_date dat_oplata,
             sf.adoption_date dat,
             o.org_name,
             o.org_inn,
             o.org_kpp,
             'Россия' Rus,
             sum(sf.summa * (1 + rp.pr_nds)) nagr_nds,
             sum(decode(rp.pr_nds, 0.18, sf.summa, 0)) nagr,
             sum(nvl(sf.summa, 0) * rp.pr_nds) nds,
             0 nagr_10,
             0 nds_10,
             0 nagr_0,
             0 nagr_20,
             0 nds_20,
             sum(decode(rp.pr_nds, 0, (sf.summa), 0)) nds_0
        from t_report_period rp,
             t_report_sf     sf,
             t_org_relations ro,
             t_organizations o
       where rp.id = sf.id_repper
         and rp.is_actual = 1
         and rp.id_org = ro.org_id
         and ro.org_reltype in (1003, 1004, 999)
         and (pi_org_pid is null or ro.org_pid = pi_org_pid)
         and rp.id_org in
             (select * from TABLE(get_user_orgs_tab(pi_worker_id)))
         and rp.id_org = o.org_id
         and (pi_dates is null or TRUNC(rp.datepo) >= (case
               when pi_dates is not null then
                pi_dates
             end))
         and (pi_datepo is null or TRUNC(rp.datepo) <= (case
               when pi_datepo is not null then
                pi_datepo
             end))
       group by rp.datepo,
                sf.payment_date,
                sf.adoption_date,
                o.org_name,
                o.org_inn,
                o.org_kpp,
                sf.number_sf
       order by rp.datepo,
                sf.payment_date,
                sf.adoption_date,
                o.org_name,
                o.org_inn,
                o.org_kpp,
                sf.number_sf;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Buying_Book;
  -----------------------------
  procedure generate_stat(pi_Id_Repper number, pi_Worker_Id number) is
    res                    sys_refcursor;
    l_Org_Id               number;
    L_DateS                date;
    l_Kol                  number;
    l_Sum_gsm              Number;
    l_NDSSum_gsm           number;
    l_SumWithNDS_gsm       number;
    l_Kol_ADSL             number;
    l_Sum_adsl             Number;
    l_NDSSum_adsl          number;
    l_SumWithNDS_adsl      number;
    l_Sum_corr_gsm         Number;
    l_NDSSum_corr_gsm      number;
    l_SumWithNDS_corr_gsm  number;
    l_Sum_corr_adsl        Number;
    l_NDSSum_corr_adsl     number;
    l_SumWithNDS_corr_adsl number;
    l_Kol_ab_ser           number;
    l_Sum_ab_ser           number;
    l_NDSSum_ab_ser        number;
    l_SumWithNDS_ab_ser    number;
    --34552
    l_Sum_corr_ab_ser        number;
    l_NDSSum_corr_ab_ser     number;
    l_SumWithNDS_corr_ab_ser number;
    --дист.продажи
    l_Kol_RemS        number;
    l_Sum_RemS        number;
    l_NDSSum_RemS     number;
    l_SumWithNDS_RemS number;
    l_cnt             number := 0;
    po_err_num        pls_integer;
    po_err_msg        varchar2(2000);
    l_dog_id          number;
    l_pr_nds          number;
    --штрафы
    l_Sum_corr_penalty_gsm    number;
    l_NDSSum_penalty_gsm      number;
    l_SumWithNDS_penalty_gsm  number;
    l_Sum_corr_penalty_adsl   number;
    l_NDSSum_penalty_adsl     number;
    l_SumWithNDS_penalty_adsl number;
    --
    kol_SKP                  number;
    SUM_SKP                  number;
    SUM_NDS_SKP              number;
    SUM_WITH_NDS_SKP         number;
    SUM_SKP_loyalty          number;
    SUM_NDS_SKP_loyalty      number;
    SUM_WITH_NDS_SKP_loyalty number;
    kol_ott                  number;
    SUM_ott                  number;
    SUM_NDS_ott              number;
    SUM_WITH_NDS_ott         number;
    --
    l_Sum_corr_ott           number;
    l_NDSSum_corr_ott        number;
    l_SumWithNDS_corr_ott    number;
    l_Sum_corr_penalty_ott   number;
    l_NDSSum_penalty_ott     number;
    l_SumWithNDS_penalty_ott number;
    --доставка
    l_SumWNDS_deliv1 Number := 0;
    l_Sum_deliv1     Number := 0;
    l_SumWNDS_deliv2 Number := 0;
    l_Sum_deliv2     Number := 0;

  begin
    Select rp.id_org, rp.dates, rp.dog_id, nvl(rp.pr_nds, 0)
      into l_Org_Id, L_DateS, l_dog_id, l_pr_nds
      from t_Report_Period rp
     Where rp.id = pi_Id_Repper
       and rp.is_actual = 1;
    Select Count(*)
      into l_Cnt
      from t_report_Stat t
     Where t.id_repper = pi_Id_Repper;
    If l_Cnt > 0 then
      delete from t_report_Stat t Where t.id_repper = pi_Id_Repper;
    End If;
    Res := Report_Period.GetReportPremiaMoney_Test(pi_Id_Repper,
                                                   777,
                                                   1,
                                                   po_err_num,
                                                   po_err_msg);
    LOOP
      FETCH res
        INTO l_Kol,
             l_Sum_gsm,
             l_NDSSum_gsm,
             l_SumWithNDS_gsm,
             l_Kol_ADSL,
             l_Sum_adsl,
             l_NDSSum_adsl,
             l_SumWithNDS_adsl,
             -- Абонентское обслуживание 31260 14.06.2011
             l_Kol_ab_ser,
             l_Sum_ab_ser,
             l_NDSSum_ab_ser,
             l_SumWithNDS_ab_ser,
             --Дист.продажи
             l_Kol_RemS,
             l_Sum_RemS,
             l_NDSSum_RemS,
             l_SumWithNDS_RemS,
             l_Sum_corr_gsm,
             l_NDSSum_corr_gsm,
             l_SumWithNDS_corr_gsm,
             l_Sum_corr_adsl,
             l_NDSSum_corr_adsl,
             l_SumWithNDS_corr_adsl,
             --34552
             l_Sum_corr_ab_ser,
             l_NDSSum_corr_ab_ser,
             l_SumWithNDS_corr_ab_ser,
             l_Sum_corr_penalty_gsm,
             l_NDSSum_penalty_gsm,
             l_SumWithNDS_penalty_gsm,
             l_Sum_corr_penalty_adsl,
             l_NDSSum_penalty_adsl,
             l_SumWithNDS_penalty_adsl,
             kol_SKP,
             SUM_SKP,
             SUM_NDS_SKP,
             SUM_WITH_NDS_SKP,
             --
             SUM_SKP_loyalty,
             SUM_NDS_SKP_loyalty,
             SUM_WITH_NDS_SKP_loyalty,
             --
             kol_ott,
             SUM_ott,
             SUM_NDS_ott,
             SUM_WITH_NDS_ott,
             --
             l_Sum_corr_ott,
             l_NDSSum_corr_ott,
             l_SumWithNDS_corr_ott,
             l_Sum_corr_penalty_ott,
             l_NDSSum_penalty_ott,
             l_SumWithNDS_penalty_ott,
             --доставка
             l_SumWNDS_deliv1,
             l_Sum_deliv1,
             l_SumWNDS_deliv2,
             l_Sum_deliv2;
      EXIT WHEN res%NOTFOUND;
      --
      if l_SumWNDS_deliv1 <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Агентское вознаграждение за доставку/самовывоз',
           l_dog_id,
           l_Sum_deliv1 - nvl(l_Sum_deliv2, 0),
           l_SumWNDS_deliv1 - nvl(l_SumWNDS_deliv2, 0) -
           (l_Sum_deliv1 - nvl(l_Sum_deliv2, 0)),
           l_SumWNDS_deliv1 - nvl(l_SumWNDS_deliv2, 0),
           9668);
      end if;
      --
      if SUM_ott <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Агентское вознаграждение за оборудование',
           l_dog_id,
           Sum_ott,
           SUM_WITH_NDS_ott - Sum_ott,
           SUM_WITH_NDS_ott,
           2012);
      end if;
      if l_Sum_corr_ott <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Корректировки по оборудованию',
           l_dog_id,
           l_Sum_corr_ott,
           l_NDSSum_corr_ott,
           l_SumWithNDS_corr_ott,
           2012);
      end if;
      if l_Sum_corr_penalty_ott <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Штрафные санкции по оборудованию',
           l_dog_id,
           l_Sum_corr_penalty_ott,
           l_NDSSum_penalty_ott,
           l_SumWithNDS_penalty_ott,
           2012);
      end if;
      --
      if SUM_SKP <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Агентское вознаграждение за компьютерную помощь',
           l_dog_id,
           Sum_skp,
           round(Sum_skp * (1 + l_pr_nds), 2) - Sum_skp,
           round(Sum_skp * (1 + l_pr_nds), 2),
           4001);
      end if;
      if SUM_SKP_loyalty <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Агентское вознаграждение за лояльность по компьютерной помощи',
           l_dog_id,
           SUM_SKP_loyalty,
           round(SUM_SKP_loyalty * (1 + l_pr_nds), 2) - SUM_SKP_loyalty,
           round(SUM_SKP_loyalty * (1 + l_pr_nds), 2),
           4001);
      end if;
      if l_Sum_corr_penalty_gsm <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Штрафные санкции GSM',
           l_dog_id,
           l_Sum_corr_penalty_gsm,
           l_NDSSum_penalty_gsm,
           l_SumWithNDS_penalty_gsm,
           2000);
      end if;
      if l_Sum_corr_penalty_adsl <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Штрафные санкции ADSL',
           l_dog_id,
           l_Sum_corr_penalty_adsl,
           l_NDSSum_penalty_adsl,
           l_SumWithNDS_penalty_adsl,
           2001);
      end if;

      if l_Sum_gsm <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Агентское вознаграждение за GSM - подключения',
           l_dog_id,
           l_Sum_gsm,
           round(l_Sum_gsm * (1 + l_pr_nds), 2) - l_Sum_gsm,
           round(l_Sum_gsm * (1 + l_pr_nds), 2),
           2000);
      end if;
      if l_Sum_adsl <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Агентское вознаграждение за ADSL - подключения',
           l_dog_id,
           l_Sum_adsl,
           l_NDSSum_adsl,
           l_SumWithNDS_adsl,
           2001);
      end if;
      if l_Sum_corr_gsm <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Корректировки (GSM)',
           l_dog_id,
           l_Sum_corr_gsm,
           l_NDSSum_corr_gsm,
           l_SumWithNDS_corr_gsm,
           2000);
      end if;
      if l_Sum_corr_adsl <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Корректировки (ADSL)',
           l_dog_id,
           l_Sum_corr_adsl,
           l_NDSSum_corr_adsl,
           l_SumWithNDS_corr_adsl,
           2001);
      end if;
      -- Абонентское обслуживание 31260 14.06.2011
      if l_Sum_ab_ser <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Агентское вознаграждение за абонентское обслуживание',
           l_dog_id,
           l_Sum_ab_ser,
           round(l_Sum_ab_ser * (1 + l_pr_nds), 2) - l_Sum_ab_ser,
           round(l_Sum_ab_ser * (1 + l_pr_nds), 2),
           4000);
      end if;
      --
      if l_Sum_RemS <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Агентское вознаграждение за дистанционные продажи',
           l_dog_id,
           l_Sum_RemS,
           round(l_Sum_RemS * (1 + l_pr_nds), 2) - l_Sum_RemS,
           round(l_Sum_RemS * (1 + l_pr_nds), 2),
           2001);
      end if;
      if l_Sum_corr_ab_ser <> 0 then
        insert into t_report_Stat
          (id_repper,
           org_id,
           d_month,
           name,
           dog_id,
           sum,
           nds,
           sum_with_nds,
           perm)
        values
          (pi_Id_Repper,
           l_Org_Id,
           L_DateS,
           'Корректировки (абонентское обслуживание)',
           l_dog_id,
           l_Sum_corr_ab_ser,
           l_NDSSum_corr_ab_ser,
           l_SumWithNDS_corr_ab_ser,
           4000);
      end if;
    End Loop;
    close res;
  Exception
    When Others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'generate_stat');
      dbms_output.put_line(sqlerrm);
      null;
  end generate_stat;
  ------------------------------------------------------------------
  --  Формирование платежных поручений за агентское вознаграждение
  --  по нескольким организациям
  ------------------------------------------------------------------
  Function Get_Plat_Por(pi_org_tab   in num_tab,
                        pi_date_top  in date,
                        pi_date_end  in date,
                        pi_number    in number,
                        pi_date      in date,
                        pi_worker_id in number,
                        po_err_num   out pls_integer,
                        po_err_msg   out varchar2) return sys_refcursor is
    res       sys_refcursor;
    L_Number  number := NVL(Pi_Number, 0);
    L_Date    date := NvL(Pi_Date, sysdate);
    l_org_pid number;
  Begin
    select max(tor.org_pid)
      into l_org_pid
      from mv_org_tree tor
     where tor.org_id in (select * from table(pi_org_tab)); -- для РТМ старое не работало
    if (not Security_pkg.Check_Rights_str('EISSD.REPORT.PRINCIPAL.VIEW',
                                          l_org_pid,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      return null;
    end if;
    Open Res for
      Select L_Number + RowNum Nomer,
             L_Date DatePor,
             (Select sum(rs.summa_with_nds)
                from t_report_sf rs
               where rs.id_repper = rp.id) Summa,
             decode(orec.rs, null, org_k.org_settl_account, orec.rs) Plat_Shet,
             null DateSpis, -- !!!!  Дата списания ДС на момент формирования ПП не известна
             org_k.org_inn Plat_INN,
             org_K.Org_Kpp Plat_KPP,
             Trim(org_K.Org_Full_Name) Plat_Name,
             Trim(org_K.Org_Full_Name) Plat_Name1,
             NVL(orec.rs, org_k.org_settl_account) Plat_RS,
             NVL(orec.name_bank, bk_k.namep) Plat_BankName1,
             'г.' || NVL(orec.city_bank, bk_k.nnp) Plat_BankName2,
             NVL(orec.bik, org_k.org_bik) Plat_BIK,
             NVL(orec.ks, org_k.org_con_account) Plat_KS,
             org_d.org_settl_account Pol_Shet,
             org_d.org_inn Pol_INN,
             org_d.Org_Kpp Pol_KPP,
             NVL(trim(org_d.org_full_name), Trim(org_d.Org_Name)) Pol_Name,
             NVL(trim(org_d.org_full_name), Trim(org_d.Org_Name)) Pol_Name1,
             org_d.org_settl_account Pol_RS,
             bk_d.namep Pol_BankName1,
             'г.' || bk_d.nnp Pol_BankName2,
             org_d.org_bik Pol_BIK,
             org_d.org_con_account Pol_KS,
             'электронно' VidPlat,
             '01' VidOpl,
             Null StatSost,
             Null P_KBK,
             Null P_OKATO,
             Null P_OSN,
             Null P_Period,
             Null P_Nomer,
             Null P_Date,
             Null P_Type,
             null SrokPlat, -- !!!!  Срок платежа на момент формирования ПП не известна
             6 Ochered,
             'Оплата' ||
             Decode(rp.num_account,
                    null,
                    '',
                    ' по счету ' || Trim(rp.num_account) || ' от ' ||
                    To_Char(rp.datepo, 'dd.mm.yyyy')) || ' по дог.№ ' ||
             trim(dog.dog_number) || ' от ' ||
             To_Char(dog.dog_date, 'dd.mm.yyyy') ||
             ', за выполненные работы. Код (C*' ||
             acc_operations.Get_Lic_Acc_Id_By_Org_Dog(rp.id_org, rp.dog_id) || '*' ||
             To_Char(rp.datepo, 'mmyyyy') || '*)' NaznPlat,
             (Select sum(rs.sum)
                from t_Report_Stat rs
               where rs.id_repper = rp.id) * NVL(rp.pr_nds, 0) Sum_NDS
        from t_org_relations tor
        Left join t_org_rekv orec
          on orec.id_org = tor.org_pid
        join t_dogovor dog
          on dog.org_rel_id = tor.id
        Join t_Organizations org_D
          on org_D.Org_Id = tor.org_id
        left join bnkseek bk_D
          on bk_D.newnum = org_D.Org_Bik
        Join t_Organizations org_K
          on org_K.Org_Id = tor.org_pid
        left join bnkseek bk_k
          on bk_k.newnum = org_K.Org_Bik
        join t_Report_Period rp
          on rp.dog_id = dog.dog_id
         and rp.is_actual = 1
       Where tor.org_id in (select * from table(pi_org_tab))
         and Trunc(rp.dates) between trunc(Pi_Date_Top) and
             Trunc(Pi_Date_End - 1)
         and (Select sum(rs.summa_with_nds)
                from t_report_sf rs
               where rs.id_repper = rp.id) > 0;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  End Get_Plat_Por;
  -- возвращает курсор, который содержит сведения о суммах агентских вознаграждений
  -- по отделению.
  -- Курсор содержит поля:
  -- 1. AGENTS - название агентов
  -- 2. D_MONTH - дата
  -- 3. NAGR - сумма агентского вознаграждения
  -- 4. KOL - количество подключений
  function Get_Report_Sum_Otdelenie(pi_org_id  in t_organizations.org_id%type,
                                    po_err_num out pls_integer,
                                    po_err_msg out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select org1.org_name AGENTS,
             sf.date_s d_month, --new
             sum(sf.summa) NAGR, -- new
             null KOL -- new
        from t_org_relations rel, t_organizations org1, t_report_sf sf -- new
       where org1.org_id = rel.org_id
         and rel.org_pid = pi_org_id
         and rel.org_reltype in (1004, 999)
         and org1.org_id = sf.id_org_sel
       group by org1.org_name, date_s
       order by org1.org_name, date_s;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Report_Sum_Otdelenie;
  -----------------------------------------------------------------------
  -- возвращает курсор, который содержит сведения о суммах агентских вознаграждений
  -- по месяцам (свёрнутый вид).
  -- Курсор содержит поля:
  -- 1. AGENTS - название агентов
  -- 2. D_MONTH - дата
  -- 3. NAGR - сумма агентского вознаграждения
  -- 4. KOL - количество подключений
  function Get_Report_Sum_Federal_Short(po_err_num out pls_integer,
                                        po_err_msg out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select o.org_name AGENTS,
             sf.date_s d_month, -- new
             sum(sf.summa) NAGR, --new
             null KOL --new
        from t_organizations o, t_org_relations rel, t_report_sf sf -- new
       where o.org_id = rel.org_id
         and rel.org_reltype in (1004, 999)
         and (sf.id_org_sel = o.org_id) -- new
       group by o.org_name, date_s
       order by o.org_name, date_s;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Report_Sum_Federal_Short;
  -----------------------------------------------------------------------
  -- возвращает курсор, который содержит сведения о суммах агентских вознаграждений
  -- по отделениям за определённый месяц (подробно).
  -- Курсор содержит поля:
  -- 1. OTDELENIE - название отделения
  -- 2. AGENTS - название агентов
  -- 3. NAGR - сумма агентского вознаграждения
  -- 4. KOL - количество подключений
  function Get_Report_Sum_Federal_Full(pi_date    in t_report_stat.d_month%type,
                                       po_data    out sys_refcursor,
                                       po_err_num out pls_integer,
                                       po_err_msg out varchar2)
    return sys_refcursor is
    res      sys_refcursor;
    new_date t_report_stat.d_month%type;
  begin
    -- Приводим дату к первому числу текущего месяца --
    new_date := last_day(add_months(pi_date, -1)) + 1;
    open res for
      select org1.org_name OTDELENIE,
             org3.org_name AGENTS,
             sum(sf.summa) NAGR, -- new
             null kol -- new
        from t_organizations org1
        join t_org_relations rel1
          on org1.org_id = rel1.org_pid
        join t_organizations org2
          on rel1.org_id = org2.org_id
        join t_org_relations rel2
          on org2.org_id = rel2.org_id
        join t_organizations org3
          on rel2.org_pid = org3.org_id
        join t_report_sf sf
          on sf.id_org_sel = org2.org_id -- new
       where org1.org_id in
             (2001272, 2001279, 2001433, 2001454, 2001270, 2001280, 2001455)
         and rel1.org_reltype in (1004, 999)
         and rel2.org_reltype = 1005
         and Trunc(sf.date_s, 'dd.mm.rrrr') = new_date -- new
       group by org3.org_name, org1.org_name
       order by org3.org_name;
    open po_data for
      select ORG.org_name OTDELENIE, f_org_name AGENTS
        from t_organizations ORG, t_org_names
       where ORG.org_id in
             (2001272, 2001270, 2001279, 2001280, 2001433, 2001455, 2001454)
         and instr(f_org_name, 'Итого') = 0
       order by 1, 2;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Report_Sum_Federal_Full;
  -----------------------------------------------------------------------
  procedure dump(pi_sql_text in varchar2) is
    pragma autonomous_transaction;
  begin
    insert into tmp_sql_test (sql_text) values (pi_sql_text);
    commit;
  end;
  -----------------------------------------------------------------------
  function Get_Report_Activ_Gr(pi_date_from in date, pi_date_to in date)
    return sys_refcursor is
    res               sys_refcursor;
    l_month_count     number;
    i                 number;
    l_from_table      varchar2(2000);
    l_where_t_abonent varchar2(2000);
    l_where_t_activ   varchar2(2000);
    l_sel_amount_pay  varchar2(2000);
    l_big_sel_amount  varchar2(32000);
    l_sel_arpu        varchar2(2000);
    l_sel_kol         varchar2(2000);
    sql_text          varchar2(32767);
    l_sel             varchar2(32767);
    l_where           varchar2(32767);
    l_date_activ1     date;
    l_date_activ2     date;
    l_date_activ3     date;
    l_date_activ4     date;
  begin
    l_month_count := months_between(pi_date_to, pi_date_from);
    i             := 0;
    for i in 1 .. l_month_count - 1 loop
      if i = 1 then
        l_date_activ1     := add_months(pi_date_from, 1);
        l_date_activ2     := add_months(pi_date_from, 2) - 1;
        l_from_table      := ', t_ab_pay_activity act';
        l_where_t_abonent := 'and a.ab_id = act.abonent_id ';
        l_where_t_activ   := 'and trunc(act.date_begin) = ''' ||
                             l_date_activ1 || '''
                            and trunc(act.date_end) = ''' ||
                             l_date_activ2 || ''' ';
        l_sel_amount_pay  := ',  sum(act.amount_pay) amount_pay, ';
        l_sel_arpu        := 'round(sum(act.amount_pay) / decode(count(ts.sim_callsign), 0, 1, count(ts.sim_callsign)), 2) arpu, ';
        l_sel_kol         := 'round(count(case
                                          when act.amount_pay <> 0 then
                                            act.amount_pay
                                        end) / decode(count(ts.sim_callsign), 0, 1, count(ts.sim_callsign)), 4) kol';
        l_big_sel_amount  := ',
           decode(is_group,
                  0,
                  to_char(decode(is_group2,
                                 0,
                                 to_char(amount_pay),
                                 1,
                                 to_char(arpu || ''/'' || kol * 100))),
                  1,
                  to_char(arpu || ''/'' || kol * 100)) amount2 ';
      else
        l_date_activ3     := add_months(pi_date_from, i);
        l_date_activ4     := add_months(pi_date_from, i + 1) - 1;
        l_from_table      := l_from_table || ',
                   (select abonent_id, amount_pay, trunc(date_begin) date_begin, trunc(date_end) date_end from  t_ab_pay_activity) act' ||
                             to_char(i - 1);
        l_where_t_abonent := l_where_t_abonent || 'and a.ab_id = act' ||
                             to_char(i - 1) || '.abonent_id(+) ';
        l_where_t_activ   := l_where_t_activ || 'and act' || to_char(i - 1) ||
                             '.date_begin(+) = ''' || l_date_activ3 || '''
                                             and act' ||
                             to_char(i - 1) || '.date_end(+) = ''' ||
                             l_date_activ4 || ''' ';
        l_sel_amount_pay  := l_sel_amount_pay || 'sum(act' ||
                             to_char(i - 1) || '.amount_pay) amount_pay' ||
                             to_char(i - 1) || ', ';
        l_sel_arpu        := l_sel_arpu || 'round(sum(act' ||
                             to_char(i - 1) ||
                             '.amount_pay) / decode(count(ts.sim_callsign), 0, 1, count(ts.sim_callsign)), 2) arpu' ||
                             to_char(i - 1) || ', ';
        l_sel_kol         := l_sel_kol ||
                             ', round(count(case
                                                         when act' ||
                             to_char(i - 1) ||
                             '.amount_pay <> 0 then
                                                           act' ||
                             to_char(i - 1) ||
                             '.amount_pay
                                                       end) / decode(count(ts.sim_callsign), 0, 1, count(ts.sim_callsign)), 4) kol' ||
                             to_char(i - 1);
        l_big_sel_amount  := l_big_sel_amount || ',
           decode(is_group,
                  0,
                  to_char(decode(is_group2,
                                 0,
                                 to_char(amount_pay' ||
                             to_char(i - 1) || '),
                                 1,
                                 to_char(arpu' ||
                             to_char(i - 1) || ' || ''/'' || kol' ||
                             to_char(i - 1) ||
                             ' * 100))),
                  1,
                  to_char(arpu' ||
                             to_char(i - 1) || ' || ''/'' || kol' ||
                             to_char(i - 1) || ' * 100)) amount' ||
                             to_char(i + 1);
      end if;
    end loop;
    l_sel    := l_sel_amount_pay || ' ' || l_sel_arpu || ' ' || l_sel_kol;
    l_where  := l_where_t_abonent || ' ' || l_where_t_activ;
    sql_text := '
  select   is_group,
           is_group2,
           region_id,
           region_name,
           diler_id,
           decode(is_group,
                  0,
                  org_name,
                  1,''Итого по всем дилерам'') diler_name,
           decode(is_group,
                  0,
                  decode(is_group2,
                         0,
                         subdiler,
                         1,''Итого: АРПУ, руб./Итого: % активных (>0)''),
                  1,
                  decode(is_group2,
                         0,
                         subdiler,
                         1,''Итого: АРПУ, руб./Итого: % активных (>0)'')) subdiler,
           abon,
           sim_callsign,
           ab_dog_date,
           decode(kol0,
                  0, null,
                  decode(is_group,
                         0, to_char(decode(is_group2,
                                           0, to_char(amount_pay0),
                                           1, to_char(arpu0 || ''/'' || kol0 * 100))),
                  1, to_char(arpu0 || ''/'' || kol0 * 100))) amount1' ||
                l_big_sel_amount || '
      from (select grouping(o1.org_name) is_group,
                   grouping(p.person_lastname) is_group2,
                   r.reg_id region_id,
                   r.name region_name,
                   o1.org_id   diler_id,
                   o1.org_name,
                   decode(a.root_org_id,
                          a.org_id,
                          null,
                          (o1.org_name || '' ('' ||
                          Get_First_CC_By_Org(o1.org_id) || '')'')) subdiler,
                   p.person_lastname || '' '' || p.person_firstname || '' '' ||
                   p.person_middlename abon,
                   ts.sim_callsign,
                   a.ab_dog_date,
                   sum(act0.amount_pay) amount_pay0,
               round(sum(act0.amount_pay) / decode(count(ts.sim_callsign), 0, 1, count(ts.sim_callsign)), 2) arpu0,
               round(count(case
                             when act0.amount_pay <> 0 then
                              act0.amount_pay
                           end) / decode(count(ts.sim_callsign), 0, 1, count(ts.sim_callsign)),
                     4) kol0 ' || l_sel || '
              from t_abonent       a,
                   t_organizations o,
                   t_organizations o1,
                   t_dic_region_data       r,
                   t_clients       c,
                   t_person        p,
                   t_tmc           t,
                   t_tmc_sim       ts,
                   (select abonent_id,
                           amount_place,
                           trunc(date_begin) date_begin,
                           trunc(date_end) date_end,
                           amount_pay
                      from t_ab_pay_activity) act0' ||
                l_from_table || '
             where a.org_id = o.org_id
               and a.root_org_id = o1.org_id
               and o.region_id = r.reg_id
               and c.client_id = a.client_id
               and p.person_id = c.fullinfo_id
               and a.ab_tmc_id = t.tmc_id
               and t.tmc_id = ts.tmc_id
               and (trunc(a.ab_dog_date) between :pi_date_from and
                   add_months(:pi_date_from, 1))
               and a.ab_id = act0.abonent_id(+)
               and act0.date_begin (+) = :pi_date_from
               and act0.date_end (+) = add_months(:pi_date_from, 1) - 1 ' ||
                l_where || '
             group by grouping sets((r.reg_id, r.name, o1.org_id, o1.org_name, p.person_lastname, ts.sim_callsign, a.ab_dog_date, o1.org_id, a.org_id, a.root_org_id, p.person_firstname, p.person_middlename),(r.reg_id, r.name, o1.org_name),(r.reg_id, r.name)))
     order by region_id, org_name, is_group2, is_group';
    dump(sql_text);
    open res for sql_text
      using pi_date_from, pi_date_from, pi_date_from, pi_date_from;
    return res;
  exception
    when others then
      return null;
  end Get_Report_Activ_Gr;
  ------------------- Номер заявки на оплату для агентов -----------------------------
  procedure Set_Num_zaiav_opl(pi_rp_id         in number,
                              pi_num_zaiav_opl in number, -- номер счет-фактуры для агентов
                              pi_DT_ZAIAV_OPL  in date,
                              pi_worker_id     in T_USERS.USR_ID%type,
                              po_err_num       out pls_integer,
                              po_err_msg       out varchar2) is
  begin
    update T_REPORT_PERIOD p
       set p.num_zaiav_opl = pi_num_zaiav_opl,
           p.dt_zaiav_opl  = pi_DT_ZAIAV_OPL
     where p.id = pi_rp_id;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
  end Set_Num_zaiav_opl;
  ------------------------------------------------------------------------------------
  -- Выгрузка информации в АСР PiterService по абонентам подключеным за последние 4 месяца.
  procedure ab_pay_request is
  begin
    -- Call the procedure
    /*asr_protocol_out.ab_pay_request(pi_asr_id     => 3,
    pi_date_begin => trunc(Add_Months(sysdate,
                                      -1),
                           'mm'),
    pi_date_end   => trunc(sysdate, 'mm') -
                     1 / 24 / 60 / 60);*/
    null;
  end;
  -------------------------------------------------------------------------------
  -- Акт инвентаризации расчётов
  function Get_Akt_Inventar(pi_date    in date,
                            pi_org_id  in number, -- курирующая
                            po_err_num out pls_integer,
                            po_err_msg out t_Err_Msg) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      Select prm_id,
             Max(org_id) org_id,
             Decode(Grouping(org_name),
                    0,
                    org_name,
                    Decode(prm_Id,
                           0,
                           'Итого по агентам:',
                           Decode(Grouping(prm_Id),
                                  0,
                                  'Итого по Комиссионерам:',
                                  'Всего:'))) org_name,
             sum(sum_nagr_opl) sum_nagr_opl,
             sign
        from (select t.prm_id,
                     t.org_id,
                     t.org_name,
                     t.dog_id,
                     abs(t.sum_nagr_opl) sum_nagr_opl,
                     (case
                       when t.sum_nagr_opl >= 0 then
                        1
                       else
                        0
                     end) sign
                from (select dog_P.prm_id,
                             org.org_id,
                             org.org_name,
                             td.dog_id,
                             sum(nvl(trs.summa_with_nds, 0)) -
                             sum(nvl(tro.amount, 0)) +
                             nvl(acc_operations.Get_Sum_Saldo_Lic_Res(td.dog_id,
                                                                      Add_Months(pi_date,
                                                                                 1) - 1),
                                 0) sum_nagr_opl
                        from t_dogovor td
                        join (Select t.dp_dog_id,
                                    Decode(t.dp_prm_id, 2003, 1, 0) prm_id
                               from t_dogovor_prm t
                              where t.dp_is_enabled = 1
                              Group By t.dp_dog_id,
                                       Decode(t.dp_prm_id, 2003, 1, 0)) dog_P
                          on dog_P.Dp_Dog_Id = td.dog_id
                        join t_report_period trp
                          on trp.dog_id = td.dog_id
                         and trunc(trp.dates) <= pi_date
                         and trp.is_actual = 1
                        join t_report_sf trs
                          on trp.id = trs.id_repper
                        left join t_report_oplata tro
                          on tro.id_repper = trp.id
                        join t_organizations org
                          on org.org_id = trp.id_org
                       where td.is_enabled = 1
                         and (org.org_id in
                             (select tor.org_id
                                 from t_org_relations tor
                               connect by prior tor.org_id = tor.org_pid
                                start with tor.org_pid = pi_org_id) or
                             org.org_id = pi_org_id)
                       group by dog_P.prm_id,
                                org.org_id,
                                org.org_name,
                                td.dog_id
                       Order by dog_P.prm_id,
                                org.org_id,
                                org.org_name,
                                td.dog_id) t
               order by sign desc, t.prm_id, t.org_name)
      having Grouping(sign) = 0 and Grouping(prm_id) = 0
       Group by rollUp(sign, prm_Id, org_name)
       order by sign desc, prm_id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Akt_Inventar;
  -----------------------------------------------------------------
  -- Возвращает список типов схем вознаграждения
  -- действующих у агента в текущем отчетном периоде
  -----------------------------------------------------------------
  function Get_Active_Schem_List(Pi_Rp_Id   in Number,
                                 po_err_num out pls_integer,
                                 po_err_msg out t_Err_Msg)
    return sys_refcursor is
    res sys_refcursor;
  begin
    Open res for
      select *
        from (select rp.id id_repper,
                     decode(rs.schema_type, 1, 3, 2, 3, rs.schema_type) Type_Shema
                from t_Report_Period rp
                join t_report_schema rs
                  on rs.id_schema = rp.premia_schema
                join t_report_real_conn c
                  on rp.id = c.id_repper
               Where rp.id = Pi_Rp_Id
                 and rp.is_actual = 1
                 and c.ci in (1, 2)
                 and rp.dates <= decode(decode(rs.schema_type,
                                               1,
                                               3,
                                               2,
                                               3,
                                               rs.schema_type),
                                        7,
                                        To_Date('01.05.2010', 'DD.MM.YYYY'),
                                        rp.dates)
              union All
              select ra.id_repper_cur id_repper,
                     decode(rs.schema_type, 1, 3, 2, 3, rs.schema_type) Type_Shema
                from t_report_activ ra
                join t_Report_Period rp
                  on rp.id = ra.id_repper
                 and rp.is_actual = 1
                join t_Report_Period r
                  on r.id = ra.id_repper_cur
                 and r.is_actual = 1
                join t_report_schema rs
                  on rs.id_schema = rp.premia_schema
               Where ra.id_repper_cur = Pi_Rp_Id
                 and r.dates <= decode(decode(rs.schema_type,
                                              1,
                                              3,
                                              2,
                                              3,
                                              rs.schema_type),
                                       7,
                                       To_Date('01.05.2010', 'DD.MM.YYYY'),
                                       r.dates))
       group By id_repper, Type_Shema;
    Return res;
  Exception
    When Others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  End Get_Active_Schem_List;
  -----------------------------------------------------------------
  --  Драйвер для получения Бонуса за продажу городских телефонов.
  Function Get_StavkaBonusCity(Pi_Org_Id in Number) return number is
  Begin
    If Pi_Org_Id in (201133) then
      Return 30;
    Else
      Return 0;
    End If;
  End;
  -----------------------------------------------------------------------------
  -- Pi_Type_Schema=3 дергает GetAgentPoruchenie_Test
  -- Pi_Type_Schema=4 дергает GetAgentPoruchenie_4
  -- Pi_Type_Schema=5 дергает GetAgentPoruchenie_5
  -- Pi_Type_Schema=6 дергает GetAgentPoruchenie_6
  -- Pi_Type_Schema=7 дергает GetAgentPoruchenie_7
  -------------------------------------------------------------------------------------
  function GetAgentPoruchenie_Test(pi_rp_id       in number,
                                   pi_worker_id   in T_USERS.USR_ID%type,
                                   Pi_Type_Schema in Number,
                                   Po_Formula     Out Varchar2,
                                   po_err_num     out pls_integer,
                                   po_err_msg     out t_Err_Msg)
    return sys_refcursor is
    res       sys_refcursor;
    i         number;
    cnt       number;
    l_Date_S  date;
    l_err_num number;
    l_err_msg varchar2(2048);
    l_check   number;
  Begin
    select t.dates
      into l_Date_S
      from t_report_period t
     where t.id = pi_rp_id;
    select count(*) into cnt from t_report_ap t where t.rp_id = pi_rp_id;
    if (cnt = 0) then
      l_check := stavka_sub.Calculation(pi_rp_id   => pi_rp_id,
                                        po_err_num => l_err_num,
                                        po_err_msg => l_err_msg);
      if l_check = 1 then
        insert into t_report_ap
          (rp_id,
           type_schema,
           dates,
           seg_id,
           seg_name,
           seg_tar_list,
           kol,
           kol_act,
           stavka_proc,
           stavka,
           sum,
           pr_nds,
           sumwnds,
           premia_schema,
           forsum,
           kod_stavka,
           kod_stavka_old,
           formula)
          select pi_rp_id,
                 4,
                 t.dates_ap dates, -- Дата периода
                 null seg_id, -- ИД сегмента
                 null as name_model, -- Название организации
                 null seg_tar_list, -- Название сегмента
                 count(t.ab_id) kol, -- Колич. прод. подкл. в периоде
                 0 kol_act, -- Колич активных из проданных в периоде
                 max(t.stavka_proc) stavka_Proc, -- Ставка с учетом процентов
                 (case
                   when nvl(max(t.stavka_proc), 0) = 0 then
                    max(t.stavka)
                   else
                    sum(t.stavka)
                 end) cost, -- Ставка действующая в периоде
                 sum(t.sum) sum, -- Сумма вознаграждения
                 max(t.pr_nds) pr_nds, -- Ставка НДС
                 sum(t.sumwnds) sumwnds, --Сумма с НДС
                 11 premia_schema, -- Схема вознаграждения действующая в тек. отч. периоде.
                 sum(t.forsum) ForSum,
                 t.kod_stavka Kod_Stavka,
                 case
                   when nvl(s.pay_number_months, 0) > 1 then
                    1420
                   when nvl(max(t.stavka_proc), 0) <> 0 then
                    1420
                   else
                    null
                 end,
                 null
            from t_report_abonent t
            join t_stavka s
              on s.id = t.kod_stavka
             and nvl(s.optional_equipment, 0) not in (7003, 4)
           where t.rp_id = pi_rp_id
             and t.type_ab = 1
             and t.sum >= 0
           group by t.kod_stavka, t.dates_ap, s.pay_number_months
          union
          select pi_rp_id,
                 4,
                 max(t.dates_ap) dates, -- Дата периода
                 null seg_id, -- ИД сегмента
                 nvl(mm.name_model, u.usb_model) name_model, -- Название организации
                 null seg_tar_list, -- Название сегмента
                 count(t.ab_id) kol, -- Колич. прод. подкл. в периоде
                 0 kol_act, -- Колич активных из проданных в периоде
                 max(t.stavka_proc) stavka_Proc, -- Ставка с учетом процентов
                 sum(t.stavka) cost, -- Ставка действующая в периоде
                 sum(t.sum) sum, -- Сумма вознаграждения
                 max(t.pr_nds) pr_nds, -- Ставка НДС
                 sum(t.sumwnds) sumwnds, --Сумма с НДС
                 11 premia_schema, -- Схема вознаграждения действующая в тек. отч. периоде.
                 sum(t.forsum) ForSum,
                 s.id Kod_Stavka,
                 1459,
                 null
            from t_report_abonent t
            join t_stavka s
              on s.id = t.kod_stavka
             and nvl(s.optional_equipment, 0) in (7003, 4)
            left join t_model_phone mm
              on mm.model_id = t.tar_id
             and s.optional_equipment = 7003
            left join t_modem_model_usb u
              on u.id = t.tar_id
             and s.optional_equipment = 4
           where t.rp_id = pi_rp_id
             and t.type_ab = 1
           group by s.id, nvl(mm.name_model, u.usb_model)
          union
          select pi_rp_id,
                 8,
                 max(dates) dates, -- Дата периода
                 seg_id, -- ИД сегмента
                 seg_name, -- Название организации
                 seg_tar_list, -- Название сегмента
                 count(kol) kol, -- Колич. прод. подкл. в периоде
                 kol_act, -- Колич активных из проданных в периоде
                 max(stavka_Proc) stavka_Proc, -- Ставка с учетом процентов
                 max(stavka) stavka, -- Ставка действующая в периоде
                 sum(sum) sum, -- Сумма вознаграждения
                 max(pr_nds) pr_nds, -- Ставка НДС
                 sum(sumwnds) sumwnds, --Сумма с НДС
                 11 premia_schema, -- Схема вознаграждения действующая в тек. отч. периоде.
                 sum(ForSum) ForSum,
                 Kod_Stavka,
                 null,
                 null
            from (select pi_rp_id,
                         8,
                         t.dates_ap    dates, -- Дата периода
                         null          seg_id, -- ИД сегмента
                         org.org_name  seg_name, -- Название организации
                         t.comments    seg_tar_list, -- Название сегмента
                         t.ab_id       kol, -- Колич. прод. подкл. в периоде
                         0             kol_act, -- Колич активных из проданных в периоде
                         t.stavka_Proc stavka_Proc, -- Ставка с учетом процентов
                         t.stavka      stavka, -- Ставка действующая в периоде
                         t.sum         sum, -- Сумма вознаграждения
                         t.pr_nds      pr_nds, -- Ставка НДС
                         t.sumwnds     sumwnds, --Сумма с НДС
                         11            premia_schema, -- Схема вознаграждения действующая в тек. отч. периоде.
                         t.ForSum      ForSum,
                         s.id          Kod_Stavka,
                         null
                    from t_report_abonent t
                    join t_stavka s
                      on s.id = t.kod_stavka
                     and nvl(s.is_ab_serv, 0) = 1
                    join t_ab_service_remote ab
                      on ab.id = t.ab_id
                    join t_dic_ab_service dic
                      on dic.id_req = ab.req_type
                    join t_organizations org
                      on org.org_id = ab.org_id
                   where t.rp_id = pi_rp_id
                     and t.type_ab = 3) ao
           group by Kod_Stavka,
                    seg_tar_list,
                    seg_name,
                    kol_act,
                    seg_id,
                    pi_rp_id
          union
          select pi_rp_id,
                 10,
                 t.dates_ap dates, -- Дата периода
                 null seg_id, -- ИД сегмента
                 null as name_model, -- Название организации
                 null seg_tar_list, -- Название сегмента
                 count(t.ab_id) kol, -- Колич. прод. подкл. в периоде
                 0 kol_act, -- Колич активных из проданных в периоде
                 max(t.stavka_proc) stavka_Proc, -- Ставка с учетом процентов
                 (case
                   when nvl(max(t.stavka_proc), 0) = 0 then
                    max(t.stavka)
                   else
                    sum(t.stavka)
                 end) cost, -- Ставка действующая в периоде
                 sum(t.sum) sum, -- Сумма вознаграждения
                 max(t.pr_nds) pr_nds, -- Ставка НДС
                 sum(t.sumwnds) sumwnds, --Сумма с НДС
                 11 premia_schema, -- Схема вознаграждения действующая в тек. отч. периоде.
                 sum(t.forsum) ForSum,
                 t.kod_stavka Kod_Stavka,
                 null,
                 null
            from t_report_abonent t
            join t_stavka s
              on s.id = t.kod_stavka
             and s.is_remote_sales = 1
            join t_stavka_orders ord
              on ord.id_stavka = s.id
           where t.rp_id = pi_rp_id
             and t.type_ab = 2
             and t.comments is not null
           group by t.kod_stavka, t.dates_ap --, s.pay_number_months
          union
          select pi_rp_id,
                 6,
                 t.dates_ap dates, -- Дата периода
                 null seg_id, -- ИД сегмента
                 null as name_model, -- Название организации
                 null seg_tar_list, -- Название сегмента
                 count(t.ab_id) kol, -- Колич. прод. подкл. в периоде
                 0 kol_act, -- Колич активных из проданных в периоде
                 max(t.stavka_proc) stavka_Proc, -- Ставка с учетом процентов
                 (case
                   when nvl(max(t.stavka_proc), 0) = 0 then
                    max(t.stavka)
                   else
                    sum(t.stavka)
                 end) cost, -- Ставка действующая в периоде
                 sum(t.sum) sum, -- Сумма вознаграждения
                 max(t.pr_nds) pr_nds, -- Ставка НДС
                 round(sum(t.sum) * (1 + nvl(max(t.pr_nds), 0)), 2) sumwnds, --Сумма с НДС
                 11 premia_schema, -- Схема вознаграждения действующая в тек. отч. периоде.
                 sum(t.forsum) ForSum,
                 t.kod_stavka Kod_Stavka,
                 null,
                 null
            from t_report_abonent t
            join t_stavka s
              on s.id = t.kod_stavka
          --and s.is_remote_sales = 0
            left join t_report_abon_ords ord
              on ord.RP_ID = t.rp_id
             and ord.INTERNAL_ID = t.ab_id
           where t.rp_id = pi_rp_id
             and t.type_ab = 2
             and ord.RP_ID is null
           group by t.kod_stavka, t.dates_ap --, s.pay_number_months
          union
          select pi_rp_id,
                 12,
                 t.dates_ap dates, -- Дата периода
                 a.addr_id seg_id, -- ИД адреса
                 org.org_name as name_model, --
                 a.addr_city || ' ' || a.addr_street || ' ' ||
                 a.addr_building seg_tar_list, --Город Улица
                 null kol, -- Колич. прод. подкл. в периоде
                 0 kol_act, -- Колич активных из проданных в периоде
                 null stavka_Proc, -- Ставка с учетом процентов
                 t.stavka cost, -- Ставка действующая в периоде
                 t.sum sum, -- Сумма вознаграждения
                 t.pr_nds pr_nds, -- Ставка НДС
                 t.sumwnds sumwnds, --Сумма с НДС
                 11 premia_schema, -- Схема вознаграждения действующая в тек. отч. периоде.
                 t.forsum ForSum,
                 t.kod_stavka Kod_Stavka,
                 null,
                 null
            from t_report_abonent t
            join t_organizations org
              on org.org_id = t.ab_id
            join t_address a
              on a.addr_id = org.adr2_id
            join t_stavka s
              on s.id = t.kod_stavka
           where t.rp_id = pi_rp_id
             and t.type_ab = 4
          union
          select pi_rp_id,
                 14,
                 t.dates_ap dates, -- Дата периода
                 t.tar_id seg_id, -- ИД сегмента
                 null as name_model, -- Название организации
                 null seg_tar_list, -- Название сегмента
                 count(t.ab_id) kol, -- Колич. прод. подкл. в периоде
                 0 kol_act, -- Колич активных из проданных в периоде
                 max(t.stavka_proc) stavka_Proc, -- Ставка с учетом процентов
                 (case
                   when nvl(max(t.stavka_proc), 0) = 0 then
                    max(t.stavka)
                   else
                    sum(t.stavka)
                 end) cost, -- Ставка действующая в периоде
                 sum(t.sum) sum, -- Сумма вознаграждения
                 max(t.pr_nds) pr_nds, -- Ставка НДС
                 sum(t.sumwnds) sumwnds, --Сумма с НДС
                 11 premia_schema, -- Схема вознаграждения действующая в тек. отч. периоде.
                 sum(t.forsum) ForSum,
                 t.kod_stavka Kod_Stavka,
                 null,
                 null
            from t_report_abonent t
            join t_stavka s
              on s.id = t.kod_stavka
           where t.rp_id = pi_rp_id
             and t.type_ab = 6
           group by t.kod_stavka, t.dates_ap, t.tar_id
          union
          select pi_rp_id,
                 13,
                 t.dates_ap    dates, -- Дата периода
                 null          seg_id, -- ИД адреса
                 null          as name_model, --
                 null          seg_tar_list, --Город Улица
                 null          kol, -- Колич. прод. подкл. в периоде
                 0             kol_act, -- Колич активных из проданных в периоде
                 t.stavka_proc stavka_Proc, -- Ставка с учетом процентов
                 t.stavka      cost, -- Ставка действующая в периоде
                 t.sum         sum, -- Сумма вознаграждения
                 t.pr_nds      pr_nds, -- Ставка НДС
                 t.sumwnds     sumwnds, --Сумма с НДС
                 11            premia_schema, -- Схема вознаграждения действующая в тек. отч. периоде.
                 t.forsum      ForSum,
                 t.kod_stavka  Kod_Stavka,
                 t.ab_id,
                 null
            from t_report_abonent t
            join t_stavka s
              on s.id = t.kod_stavka
           where t.rp_id = pi_rp_id
             and t.type_ab = 5
          union
          select pi_rp_id,
                 15,
                 t.dates_ap dates, -- Дата периода
                 t.tar_id seg_id, -- ИД сегмента
                 null as name_model, -- Название организации
                 null seg_tar_list, -- Название сегмента
                 count(t.ab_id) kol, -- Колич. прод. подкл. в периоде
                 0 kol_act, -- Колич активных из проданных в периоде
                 max(t.stavka_proc) stavka_Proc, -- Ставка с учетом процентов
                 (case
                   when nvl(max(t.stavka_proc), 0) = 0 then
                    max(t.stavka)
                   else
                    sum(t.stavka)
                 end) cost, -- Ставка действующая в периоде
                 sum(t.sum) sum, -- Сумма вознаграждения
                 max(t.pr_nds) pr_nds, -- Ставка НДС
                 sum(t.sumwnds) sumwnds, --Сумма с НДС
                 11 premia_schema, -- Схема вознаграждения действующая в тек. отч. периоде.
                 sum(t.forsum) ForSum,
                 t.kod_stavka Kod_Stavka,
                 null,
                 null
            from t_report_abonent t
            join t_stavka s
              on s.id = t.kod_stavka
           where t.rp_id = pi_rp_id
             and t.type_ab = 7
           group by t.kod_stavka, t.dates_ap, t.tar_id
          union
          select pi_rp_id,
                 16,
                 t.dates_ap dates, -- Дата периода
                 count(t.tar_id) seg_id, -- ИД сегмента
                 t.comments as name_model, -- Название организации
                 null seg_tar_list, -- Название сегмента
                 count(t.ab_id) kol, -- Колич. прод. подкл. в периоде
                 0 kol_act, -- Колич активных из проданных в периоде
                 /*max*/(t.stavka_proc) stavka_Proc, -- Ставка с учетом процентов
                 (case
                   when nvl(/*max*/(t.stavka_proc), 0) = 0 then
                    /*max*/(t.stavka)
                   else
                    /*sum*/(t.stavka)
                 end) cost, -- Ставка действующая в периоде
                 sum(t.sum) sum, -- Сумма вознаграждения
                 max(t.pr_nds) pr_nds, -- Ставка НДС
                 sum(t.sumwnds) sumwnds, --Сумма с НДС
                 11 premia_schema, -- Схема вознаграждения действующая в тек. отч. периоде.
                 sum(t.forsum) ForSum,
                 t.kod_stavka Kod_Stavka,
                 null,
                 null
            from t_report_abonent t
            join t_stavka s
              on s.id = t.kod_stavka
           where t.rp_id = pi_rp_id
             and t.type_ab = 8
           group by t.kod_stavka, t.dates_ap, /*t.tar_id,*/ t.comments, t.stavka_proc, t.stavka
          union
          select pi_rp_id,
                 17,
                 t.dates_ap dates, -- Дата периода
                 t.tar_id seg_id, -- ИД сегмента
                 t.comments as name_model, -- Название организации
                 null seg_tar_list, -- Название сегмента
                 count(t.ab_id) kol, -- Колич. прод. подкл. в периоде
                 0 kol_act, -- Колич активных из проданных в периоде
                 max(t.stavka_proc) stavka_Proc, -- Ставка с учетом процентов
                 /*(case
                 when nvl(max(t.stavka_proc), 0) = 0 then*/
                 /*else
                    sum(t.stavka)
                 end)*/
                 max(t.stavka) cost, -- Ставка действующая в периоде
                 sum(t.sum) sum, -- Сумма вознаграждения
                 max(t.pr_nds) pr_nds, -- Ставка НДС
                 sum(t.sumwnds) sumwnds, --Сумма с НДС
                 11 premia_schema, -- Схема вознаграждения действующая в тек. отч. периоде.
                 sum(t.forsum) ForSum,
                 t.kod_stavka Kod_Stavka,
                 null,
                 null
            from t_report_abonent t
            join t_stavka s
              on s.id = t.kod_stavka
           where t.rp_id = pi_rp_id
             and t.type_ab = 9
           group by t.kod_stavka, t.dates_ap, t.tar_id, t.comments;
        i := 1;
        for ora in (select t.dates,
                           t.kod_stavka,
                           t.seg_name,
                           t.seg_tar_list,
                           t.stavka
                      from t_report_ap t
                     where t.rp_id = pi_rp_id
                     order by t.dates, t.kod_stavka) loop
          update t_report_ap t
             set t.row_number = i
           where t.rp_id = pi_rp_id
             and t.kod_stavka = ora.kod_stavka
             and t.dates = ora.dates
             and nvl(t.seg_name, 0) = nvl(ora.seg_name, 0)
             and nvl(t.seg_tar_list, 0) = nvl(ora.seg_tar_list, 0)
             and nvl(t.stavka, 0) = nvl(ora.stavka, 0);
          i := i + 1;
        end loop;
      end if;
    end if;
    --берем данные из кэша
    /*Значения type_vozn:
    ACCOUNT_REPORT_PERIOD(1, "вознаграждение Агента по итогам отчетного периода"),
    BONUS_BRANDED(2, "вознаграждение Агента за продажу брендированного оборудования"),
    BONUS_USB(3, "вознаграждение Агента за продажу USB-модемов"),
    BONUS_CITY_NUMBER(4, "вознаграждение Агента за продажу прямого городского номера"),
    BONUS_EXCESS_PAY(5, "вознаграждение Агента за платежи совершенные абонентом сверх прейскуранта"),
    BONUS_ACTIVE(6, "вознаграждение Агента за активных абонентов"),
    BONUS_PLANE(7, "вознаграждение Агента за выполнение плана"),
    BONUS_ABONENT_SRVICE(8, "вознаграждение Агента за абонентское обслуживание"),
    BONUS_ADSL(9, "вознаграждение Агента за ADSL"),
    BONUS_PLANE_ADSL(11, "вознаграждение Агента за выполнение плана"),
    BONUS_SALE_POINT(12, "вознаграждение Агента за организацию и содержание монобрендовой точки продаж")*/
    open res for
      select t.dates,
             t.seg_id,
             t.seg_name,
             t.seg_tar_list,
             NVL(t.kol, 0) kol,
             NVL(t.kol_act, 0) kol_act,
             NVL(t.stavka_proc, 0) stavka_proc,
             NVL(t.stavka, 0) stavka,
             NVL(t.sum, 0) sum,
             NVL(t.pr_nds, 0) pr_nds,
             NVL(t.sumwnds, 0) sumwnds,
             t.premia_schema,
             NVL(t.forsum, 0) forsum,
             (case
               when t.kod_stavka_old is null then
                t.kod_stavka
               else
                t.kod_stavka_old
             end) as kod_stavka,
             (case
               when l_Date_S >= to_date('01.05.2012', 'dd.mm.yyyy') then
                (case
                  when t.type_schema = 12 then
                   12
                  when t.type_schema = 6 and (nvl(ts.amount_bottom_tar, 0) <> 0 or
                       nvl(ts.amount_top_tar, 0) <> 0) then
                   11 --адсл (не за выполнение плана)
                  when t.type_schema in (6, 13) then
                   9 --адсл с выполнением плана
                  when t.type_schema in (14) then
                   13 --
                  when t.type_schema = 10 then
                   10 --дист.продажи
                  when t.type_schema = 8 then
                   8 --абонентка
                /*when ts.pay_number_months = 3 and
                    (nvl(ts.amount_bottom_tar, 0) <> 0 or
                    nvl(ts.amount_top_tar, 0) <> 0) then
                6 --п.2 вроде как за активных*/
                  when ts.pay_number_months > 1 /*and t.dates <> l_Date_S*/ 
                   then
                   1 --длительная процентная ставка(в п.1)
                  when ts.optional_equipment = 7003 then
                   2 --брендированное оборудование
                  when ts.optional_equipment = 4 then
                   3 --усб модем
                  when ts.optional_equipment = 9002 then
                   4 --городской телефон
                  when nvl(ts.over_pay_top, 0) <> 0 then
                   5 --сверхплатежи(п.5)
                  when ts.pay_number_months = 1 and
                       nvl(ts.amount_bottom_tar, 0) = 0 and
                       nvl(ts.amount_top_tar, 0) = 0 then
                   6 --п.2 вроде как за активных
                  else
                   7 --п.5
                end)
               else
               --для старых отчетов, которые считались не исходя t_stavka
                (case
                  when t.type_schema = 6 then
                   9 --адсл
                  when t.type_schema = 10 then
                   10 --дист.продажи
                  when t.type_schema = 8 then
                   8 --абонентка
                  when t.kod_stavka = 1457 then
                   5
                  when t.kod_stavka = 1420 and t.dates <> l_Date_S then
                   1
                  when t.kod_stavka = 1420 then
                   7
                  when t.kod_stavka = 1459 then
                   2
                  else
                   6
                end)
             end) as type_vozn,
             t.formula,
             ROUND((decode(t.kol, 0, 0, NVL(t.stavka, 0) / t.kol)) / 1.18, 2) stavka_ed,
             type_schema
        from t_report_ap t
        left join t_stavka ts
          on ts.id = t.kod_stavka
       where t.rp_id = pi_rp_id
       order by t.row_number, t.dates, t.kod_stavka;
    Po_Formula := 'S = ';
    return res;
  Exception
    when Others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  End GetAgentPoruchenie_Test;
  --------------------------------------------------------------------------
  -- Отчёт об активности абонентов по дереву организации за промежуток времени
  function AbonentActivityFull(pi_org_id       in array_num_2,
                               pi_block        in number,
                               pi_org_relation in num_tab,
                               pi_date_beg     in date,
                               pi_date_end     in date,
                               po_err_num      out pls_integer,
                               po_err_msg      out t_Err_Msg)
    return sys_refcursor is
    res           sys_refcursor;
    l_month_count number;
    l_month_name  varchar2(15); -- название месяца
    l_sql_text    varchar2(20000);
    l_date_beg    date := trunc(pi_date_beg, 'MM');
    l_date_end    date := trunc(add_months(pi_date_end, 1), 'mm') - 1;
    l_org_tab     num_tab := num_tab();
  begin
    l_org_tab  := get_orgs_tab_for_multiset(pi_orgs         => pi_org_id,
                                            Pi_worker_id    => 777,
                                            pi_block        => pi_block,
                                            pi_org_relation => pi_org_relation,
                                            pi_is_rtmob     => 1);
    l_sql_text := '
      declare
      l_date_beg date;
      l_date_end date;
      org_tab   num_tab := num_tab();
      l_res      sys_refcursor;
      begin
        org_tab :=:exp1;
        l_date_beg :=:exp2;
        l_date_end :=:exp3;
        l_res:=:exp4;
        open l_res for
        ';
    l_sql_text := l_sql_text || 'Select distinct orgP.org_name org_nameP,
       orgP.org_id org_idP,
                                t_res.org_name,
                                t_res.org_id
                               ';
    -- кол-во полных месяцев между начальной и конечной датой
    l_month_count := round(months_between(l_date_end, l_date_beg), 0);

    for i in 1 .. l_month_count loop
      -- получаем нормальное название месяца (буквенное)
      select rtrim(to_char(add_months(pi_date_beg, i - 1), 'Month')) || '_' ||
             ltrim(to_char(add_months(pi_date_beg, i - 1), 'YYYY'))
        into l_month_name
        from dual;
      -- формируем столбцы по месяцам
      l_sql_text := l_sql_text || ',sum(t_res.' || l_month_name || ') ' ||
                    l_month_name;
    end loop;

    l_sql_text := l_sql_text || ' from (';
    l_sql_text := l_sql_text || 'select org.org_name, org.org_id--,orgP.org_name org_nameP, orgP.org_id org_idP
                                ';
    for i in 1 .. l_month_count loop
      -- получаем нормальное название месяца (буквенное)
      select rtrim(to_char(add_months(pi_date_beg, i - 1), 'Month')) || '_' ||
             ltrim(to_char(add_months(pi_date_beg, i - 1), 'YYYY'))
        into l_month_name
        from dual;
      -- формируем столбцы по месяцам
      l_sql_text := l_sql_text ||
                    ', nvl(/*sum*/(decode(trunc(t_active.dates),
                                    add_months(trunc(l_date_beg), ' ||
                    to_char(i - 1) ||
                    '), round(sum(t_active.count_active)/GREATEST(sum(t_conn.count_podkluch),1), 4)*100)),
                                    0) ' || l_month_name;
    end loop;
    l_sql_text := l_sql_text || '   from (select t.qwe,--t.org_pid, --t.org_id, trp1.id_org,
                       trp1.dates,--,
                       trp1.id,
                       sum(tra.amount) COUNT_ACTIVE
                  from (
                  select column_value org_id,
                              column_value qwe
                               from table(org_tab)
                  ) t
                  left join t_report_period trp1 on trp1.id_org = t.org_id
                     and trp1.is_actual=1
                  left join t_report_activ tra on tra.id_repper_cur = trp1.id
                                               and tra.id_repper in (select t.id
                                                                       from t_report_period t
                                                                      where t.dates < trp1.dates
                                                                        and t.is_actual=1
                                                                        and t.dates >= add_months(trp1.dates, -4)
                                                                        and t.id_org = trp1.id_org)
                                               and tra.ci = 1
                 where  trp1.dates >= l_date_beg
                   and trp1.datepo <= l_date_end
                 group by t.qwe--,t.org_pid
                          ,trp1.dates,
                          trp1.id) t_active
          join (select trp.dates,
                       trp.id,
                       torg.qwe,
                       /*sum(*/(select count(*)
                              from t_report_period t
                              join t_report_real_conn trc on trc.id_repper = t.id
                                                           and trc.ci = 1
                             where t.id_org = trp.id_org
                               and t.is_actual=1
                               and t.dates < trp.dates
                               and t.dates >= add_months(trp.dates, -4))/*)*/ COUNT_PODKLUCH
                  from (select column_value org_id,
                              column_value qwe
                               from table(org_tab)) torg
                  left join t_report_period trp on trp.id_org = torg.org_id
                       and trp.is_actual=1
                 where trp.dates >= l_date_beg
                   and trp.datepo <= l_date_end
                   ) t_conn on t_conn.id = t_active.id
          join t_organizations org on org.org_id = t_active.qwe
         group by org.org_name,
                  org.org_id,
                  t_active.dates
         order by org.org_name) t_res
         left join t_organizations orgX
           on orgX.Org_Id = t_res.org_id
         left join t_organizations orgP
           on orgP.Org_Id = OrgX.root_org_id2
         group by t_res.org_name,
                                t_res.org_id,orgP.org_name,
                                orgP.org_id;
         end;';
    dump(l_sql_text);
    execute immediate l_sql_text
      using l_org_tab, l_date_beg, l_date_end, res;
    return res;
  Exception
    when Others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end AbonentActivityFull;
  --------------------------------------------------------------------------
  -- возвращает акт сверки
  function Get_Act_Revision(pi_org_pid   in t_organizations.org_id%type /*28.12.09*/,
                            pi_org_id    in t_organizations.org_id%type /*28.12.09*/,
                            pi_date_from in date,
                            pi_date_to   in date,
                            pi_dog_id    in t_dogovor.dog_id%type,
                            po_err_num   out pls_integer,
                            po_err_msg   out varchar2) return sys_refcursor is
    res       sys_refcursor;
    l_dog_id  number := pi_dog_id;
    l_org_id  number := pi_org_id;
    l_org_pid number := pi_org_pid;
  begin
    if (pi_org_id is null or pi_org_pid is null) then
      orgs.Get_Orgs_By_Dog(l_dog_id, l_org_id, l_orG_pid);
    end if;
    open res for
      Select (0) dog_id,
             (0) ord_fld,
             (0) num_fld,
             (NULL) type_fld,
             'Входящее сальдо:' descr_fld,
             (NULL) dv_id,
             (NULL) rep_id,
             (NULL) data_opl,
             (NULL) debet,
             sum(sum_nagr) kredit
        from (select dog_id,
                     (nvl(acc_operations.get_acc_saldo2(dog_id,
                                                        pi_date_from,
                                                        5001),
                          0) + nvl(acc_operations.get_acc_saldo2(dog_id,
                                                                  pi_date_from,
                                                                  5004),
                                    0) + nvl(acc_operations.get_acc_saldo2(dog_id,
                                                                            pi_date_from,
                                                                            5006),
                                              0) +
                     nvl(acc_operations.get_acc_saldo2(dog_id,
                                                        pi_date_from,
                                                        5007),
                          0) + nvl(acc_operations.get_acc_saldo2(dog_id,
                                                                  pi_date_from,
                                                                  5008),
                                    0) + nvl(acc_operations.get_acc_saldo2(dog_id,
                                                                            pi_date_from,
                                                                            5009),
                                              0)) sum_nagr
                from (select dog.dog_id
                        from t_Org_Relations tor
                        join t_dogovor dog
                          on tor.id = dog.org_rel_id
                        join t_Report_Period rp
                          on rp.dog_id = dog.dog_id
                         and rp.is_actual = 1
                       where tor.org_pid = l_org_pid
                         and tor.org_id = l_org_id
                         and (dog.dog_id = l_dog_id or l_dog_id is null)
                       group by dog.dog_id)
              union
              select dog_id, sum(sum_nagr) - sum(amount) sum_nagr
                from (select dog.dog_id,
                             (1) ord_fld,
                             (NULL) dv_id,
                             rp.id rep_id,
                             rp.datepo data_opl,
                             (null) amount,
                             nagr.sum_nagr
                        from t_Org_Relations tor
                        join t_dogovor dog
                          on tor.id = dog.org_rel_id
                        join t_Report_Period rp
                          on rp.dog_id = dog.dog_id
                         and rp.is_actual = 1
                        left join (select rp.dog_id,
                                         rp.id,
                                         sum(sf.summa) sum_nagr
                                    from t_Report_Period rp
                                    left join t_report_sf sf
                                      on sf.id_repper = rp.id
                                   where rp.is_actual = 1
                                   group by rp.dog_id, rp.id) nagr
                          on nagr.id = rp.id
                         and nagr.dog_id = dog.dog_id
                       where tor.org_pid = l_org_pid
                         and tor.org_id = l_org_id
                         and (dog.dog_id = l_dog_id or l_dog_id is null)
                      union
                      select dog.dog_id,
                             (1) ord_fld,
                             (null) dv_id,
                             rp.id rep_id,
                             ro.data_opl,
                             ro.amount,
                             (null) nagr
                        from t_Org_Relations tor
                        join t_dogovor dog
                          on tor.id = dog.org_rel_id
                        join t_Report_Period rp
                          on rp.dog_id = dog.dog_id
                         and rp.is_actual = 1
                        left join t_Report_Oplata ro
                          on ro.id_repper = rp.id
                       where tor.org_pid = l_org_pid
                         and tor.org_id = l_org_id
                         and (dog.dog_id = l_dog_id or l_dog_id is null))
               where data_opl < pi_date_from
               group by dog_id)
      union
      select rep.dog_id,
             rep.ord_fld,
             (case
               when rep.ord_fld = 2 and rep.amount is null then
                1
               when rep.ord_fld = 4 and rep.sum_nagr is null then
                1
               else
                0
             end) num_fld,
             (case
               when rep.ord_fld = 1 and rep.type_fld is null then
                'Агентское вознаграждение по договору ' || dog.dog_number ||
                ' от ' || dog.dog_date || ' (руб)'
               when rep.ord_fld = 1 and rep.type_fld = 1 then
                'Вознагражд по договору комиссии ' || dog.dog_number ||
                ' от ' || dog.dog_date || ' (руб)'
               when rep.ord_fld = 2 and rep.sum_nagr is null then
                'Выписка'
               when rep.ord_fld = 2 and rep.amount is null then
                'Усл.стор.орг.(прочие)'
               when rep.ord_fld = 3 and rep.type_fld is null then
                'Обеспечительный платеж по договору ' || dog.dog_number ||
                ' от ' || dog.dog_date || ' (руб)'
               when rep.ord_fld = 3 and rep.type_fld = 1 then
                'Договор комиссии ' || dog.dog_number || ' от ' ||
                dog.dog_date || ' (руб)'
               when rep.ord_fld = 4 and rep.sum_nagr is null then
                'Операция'
               when rep.ord_fld = 4 and rep.amount is null then
                'Выписка'
               else
                NULL
             end) type_fld,
             (case
               when rep.ord_fld = 1 or rep.ord_fld = 3 then
                'Входящее сальдо:'
               when rep.ord_fld = 2 and rep.sum_nagr is null and
                    rep.type_fld is null then
                'Оплата Агентск.вознагражд.за ' ||
                trim(to_char(add_months(rep.data_opl, -1),
                             'month',
                             'NLS_DATE_LANGUAGE = RUSSIAN')) || ' ' ||
                trim(to_char(rep.data_opl, 'yyyy')) ||
                decode(rep.pr_nds,
                       0,
                       '',
                       ' ,В том числе НДС ' ||
                       trunc((rep.amount / (1 + rep.pr_nds) * rep.pr_nds), 2))
               when rep.ord_fld = 2 and rep.sum_nagr is null and
                    rep.type_fld = 1 then
                'Оплата Вознагражд. по договору комиссии за ' ||
                trim(to_char(add_months(rep.data_opl, -1),
                             'month',
                             'NLS_DATE_LANGUAGE = RUSSIAN')) || ' ' ||
                trim(to_char(rep.data_opl, 'yyyy')) ||
                decode(rep.pr_nds,
                       0,
                       '',
                       ' ,В том числе НДС ' ||
                       trunc((rep.amount / (1 + rep.pr_nds) * rep.pr_nds), 2))
               when rep.ord_fld = 2 and rep.amount is null and
                    rep.type_fld is null then
                'Агентское вознаграждение за ' ||
                trim(to_char(rep.data_opl,
                             'month',
                             'NLS_DATE_LANGUAGE = RUSSIAN')) || ' ' ||
                trim(to_char(rep.data_opl, 'yyyy'))
               when rep.ord_fld = 2 and rep.amount is null and
                    rep.type_fld = 1 then
                'Вознаграждение по дог. комисси за ' ||
                trim(to_char(rep.data_opl,
                             'month',
                             'NLS_DATE_LANGUAGE = RUSSIAN')) || ' ' ||
                trim(to_char(rep.data_opl, 'yyyy'))
               when rep.ord_fld = 4 and rep.sum_nagr is null then
                'Отчет по подключениям за ' ||
                trim(to_char(rep.data_opl,
                             'month',
                             'NLS_DATE_LANGUAGE = RUSSIAN')) || ' ' ||
                trim(to_char(rep.data_opl, 'yyyy'))
               when rep.ord_fld = 4 and rep.amount is null then
                'Обесп.платеж по агентск.договору'
               else
                NULL
             end) descr_fld,
             rep.dv_id,
             rep.rep_id,
             rep.data_opl,
             rep.amount debet,
             rep.sum_nagr kredit
        from (select dog_id,
                     ord_fld,
                     type_fld,
                     (NULL) pr_nds,
                     (NULL) dv_id,
                     (NULL) rep_id,
                     (NULL) data_opl,
                     sum(amount) amount,
                     sum(sum_nagr) sum_nagr
                from (select dog.dog_id,
                             (1) ord_fld,
                             decode(nvl(prm.cnt, 0), 0, NULL, 1) type_fld,
                             (NULL) dv_id,
                             rp.id rep_id,
                             rp.datepo data_opl,
                             (null) amount,
                             nagr.sum_nagr
                        from t_Org_Relations tor
                        join t_dogovor dog
                          on tor.id = dog.org_rel_id
                        join t_Report_Period rp
                          on rp.dog_id = dog.dog_id
                         and rp.is_actual = 1
                        left join (select rp.dog_id,
                                         rp.id,
                                         sum(sf.summa) sum_nagr
                                    from t_Report_Period rp
                                    left join t_report_sf sf
                                      on sf.id_repper = rp.id
                                   where rp.is_actual = 1
                                   group by rp.dog_id, rp.id) nagr
                          on nagr.id = rp.id
                         and nagr.dog_id = dog.dog_id
                        left join (Select t.dog_id, count(*) cnt
                                    from t_dogovor t
                                    join t_dogovor_prm dp
                                      on dp.dp_dog_id = t.dog_id
                                   Where dp.dp_prm_id = 2003
                                     and (t.dog_id = l_dog_id or
                                         l_dog_id is null)
                                   group by t.dog_id) prm
                          on prm.dog_id = dog.dog_id
                         and ((prm.cnt >= 1) or prm.cnt = 0)
                       where tor.org_pid = l_org_pid
                         and tor.org_id = l_org_id
                         and (dog.dog_id = l_dog_id or l_dog_id is null)
                      union
                      select dog.dog_id,
                             (1) ord_fld,
                             decode(nvl(prm.cnt, 0), 0, NULL, 1) type_fld,
                             (null) dv_id,
                             rp.id rep_id,
                             ro.data_opl,
                             ro.amount,
                             (null) nagr
                        from t_Org_Relations tor
                        join t_dogovor dog
                          on tor.id = dog.org_rel_id
                        join t_Report_Period rp
                          on rp.dog_id = dog.dog_id
                         and rp.is_actual = 1
                        left join t_Report_Oplata ro
                          on ro.id_repper = rp.id
                        left join (Select t.dog_id, count(*) cnt
                                     from t_dogovor t
                                     join t_dogovor_prm dp
                                       on dp.dp_dog_id = t.dog_id
                                    Where dp.dp_prm_id = 2003
                                      and (t.dog_id = l_dog_id or
                                          l_dog_id is null)
                                    group by t.dog_id) prm
                          on prm.dog_id = dog.dog_id
                         and ((prm.cnt >= 1) or prm.cnt = 0)
                       where tor.org_pid = l_org_pid
                         and tor.org_id = l_org_id
                         and (dog.dog_id = l_dog_id or l_dog_id is null))
               where data_opl < trunc(pi_date_from)
               group by dog_id, ord_fld, type_fld
              union
              select dog_id,
                     ord_fld,
                     type_fld,
                     pr_nds,
                     (NULL) dv_id,
                     rep_id,
                     data_opl,
                     amount,
                     sum_nagr
                from (select dog.dog_id,
                             (2) ord_fld,
                             decode(nvl(prm.cnt, 0), 0, NULL, 1) type_fld,
                             (NULL) pr_nds,
                             (NULL) dv_id,
                             rp.id rep_id,
                             rp.datepo data_opl,
                             (null) amount,
                             nagr.sum_nagr
                        from t_Org_Relations tor
                        join t_dogovor dog
                          on tor.id = dog.org_rel_id
                        join t_Report_Period rp
                          on rp.dog_id = dog.dog_id
                         and rp.is_actual = 1
                        left join (select rp.dog_id,
                                         rp.id,
                                         sum(sf.summa) sum_nagr
                                    from t_Report_Period rp
                                    left join t_report_sf sf
                                      on sf.id_repper = rp.id
                                   where rp.is_actual = 1
                                   group by rp.dog_id, rp.id) nagr
                          on nagr.id = rp.id
                         and nagr.dog_id = dog.dog_id
                        left join (Select t.dog_id, count(*) cnt
                                    from t_dogovor t
                                    join t_dogovor_prm dp
                                      on dp.dp_dog_id = t.dog_id
                                   Where dp.dp_prm_id = 2003
                                     and (t.dog_id = l_dog_id or
                                         l_dog_id is null)
                                   group by t.dog_id) prm
                          on prm.dog_id = dog.dog_id
                         and ((prm.cnt >= 1) or prm.cnt = 0)
                       where tor.org_pid = l_org_pid
                         and tor.org_id = l_org_id
                         and (dog.dog_id = l_dog_id or l_dog_id is null)
                      union
                      select dog.dog_id,
                             (2) ord_fld,
                             decode(nvl(prm.cnt, 0), 0, NULL, 1) type_fld,
                             rp.pr_nds,
                             (null) dv_id,
                             rp.id rep_id,
                             ro.data_opl,
                             ro.amount,
                             (null) nagr
                        from t_Org_Relations tor
                        join t_dogovor dog
                          on tor.id = dog.org_rel_id
                        join t_Report_Period rp
                          on rp.dog_id = dog.dog_id
                         and rp.is_actual = 1
                        left join t_Report_Oplata ro
                          on ro.id_repper = rp.id
                        left join (Select t.dog_id, count(*) cnt
                                     from t_dogovor t
                                     join t_dogovor_prm dp
                                       on dp.dp_dog_id = t.dog_id
                                    Where dp.dp_prm_id = 2003
                                    group by t.dog_id) prm
                          on prm.dog_id = dog.dog_id
                         and ((prm.cnt >= 1) or prm.cnt = 0)
                       where tor.org_pid = l_org_pid
                         and tor.org_id = l_org_id
                         and (dog.dog_id = l_dog_id or l_dog_id is null))
               where data_opl >= trunc(pi_date_from)
                 and data_opl <= trunc(pi_date_to)
              union
              select dog_id,
                     (3) ord_fld,
                     type_fld,
                     (NULL) pr_nds,
                     (NULL) dv_id,
                     (NULL) rep_id,
                     (NULL) data_opl,
                     (NULL) amount,
                     (nvl(acc_operations.get_acc_saldo2(dog_id,
                                                        pi_date_from,
                                                        5001),
                          0) + nvl(acc_operations.get_acc_saldo2(dog_id,
                                                                  pi_date_from,
                                                                  5004),
                                    0) + nvl(acc_operations.get_acc_saldo2(dog_id,
                                                                            pi_date_from,
                                                                            5006),
                                              0) +
                     nvl(acc_operations.get_acc_saldo2(dog_id,
                                                        pi_date_from,
                                                        5007),
                          0) + nvl(acc_operations.get_acc_saldo2(dog_id,
                                                                  pi_date_from,
                                                                  5008),
                                    0) + nvl(acc_operations.get_acc_saldo2(dog_id,
                                                                            pi_date_from,
                                                                            5009),
                                              0)) sum_nagr
                from (select dog.dog_id,
                             decode(nvl(prm.cnt, 0), 0, NULL, 1) type_fld
                        from t_Org_Relations tor
                        join t_dogovor dog
                          on tor.id = dog.org_rel_id
                        join t_Report_Period rp
                          on rp.dog_id = dog.dog_id
                         and rp.is_actual = 1
                        left join (Select t.dog_id, count(*) cnt
                                    from t_dogovor t
                                    join t_dogovor_prm dp
                                      on dp.dp_dog_id = t.dog_id
                                   Where dp.dp_prm_id = 2003
                                     and (t.dog_id = l_dog_id or
                                         l_dog_id is null)
                                   group by t.dog_id) prm
                          on prm.dog_id = dog.dog_id
                         and ((prm.cnt >= 1) or prm.cnt = 0)
                       where tor.org_pid = l_org_pid
                         and tor.org_id = l_org_id
                         and (dog.dog_id = l_dog_id or l_dog_id is null)
                       group by dog.dog_id,
                                decode(nvl(prm.cnt, 0), 0, NULL, 1))
              union
              select dog.dog_id,
                     (4) ord_fld,
                     decode(nvl(prm.cnt, 0), 0, NULL, 1) type_fld,
                     (NULL) pr_nds,
                     Max(dv.dv_id),
                     (NULL) rep_id,
                     tap.pay_date data_opl,
                     Sum((Case
                           When dv_id in (6007, 6020, 6021) then
                            abs(tap.amount)
                         end)) amount,
                     Sum(decode(dv_id, 6000, abs(tap.amount), NULL)) sum_nagr
                from t_Org_Relations tor
                join t_dogovor dog
                  on tor.id = dog.org_rel_id
                join t_acc_owner tao
                  on tao.owner_ctx_id = dog.dog_id
                join t_accounts acc
                  on acc.acc_owner_id = tao.owner_id
                Join t_acc_pay tap
                  on tap.acc_id = acc.acc_id
                join t_dic_values dv
                  on dv.dv_id = tap.tr_type
                left join (Select t.dog_id, count(*) cnt
                             from t_dogovor t
                             join t_dogovor_prm dp
                               on dp.dp_dog_id = t.dog_id
                            Where dp.dp_prm_id = 2003
                              and (t.dog_id = l_dog_id or l_dog_id is null)
                            group by t.dog_id) prm
                  on prm.dog_id = dog.dog_id
                 and ((prm.cnt >= 1) or prm.cnt = 0)
               where tor.org_pid = l_org_pid
                 and tor.org_id = l_org_id
                 and (dog.dog_id = l_dog_id or l_dog_id is null)
                 and tap.tr_type in (6007, 6020, 6021, 6000)
                 and tap.pay_date >= trunc(pi_date_from)
                 and tap.pay_date <= trunc(pi_date_to)
               Group by dog.dog_id,
                        dv.dv_id,
                        decode(nvl(prm.cnt, 0), 0, NULL, 1),
                        tap.pay_date) rep,
             t_dogovor dog
       where (rep.dog_id = l_dog_id or l_org_id is null)
         and rep.dog_id = dog.dog_id
         and (rep.sum_nagr > 0 or rep.amount > 0)
       order by dog_id, ord_fld, rep_id, data_opl;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Act_Revision;
  ----------------------------------------------------------------------------
  -- заполняет табличку t_report_sf при закрытии отчётного периода
  procedure Save_Data_SF(pi_rp_id       in number,
                         pi_number_sf   in varchar, -- номер счёт-фактуры
                         pi_date_sf     in date, -- дата счёт-фактуры
                         pi_org_id_sel  in number, -- продавец
                         pi_org_id_send in number, -- грузоотрправитель
                         pi_org_id_buy  in number, --покупатель
                         pi_flag_corr   in number, -- для рассчёта суммы СФ
                         pi_worker_id   in T_USERS.USR_ID%type,
                         po_err_num     out pls_integer,
                         po_err_msg     out t_Err_Msg) is
    l_dog_id     number;
    l_dog_date   date;
    l_dog_number varchar2(255);
    -- продавец
    l_name_org_sel varchar2(255) := null;
    l_adress_sel   varchar2(255) := null;
    l_kpp_sel      varchar2(64) := null;
    l_inn_sel      number := null;
    -- грузоотправитель
    l_name_org_send varchar2(255) := null;
    l_adress_send   varchar2(255) := null;
    -- покупатель
    l_name_org_buy    varchar2(255) := null;
    l_adress_buy      varchar2(255) := null;
    l_kpp_buy         varchar2(64) := null;
    l_inn_buy         number := null;
    l_sum             number;
    l_sum_nds         number;
    l_sum_with_nds    number;
    l_ogrn_sel        varchar2(64) := null;
    l_num_zaiav_opl   number;
    l_DT_ZAIAV_OPL    date;
    l_v_lice_pr       varchar2(255); -- в лице (принципала)
    l_na_osnovanii_pr varchar2(255); -- на основании (принципала)
    l_v_lice_ag       varchar2(255); -- в лице (агента)
    l_na_osnovanii_ag varchar2(255); -- на основании (агента)
    l_region_id       number; -- id региона
    l_date_s          date;
    l_date_po         date;
    l_sf_id           number; -- id счёт-фактуры
    l_org_buy         varchar2(255);
  begin
    select Max(sf.id)
      into l_sf_id
      from t_report_sf sf
     where sf.id_repper = pi_rp_id;
    -- если счёт-фактура с указанными параметрами уже существует, удаляем её и считаем заново
    if NVL(l_sf_id, 0) > 0 then
      delete from t_report_sf sf where sf.id_repper = pi_rp_id;
    end if;
    -- id договора
    select max(t.dog_id)
      into l_dog_id
      from t_report_period t
     where t.id = pi_rp_id
       and t.is_actual = 1;
    -- информация по договору
    select td.dog_date, td.dog_number
      into l_dog_date, l_dog_number
      from t_dogovor td
     where td.dog_id = l_dog_id;
    -- продавец
    if pi_org_id_sel is not null then
      select org.org_name,
             org.org_kpp,
             org.org_inn,
             nvl(ta.addr_index || ', ', '') ||
             nvl(ta.addr_country || ', ', '') ||
             nvl(ta.addr_city || ', ', '') || nvl(ta.addr_street, '') ||
             nvl(' ' || ta.addr_building, '') ||
             nvl(' - ' || ta.addr_office, ''),
             org.org_ogrn
        into l_name_org_sel, l_kpp_sel, l_inn_sel, l_adress_sel, l_ogrn_sel
        from t_organizations org
        left join t_address ta
          on ta.addr_id = NVL(org.adr2_id, org.adr1_id) --org.adr1_id  08.02.10
       where org.org_id = pi_org_id_sel;
    end if;
    -- грузоотправитель
    if pi_org_id_send is not null then
      select org.org_name,
             nvl(ta.addr_index || ', ', '') ||
             nvl(ta.addr_country || ', ', '') ||
             nvl(ta.addr_city || ', ', '') || nvl(ta.addr_street, ' ') ||
             nvl(ta.addr_building, '') || nvl(' - ' || ta.addr_office, '')
        into l_name_org_send, l_adress_send
        from t_organizations org
        left join t_address ta
          on ta.addr_id = NVL(org.adr1_id, org.adr2_id)
       where org.org_id = pi_org_id_send;
    end if;
    -- покупатель
    if pi_org_id_buy is not null then
      select org.org_name,
             org.org_kpp,
             org.org_inn,
             Decode(pi_org_id_buy,
                    2001825,
                    '614096, Россия, г.Пермь, ул.Ленина, 68',
                    nvl(ta.addr_index || ', ', '') ||
                    nvl(ta.addr_country || ', ', '') ||
                    nvl(ta.addr_city || ', ', '') ||
                    nvl(ta.addr_street || ' ', '') ||
                    nvl(ta.addr_building, '') ||
                    nvl(' - ' || ta.addr_office, '')),
             org.org_buy
        into l_name_org_buy, l_kpp_buy, l_inn_buy, l_adress_buy, l_org_buy
        from t_organizations org
        left join t_address ta
          on ta.addr_id = NVL(org.adr1_id, org.adr2_id)
       where org.org_id = pi_org_id_buy;
    end if;

    select sum(trs.sum), sum(trs.nds), sum(trs.sum_with_nds)
      into l_sum, l_sum_nds, l_sum_with_nds
      from t_report_Stat trs
      join t_report_period trp
        on trp.id = trs.id_repper
       and trp.is_actual = 1
     where trp.id = pi_rp_id;
    -- берём данные из getReportDetails
    select distinct rp.num_zaiav_opl,
                    rp.DT_ZAIAV_OPL,
                    /*decode(org_dil.USE_CHILD_REQ,
                    1,
                    ooc.v_lice,*/
                    org_kur.v_lice v_lice_pr,
                    /*decode(org_dil.USE_CHILD_REQ,
                    1,
                    ooc.na_osnovanii,*/
                    org_kur.na_osnovanii na_osnovanii_pr,
                    --nvl(ooc.v_lice, org_kur.v_lice) v_lice_pr, -- в лице (принципала)
                    --nvl(ooc.na_osnovanii, org_kur.na_osnovanii) na_osnovanii_pr, -- на основании (принципала)
                    d.v_lice          v_lice_ag, -- в лице (агента)
                    d.na_osnovanii    na_osnovanii_ag, -- на основании (агента)
                    org_dil.region_id region_number, -- id региона
                    rp.dates,
                    rp.datepo
      into l_num_zaiav_opl,
           l_DT_ZAIAV_OPL,
           l_v_lice_pr,
           l_na_osnovanii_pr,
           l_v_lice_ag,
           l_na_osnovanii_ag,
           l_region_id,
           l_date_s,
           l_date_po
      from t_report_period rp
      join t_organizations org_dil
        on org_dil.org_id = rp.id_org
      join mv_org_tree ro
        on ro.org_id = rp.id_org
      join t_organizations org_kur
        on org_kur.org_id = ro.org_pid
      join t_dogovor d
        on rp.dog_id = d.dog_id
       and d.org_rel_id = ro.root_rel_id
     where rp.is_actual = 1
       and pi_rp_id = rp.id
       and ro.root_reltype in (1003, 1004, 1007, 1008, 999)
       and (rp.id_org in
           (select * from TABLE(get_user_orgs_tab(pi_worker_id))));
    -- заполняем таблицу
    insert into t_report_sf SF
      (id_repper,
       id_dog,
       date_dog,
       number_dog,
       number_sf,
       date_sf,
       id_org_sel,
       name_org_sel,
       adress_sel,
       inn_sel,
       kpp_sel,
       ogrn_sel,
       id_org_send,
       name_org_send,
       adress_send,
       id_org_buy,
       name_org_buy,
       adress_buy,
       inn_buy,
       kpp_buy,
       summa,
       summa_nds,
       summa_with_nds,
       v_lice_pr,
       na_osnovanii_pr,
       NUM_ZAIAV_OPL,
       DT_ZAIAV_OPL,
       v_lice_ag,
       na_osnovanii_ag,
       REGION_id,
       date_s,
       date_po,
       NAME_BUY)
    values
      (pi_rp_id,
       l_dog_id,
       l_dog_date,
       l_dog_number,
       pi_number_sf,
       pi_date_sf,
       pi_org_id_sel,
       l_name_org_sel,
       l_adress_sel,
       l_inn_sel,
       l_kpp_sel,
       l_ogrn_sel,
       pi_org_id_send,
       l_name_org_send,
       l_adress_send,
       pi_org_id_buy,
       l_name_org_buy,
       l_adress_buy,
       l_inn_buy,
       l_kpp_buy,
       l_sum,
       l_sum_nds,
       l_sum_with_nds,
       l_v_lice_pr,
       l_na_osnovanii_pr,
       l_num_zaiav_opl,
       l_DT_ZAIAV_OPL,
       l_v_lice_ag,
       l_na_osnovanii_ag,
       l_region_id,
       l_date_s,
       l_date_po,
       l_org_buy);
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
  end Save_Data_SF;
  ----------------------------------------------------------------------------
  -- Перевызов
  procedure Save_Data_SF(pi_rp_id     in number,
                         pi_number_sf in varchar, -- номер счёт-фактуры
                         pi_date_sf   in date, -- дата счёт-фактуры
                         pi_worker_id in T_USERS.USR_ID%type,
                         po_err_num   out pls_integer,
                         po_err_msg   out t_Err_Msg) is
    l_org_id_sel number; -- продавец
    l_org_id_buy number; -- покупатель
  begin
    select t.id_org
      into l_org_id_sel -- продавец
      from t_report_period t
     where t.id = pi_rp_id
       and t.is_actual = 1;
    select tor.org_pid
      into l_org_id_buy -- покупатель
      from t_report_period t
      join t_dogovor td
        on td.dog_id = t.dog_id
      join t_org_relations tor
        on tor.id = td.org_rel_id
      left join t_organizations org
        on org.org_id = tor.org_id
     where t.id = pi_rp_id
       and t.is_actual = 1;
    Save_Data_SF(pi_rp_id       => pi_rp_id,
                 pi_number_sf   => null,
                 pi_date_sf     => pi_date_sf,
                 pi_org_id_sel  => l_org_id_sel,
                 pi_org_id_send => null,
                 pi_org_id_buy  => l_org_id_buy,
                 pi_flag_corr   => 1,
                 pi_worker_id   => pi_worker_id,
                 po_err_num     => po_err_num,
                 po_err_msg     => po_err_msg);
  end Save_Data_SF;
  ----------------------------------------------------------------------------
  -- возвращает счёт-фактуру по id отчётного периода
  function Get_Data_SF(pi_rp_id     in number, -- id отчётного перииода
                       pi_worker_id in number,
                       po_err_num   out pls_integer,
                       po_err_msg   out t_Err_Msg) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select id,
             id_repper,
             id_dog,
             date_dog,
             number_dog,
             number_sf,
             date_sf,
             id_org_sel,
             name_org_sel,
             adress_sel,
             inn_sel,
             kpp_sel,
             ogrn_sel,
             id_org_send,
             name_org_send,
             adress_send,
             id_org_buy,
             name_org_buy,
             org_full_name_buy,
             name_buy,
             org_ogrn,
             adress_buy,
             inn_buy,
             kpp_buy,
             sum(summa) * 100 as summa,
             sum(summa_nds) * 100 as summa_nds,
             sum(summa_with_nds) * 100 as summa_with_nds,
             v_lice_pr,
             na_osnovanii_pr,
             num_zaiav_opl,
             dt_zaiav_opl,
             v_lice_ag,
             na_osnovanii_ag,
             region_id,
             date_s,
             date_po,
             codes,
             status_report,
             adoption_date,
             payment_date,
             transcript,
             is_pay_espp1
        from (select sf.id,
                     sf.id_repper,
                     sf.id_dog,
                     sf.date_dog,
                     sf.number_dog,
                     sf.number_sf,
                     sf.date_sf,
                     sf.id_org_sel,
                     (case
                       when d.ORG_LEGAL_FORM = 1 then
                        org.org_full_name || ', ' || org.org_name
                       when d.ORG_LEGAL_FORM = 2 then
                        nvl(org.org_full_name, org.org_name)
                       else
                        nvl(org.org_full_name, org.org_name)
                     end) name_org_sel,
                     sf.adress_sel,
                     sf.inn_sel,
                     sf.kpp_sel,
                     sf.ogrn_sel,
                     sf.id_org_send,
                     sf.name_org_send,
                     sf.adress_send,
                     sf.id_org_buy,
                     sf.name_org_buy,
                     org_buy.org_full_name org_full_name_buy,
                     org_buy.org_ogrn,
                     sf.adress_buy,
                     sf.inn_buy,
                     sf.kpp_buy,
                     rs.sum as summa,
                     rs.nds as summa_nds,
                     rs.sum_with_nds as summa_with_nds,
                     (case
                       when rep.v_lice is not null then
                        rep.v_lice
                       else
                        sf.v_lice_pr
                     end) v_lice_pr,
                     (case
                       when rep.na_osnovanii is not null then
                        rep.na_osnovanii
                       else
                        sf.na_osnovanii_pr
                     end) na_osnovanii_pr,
                     sf.num_zaiav_opl,
                     sf.dt_zaiav_opl,
                     sf.v_lice_ag,
                     sf.na_osnovanii_ag,
                     sf.region_id,
                     sf.date_s,
                     sf.date_po,
                     d.codes,
                     st.name status_report,
                     sf.adoption_date,
                     sf.payment_date,
                     case
                       when upper(rs.name) like '%GSM%' then
                        'GSM'
                       when upper(rs.name) like '%OTT%' then
                        'OTT'
                       when upper(rs.name) like '%ADSL%' then
                        'ADSL'
                       when upper(rs.name) like upper('%АБОНЕНТСКОЕ%') then
                        'Абонентское обслуживание'
                       when upper(rs.name) like upper('%ДИСТАНЦИОННЫЕ%') then
                        'Дистанционные продажи'
                       when upper(rs.name) like upper('%ШТРАФНЫЕ%') then
                        'Штрафные санкции'
                       else
                        decode((select min(rc.ci)
                                 from t_report_real_conn rc
                                where rc.id_repper = pi_rp_id),
                               1,
                               'GSM',
                               2,
                               'ADSL',
                               3,
                               'Абонентское обслуживание',
                               rs.name)
                     end as transcript,
                     nvl(d.is_pay_espp1, 0) is_pay_espp1,
                     sf.name_buy
                from dual
                left join t_report_sf SF
                  on sf.id_repper = pi_rp_id
                left join t_report_stat rs
                  on rs.id_repper = sf.id_repper
                left join t_dogovor d
                  on d.dog_id = sf.id_dog
                left join t_org_relations rel
                  on d.org_rel_id = rel.id
                left join t_matching_report_doc rep
                  on rel.org_id = rep.id_org
                left join t_report_period rp
                  on rp.id = sf.id_repper
                left join t_dic_report_status st
                  on st.id = rp.status
                left join t_organizations org
                  on org.org_id = rel.org_id
                left join t_organizations org_buy
                  on org_buy.org_id = sf.id_org_buy
               where sf.id_repper is not null
                  or rs.id_repper is not null
                 and sf.summa is not null) ss
       group by id,
                id_repper,
                id_dog,
                date_dog,
                number_dog,
                number_sf,
                date_sf,
                id_org_sel,
                name_org_sel,
                adress_sel,
                inn_sel,
                kpp_sel,
                ogrn_sel,
                id_org_send,
                name_org_send,
                adress_send,
                id_org_buy,
                name_org_buy,
                org_full_name_buy,
                adress_buy,
                inn_buy,
                kpp_buy,
                v_lice_pr,
                na_osnovanii_pr,
                num_zaiav_opl,
                dt_zaiav_opl,
                v_lice_ag,
                na_osnovanii_ag,
                region_id,
                date_s,
                date_po,
                codes,
                status_report,
                adoption_date,
                payment_date,
                transcript,
                is_pay_espp1,
                name_buy,
                org_ogrn;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Data_SF;
  --------------------------------------------------------------
  ---  Выполнила
  --------------------------------------------------------------
  Function Is_Plan_Post(Pi_repper in Number) return number is
    res number;
  Begin
    Select (Case
             When Sum(amount) > 0 then
              1
             else
              0
           end)
      into res
      from (Select t.id,
                   t.date_Top,
                   t.Date_End,
                   t.id_org,
                   t.Dog_Id,
                   Sum(apa.amount_pay) - rrc.stavka_1401 - rrc.stavka_1402 amount
              from (Select r.id,
                           rp.dates Date_End,
                           r.dates  Date_Top,
                           r.id_org,
                           r.dog_id
                      from t_Report_Period rp
                      join t_report_activ ra
                        on ra.id_repper_cur = rp.id
                      join t_Report_Period r
                        on r.id = ra.id_repper
                       and r.is_actual = 1
                     where rp.id = Pi_repper
                       and rp.is_actual = 1
                       and Trunc(r.dates) = Add_Months(Trunc(rp.dates), -3)
                     Group By rp.id,
                              rp.dates,
                              r.id,
                              r.dates,
                              r.id_org,
                              r.dog_id) t
              Join t_Report_Real_Conn rrc
                on rrc.id_repper = t.id
              JOin t_ab_pay_activity apa
                on apa.abonent_id = rrc.id_conn
             Where apa.date_begin between t.Date_Top and t.Date_End
               and rrc.ci = 1
             Group By t.id,
                      t.date_Top,
                      t.Date_End,
                      rrc.id_conn,
                      t.id_org,
                      t.Dog_Id,
                      rrc.stavka_1401,
                      rrc.stavka_1402);
    return res;
  Exception
    when Others then
      res := 0;
      return res;
  End;
  --------------------------------------------------------------
  -- Сводный отчёт по агентскому вознаграждению
  function Get_Agent_Poruchenie_Svod(pi_org_pid   in number,
                                     pi_date_beg  in date, -- включаем
                                     pi_date_end  in date, -- не включаем
                                     pi_worker_id in number,
                                     po_err_num   out pls_integer,
                                     po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    if (not Security_pkg.Check_Rights_str('EISSD.REPORT.PRINCIPAL.VIEW',
                                          pi_org_pid,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      return null;
    end if;
    open res for
      Select tor.org_id,
             org.org_name DILER,
             Sum((case
                   when rp.dates < Trunc(pi_date_beg) then
                    NVL(trs.sum_with_nds, 0)
                   else
                    0
                 end)) - NVL((Select sum(ro.amount)
                               from t_report_oplata ro
                              where ro.dog_id = dog.dog_id
                                and ro.data_opl < pi_date_beg),
                             0) BEG_SALDO,
             Sum(nvl(Decode(rp.dates, Trunc(pi_date_beg), trs.sum, 0), 0)) NoNDS,
             Sum(nvl(Decode(rp.dates, Trunc(pi_date_beg), trs.nds, 0), 0)) NDS,
             Sum(nvl(Decode(rp.dates,
                            Trunc(pi_date_beg),
                            trs.sum_with_nds,
                            0),
                     0)) WithNDS,
             NVL((Select sum(ro.amount)
                   from t_report_oplata ro
                  where ro.dog_id = dog.dog_id
                    and Trunc(ro.data_opl) Between pi_date_beg and
                        pi_date_end),
                 0) PAYD,
             sum(NVL(trs.sum_with_nds, 0)) -
             NVL((Select sum(ro.amount)
                   from t_report_oplata ro
                  where ro.dog_id = dog.dog_id
                    and ro.data_opl <= pi_date_end),
                 0) SALDO_END,
             null summ_corr,
             p.dp_prm_id perm,
             max(prm.prm_name) prm_name
        from t_org_relations tor
        join t_dogovor dog
          on dog.org_rel_id = tor.id
         and dog.is_enabled = 1
        join t_dogovor_prm p
          on p.dp_dog_id = dog.dog_id
         and p.dp_prm_id in (2000, 2001 /*, 4000*/)
         and p.dp_is_enabled = 1
        join t_Perm prm
          on prm.prm_id = p.dp_prm_id
        left join t_Report_Period rp
          on rp.dog_id = dog.dog_id
         and rp.is_actual = 1
         and rp.dates <= Trunc(pi_date_beg)
        left join (select stat.id_repper,
                          stat.sum,
                          stat.sum_with_nds,
                          stat.nds,
                          (case
                            when stat.perm = 4000 then
                             2000
                            else
                             stat.perm
                          end) as perm
                     from t_report_stat stat) trs
          on trs.id_repper = rp.id
         and trs.perm = prm.prm_id
        Join t_Organizations org
          on org.Org_Id = tor.org_id
      Connect by prior tor.org_id = tor.org_pid
       start with tor.org_pid = pi_org_pid
       Group by p.dp_prm_id, org.org_name, tor.org_id, dog.dog_id
       Order by p.dp_prm_id, org.org_name, tor.org_id, dog.dog_id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Agent_Poruchenie_Svod;
  --------------------------------------------------------------
  -- Оборотно-сальдовая ведомость по лицевым счетам дилеров
  function Get_Ob_Sald_Ved(pi_org_pid   in number,
                           pi_date_beg  in date, -- включаем
                           pi_date_end  in date, -- не включаем
                           pi_worker_id in number,
                           pi_prm_id    in number, -- 2000  Продажа GSM, 2001 Продажа ADSL, 2003  Продажа карт оплаты задача № 21054 patrick 13.07.2010
                           po_err_num   out pls_integer,
                           po_err_msg   out varchar2) return sys_refcursor is
    res sys_refcursor;
  begin
    if (not Security_pkg.Check_Rights_str('EISSD.REPORT.PRINCIPAL.VIEW',
                                          pi_org_pid,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      return null;
    end if;
    open res for
      Select prm_Id,
             Decode(Grouping(org_name),
                    0,
                    org_name,
                    Decode(prm_Id,
                           0,
                           'Итого по агентам:',
                           Decode(Grouping(prm_Id),
                                  0,
                                  'Итого по Комиссионерам',
                                  'Всего:'))) org_name,
             Sum(SALDO_KT_BEG) SALDO_KT_BEG, --сальдо на начало периода (кредит. Деньги, которые принципал должен агенту)
             Sum(SALDO_DT_BEG) SALDO_DT_BEG, --сальдо на начало периода (дебет Деньги, которые агент должен принципалу)
             Sum(SALDO_KT_BEG_OVER) SALDO_KT_BEG_OVER,
             Sum(SALDO_DT_BEG_OVER) SALDO_DT_BEG_OVER,
             Sum(prix_lic) prix_lic, -- поступило на лиц. счёт
             Sum(prix_lic_other) prix_lic_other, --прочие поступления на лиц. счёт
             Sum(prix_itogo) prix_itogo, -- итого
             Sum(rash_podkl_realiz) rash_podkl_realiz, -- подключено/реализовано
             Sum(rash_lost_active) rash_lost_active, --утеряно / активировано
             Sum(rash_lost_notactiv) rash_lost_notactiv, -- утеряно/ неактивировано
             Sum(rash_other) rash_other, -- прочие расходы (корректировки + списание средствс лиц. счёта по коду 6007)
             Sum(rash_itogo) rash_itogo, -- итого расход
             GREATEST(Sum(SALDO_DT_BEG) - Sum(SALDO_KT_BEG) +
                      Sum(SALDO_DT_END) - Sum(SALDO_KT_END),
                      0) SALDO_DT_END, --сальдо на конец периода (кредит)
             abs(least(Sum(SALDO_DT_BEG) - Sum(SALDO_KT_BEG) +
                       Sum(SALDO_DT_END) - Sum(SALDO_KT_END),
                       0)) saldo_Kt_end, --сальдо на конец периода (дебет)
             GREATEST(Sum(SALDO_DT_BEG) - Sum(SALDO_KT_BEG) +
                      Sum(SALDO_DT_END_OVER) - Sum(SALDO_KT_END_OVER),
                      0) SALDO_DT_END_OVER,
             abs(least(Sum(SALDO_DT_BEG) - Sum(SALDO_KT_BEG) +
                       Sum(SALDO_DT_END_OVER) - Sum(SALDO_KT_END_OVER),
                       0)) SALDO_KT_END_OVER
        from (select t.Dog_id,
                     t.prm_Id,
                     t.org_name,
                     GREATEST(acc_operations.Get_acc_Saldo2(t.dog_id,
                                                            PI_DATE_BEG,
                                                            acc_operations.c_acc_type_lic) +
                              acc_operations.Get_acc_Saldo2(t.dog_id,
                                                            PI_DATE_BEG,
                                                            acc_operations.c_acc_type_res),
                              0) SALDO_KT_BEG, --сальдо на начало периода (кредит. Деньги, которые принципал должен агенту)
                     abs(least(acc_operations.Get_acc_Saldo2(t.dog_id,
                                                             PI_DATE_BEG,
                                                             acc_operations.c_acc_type_lic) +
                               acc_operations.Get_acc_Saldo2(t.dog_id,
                                                             PI_DATE_BEG,
                                                             acc_operations.c_acc_type_res),
                               0)) SALDO_DT_BEG, --сальдо на начало периода (дебет Деньги, которые агент должен принципалу)
                     GREATEST(sum(nvl(t.SALDO_BEG, 0)) +
                              (select nvl(tas.overdraft, 0)
                                 from t_accounts ta
                                 join t_acc_owner tao
                                   on tao.owner_id = ta.acc_owner_id
                                 join t_acc_schema tas
                                   on tas.id_acc = ta.acc_id
                                where tao.owner_ctx_id = t.dog_id),
                              0) SALDO_KT_BEG_OVER,
                     abs(LEAST(sum(nvl(t.SALDO_BEG, 0)) +
                               (select nvl(tas.overdraft, 0)
                                  from t_accounts ta
                                  join t_acc_owner tao
                                    on tao.owner_id = ta.acc_owner_id
                                  join t_acc_schema tas
                                    on tas.id_acc = ta.acc_id
                                 where tao.owner_ctx_id = t.dog_id),
                               0)) SALDO_DT_BEG_OVER,
                     sum(nvl(t.prix_lic, 0)) prix_lic, -- поступило на лиц. счёт
                     sum(NVL(t.prix_lic_other, 0)) prix_lic_other, --прочие поступления на лиц. счёт
                     sum(nvl(t.prix_lic, 0) + nvl(t.prix_lic_other, 0)) prix_itogo, -- итого
                     sum(nvl(abs(t.rash_podkl_realiz), 0)) rash_podkl_realiz, -- подключено/реализовано
                     sum(nvl(abs(t.rash_lost_active), 0)) rash_lost_active, --утеряно / активировано
                     sum(nvl(abs(t.rash_lost_notactiv), 0)) rash_lost_notactiv, -- утеряно/ неактивировано
                     sum(nvl(abs(t.rash_other), 0)) rash_other, -- прочие расходы (корректировки + списание средствс лиц. счёта по коду 6007)
                     sum(nvl(abs(t.rash_podkl_realiz), 0) +
                         nvl(abs(t.rash_lost_active), 0) +
                         nvl(abs(t.rash_lost_notactiv), 0) +
                         nvl(abs(t.rash_other), 0)) rash_itogo, -- итого расход
                     GREATEST(sum(nvl(t.SALDO_END, 0)), 0) saldo_Kt_end, --сальдо на начало периода (дебет)
                     ABS(least(sum(nvl(t.SALDO_END, 0)), 0)) SALDO_DT_END, --сальдо на начало периода (кредит)
                     GREATEST(sum(nvl(t.SALDO_END, 0)) +
                              (select nvl(tas.overdraft, 0)
                                 from t_accounts ta
                                 join t_acc_owner tao
                                   on tao.owner_id = ta.acc_owner_id
                                 join t_acc_schema tas
                                   on tas.id_acc = ta.acc_id
                                where tao.owner_ctx_id = t.dog_id),
                              0) SALDO_KT_END_OVER,
                     abs(LEAST(sum(nvl(t.SALDO_END, 0)) +
                               (select nvl(tas.overdraft, 0)
                                  from t_accounts ta
                                  join t_acc_owner tao
                                    on tao.owner_id = ta.acc_owner_id
                                  join t_acc_schema tas
                                    on tas.id_acc = ta.acc_id
                                 where tao.owner_ctx_id = t.dog_id),
                               0)) SALDO_DT_END_OVER
                from (select org.org_id diler,
                             org.org_name,
                             dog.dog_id,
                             dog_P.prm_id,
                             (Case
                               when trunc(tap.pay_date) < trunc(PI_DATE_BEG) then
                                Decode(tap.tr_type,
                                       6007,
                                       tap.amount * (-1),
                                       tap.amount)
                               else
                                0
                             end) SALDO_BEG,
                             (Case
                               when trunc(tap.pay_date) >= trunc(PI_DATE_BEG) and
                                    trunc(tap.pay_date) < trunc(PI_DATE_END) then
                                decode(tap.tr_type, 6000, tap.amount)
                               else
                                0
                             end) prix_lic,
                             (Case
                               when trunc(tap.pay_date) >= trunc(PI_DATE_BEG) and
                                    trunc(tap.pay_date) < trunc(PI_DATE_END) then
                                decode(tap.tr_type,
                                       6022,
                                       case
                                         when tap.amount > 0 then
                                          tap.amount
                                         else
                                          0
                                       end)
                               else
                                0
                             end) prix_lic_other, -- учитываем только корректировки с "+"
                             (Case
                               when trunc(tap.pay_date) >= trunc(PI_DATE_BEG) and
                                    trunc(tap.pay_date) < trunc(PI_DATE_END) then
                                decode(tap.tr_type,
                                       6020,
                                       tap.amount,
                                       6021,
                                       tap.amount)
                               else
                                0
                             end) rash_podkl_realiz,
                             (Case
                               when trunc(tap.pay_date) >= trunc(PI_DATE_BEG) and
                                    trunc(tap.pay_date) < trunc(PI_DATE_END) then
                                decode(tap.tr_type, 6033, tap.amount)
                               else
                                0
                             end) rash_lost_active,
                             (Case
                               when trunc(tap.pay_date) >= trunc(PI_DATE_BEG) and
                                    trunc(tap.pay_date) < trunc(PI_DATE_END) then
                                decode(tap.tr_type, 6019, tap.amount)
                               else
                                0
                             end) rash_lost_notactiv,
                             (Case
                               when trunc(tap.pay_date) >= trunc(PI_DATE_BEG) and
                                    trunc(tap.pay_date) < trunc(PI_DATE_END) then
                                decode(tap.tr_type,
                                       6007,
                                       tap.amount,
                                       6022,
                                       case
                                         when tap.amount < 0 then -- учитываем только корректировки с "-"
                                          abs(tap.amount)
                                         else
                                          0
                                       end)
                               else
                                0
                             end) rash_other,
                             (Case
                               when trunc(tap.pay_date) <= trunc(PI_DATE_END) then
                                Decode(tap.tr_type,
                                       6007,
                                       tap.amount * (-1),
                                       tap.amount)
                               else
                                0
                             end) SALDO_END
                        from t_org_relations tor
                        join t_dogovor dog
                          on dog.org_rel_id = tor.id
                         and dog.is_enabled = 1
                      -- задача № 21054 patrick 13.07.2010
                        join (select distinct tdp.dp_dog_id
                               from t_dogovor_prm tdp
                              where ((pi_prm_id is null) or
                                    (tdp.dp_prm_id = pi_prm_id))
                                and tdp.dp_is_enabled = 1) dp
                          on dog.dog_id = dp.dp_dog_id
                        join (Select t.dp_dog_id,
                                    Decode(t.dp_prm_id, 2003, 1, 0) prm_id
                               from t_dogovor_prm t
                              where t.dp_is_enabled = 1
                              Group By t.dp_dog_id,
                                       Decode(t.dp_prm_id, 2003, 1, 0)) dog_P
                          on dog_P.Dp_Dog_Id = dog.dog_id
                        join t_acc_owner tao
                          on tao.owner_ctx_id = dog.dog_id
                        join t_accounts acc
                          on acc.acc_owner_id = tao.owner_id
                        left join t_acc_pay tap
                          on acc.acc_id = tap.acc_id
                         and trunc(tap.pay_date) >= trunc(pi_date_beg)
                         and trunc(tap.pay_date) < trunc(PI_DATE_END)
                        Join t_Organizations org
                          on org.Org_Id = tor.org_Id
                       where tor.org_pid = PI_ORG_PID) t
               group by t.prm_Id, t.org_name, t.diler, t.dog_id
               order by t.prm_Id, t.org_name)
       Group by rollUp(prm_Id, org_name)
       order by prm_Id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Ob_Sald_Ved;
  ---------------------------------------------------------
  -- Возвращает список сотрудников заданной организации и кол-во их подключений
  -- за конкретный период
  ---------------------------------------------------------
  function Get_Report_User_Podkluch(pi_org_id     in t_organizations.org_id%type,
                                    pi_begin_date in date,
                                    pi_end_date   in date,
                                    pi_worker_id  in T_USERS.USR_ID%type,
                                    po_err_num    out pls_integer,
                                    po_err_msg    out t_Err_Msg)
    return sys_refcursor is
    res sys_refcursor;
  begin
    if (not Security_pkg.Check_Rights_str('EISSD.REPORT.CONNECTIONS_STAT_BY_WORKER',
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      return null;
    end if;

    open res for
      select u.usr_id,
             person_lastname p_surname,
             person_firstname p_name,
             person_middlename p_secondname,
             count(ab.ab_id) kol,
             count(act.abonent_id) kol_act,
             org.org_id,
             org.org_name
        from t_users u
        join t_person p
          on u.usr_person_id = p.person_id
        join t_abonent ab
          on ab.user_id = u.usr_id
        join t_organizations org
          on org.org_id = ab.org_id
        left join t_abonent_activated act
          on act.abonent_id = ab.ab_id
       where ab.is_deleted = 0
         and ab.ab_status in (104, 105)
         and ab.org_id in (select org_id
                             from t_org_relations rel
                           connect by prior rel.org_id = rel.org_pid
                            start with rel.org_id = pi_org_id
                                   and rel.org_reltype = 1001)
         and trunc(ab.change_status_date - 2 / 24) between
             trunc(pi_begin_date) and trunc(pi_end_date)
       group by u.usr_id,
                person_lastname,
                person_firstname,
                person_middlename,
                org.org_id,
                org.org_name
       order by person_lastname,
                person_firstname,
                person_middlename,
                org.org_name;

    /* e.komissarov 17.04.2009 бред...
         select tu.usr_id,
                person_lastname P_SURNAME,
                person_firstname P_NAME,
                person_middlename P_SECONDNAME,
                count(tab.ab_id) KOL

           from t_users TU,
                t_person TP,
                t_abonent TAB,
                t_user_org UO

          where uo.org_id in
                (select org_id
                   from t_org_relations rel
                  connect by prior rel.ORG_ID = rel.ORG_PID
                  start with rel.ORG_ID = pi_org_id
                    and rel.org_reltype = 1001)
            and uo.usr_id = tu.usr_id
            and tu.usr_person_id = tp.person_id
            and tab.user_id = tu.usr_id
            and (trunc(tab.change_status_date) between pi_begin_date and pi_end_date)
       group by tu.usr_id, person_lastname, person_firstname, person_middlename
       order by 2,3,4;
    */
    return res;

  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;

  end Get_Report_User_Podkluch;
  -------------------------------------------------------------------------
  function save_documents(pi_doc      in blob,
                          pi_doc_name varchar2,
                          pi_doc_id   in number,
                          pi_rep_id   in number) return number as
    l_doc_id number;
    dummy    number;
  begin
    l_doc_id := pi_doc_id;
    dummy    := documents.Add_document(l_doc_id, pi_doc_name, pi_doc);
    update t_report_period t
       set t.id_document = l_doc_id
     where t.id = pi_rep_id;
    return l_doc_id;
  end;
  -------------------------------------------------------------------------
  --  инвентаризационная ведомость по агентскому вознаграждению
  -------------------------------------------------------------------------
  function Get_INV_Nagr(pi_date    in date,
                        pi_org_id  in number, -- курирующая
                        po_err_num out pls_integer,
                        po_err_msg out t_Err_Msg) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      Select prm_id,
             Max(org_id) org_id,
             Decode(Grouping(org_name),
                    0,
                    org_name,
                    Decode(prm_Id,
                           0,
                           'Итого по агентам:',
                           Decode(Grouping(prm_Id),
                                  0,
                                  'Итого по Комиссионерам:',
                                  'Всего:'))) org_name,
             sum(sum_nagr_opl) sum_nagr_opl,
             sign
        from (select t.prm_id,
                     t.org_id,
                     t.org_name,
                     t.dog_id,
                     abs(t.sum_nagr_opl - nvl(tro.amount, 0)) sum_nagr_opl,
                     (case
                       when t.sum_nagr_opl - nvl(tro.amount, 0) >= 0 then
                        1
                       else
                        0
                     end) sign
                from (select dog_P.prm_id,
                             org.org_id,
                             org.org_name,
                             td.dog_id,
                             sum(nvl(trs.summa_with_nds, 0)) sum_nagr_opl
                        from t_dogovor td
                        join (Select t.dp_dog_id,
                                    Decode(t.dp_prm_id, 2003, 1, 0) prm_id
                               from t_dogovor_prm t
                              where t.dp_is_enabled = 1
                              Group By t.dp_dog_id,
                                       Decode(t.dp_prm_id, 2003, 1, 0)) dog_P
                          on dog_P.Dp_Dog_Id = td.dog_id
                        join t_report_period trp
                          on trp.dog_id = td.dog_id
                         and trunc(trp.dates) <= pi_date
                         and trp.is_actual = 1
                        join t_report_sf trs
                          on trp.id = trs.id_repper
                        join t_organizations org
                          on org.org_id = trp.id_org
                       where td.is_enabled = 1
                         and (org.org_id in
                             (select tor.org_id
                                 from t_org_relations tor
                               connect by prior tor.org_id = tor.org_pid
                                start with tor.org_pid = pi_org_id) or
                             org.org_id = pi_org_id)
                       group by dog_P.prm_id,
                                org.org_id,
                                org.org_name,
                                td.dog_id
                       Order by dog_P.prm_id,
                                org.org_id,
                                org.org_name,
                                td.dog_id) t
                left join (Select tro.dog_id, Sum(tro.amount) amount
                            from t_report_oplata tro
                           where tro.data_opl <=
                                 trunc(Add_months(pi_date, 1), 'mm')
                           Group by tro.dog_id) tro
                  on tro.dog_id = t.dog_id
               order by sign desc, t.prm_id, t.org_name)
      having Grouping(sign) = 0 and Grouping(prm_id) = 0
       Group by rollUp(sign, prm_Id, org_name)
       order by sign desc, prm_id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_inv_Nagr;
  ------------------------------------------------------------
  function Get_INV_Obesp(pi_date    in date,
                         pi_org_id  in number, -- курирующая
                         pi_prm_id  in number, -- 2000  Продажа GSM, 2001 Продажа ADSL, 2003  Продажа карт оплаты задача № 21054 patrick 13.07.2010
                         po_err_num out pls_integer,
                         po_err_msg out t_Err_Msg) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      Select prm_id,
             Max(org_id) org_id,
             Decode(Grouping(org_name),
                    0,
                    org_name,
                    Decode(prm_Id,
                           0,
                           'Итого по агентам:',
                           Decode(Grouping(prm_Id),
                                  0,
                                  'Итого по Комиссионерам:',
                                  'Всего:'))) org_name,
             sum(sum_nagr_opl) sum_nagr_opl,
             sign
        from (select t.prm_id,
                     t.org_id,
                     t.org_name,
                     t.dog_id,
                     abs(t.sum_nagr_opl) sum_nagr_opl,
                     (case
                       when t.sum_nagr_opl >= 0 then
                        1
                       else
                        0
                     end) sign
                from (select dog_P.prm_id,
                             org.org_id,
                             org.org_name,
                             td.dog_id,
                             nvl(acc_operations.Get_Sum_Saldo_Lic_Res(td.dog_id,
                                                                      Add_Months(pi_date,
                                                                                 1)),
                                 0) sum_nagr_opl
                        from t_dogovor td
                        join (select distinct tdp.dp_dog_id
                               from t_dogovor_prm tdp
                              where ((pi_prm_id is null) or
                                    (tdp.dp_prm_id = pi_prm_id))
                                and tdp.dp_is_enabled = 1) dp
                          on td.dog_id = dp.dp_dog_id
                        join (Select t.dp_dog_id,
                                    Decode(t.dp_prm_id, 2003, 1, 0) prm_id
                               from t_dogovor_prm t
                              where t.dp_is_enabled = 1
                              Group By t.dp_dog_id,
                                       Decode(t.dp_prm_id, 2003, 1, 0)) dog_P
                          on dog_P.Dp_Dog_Id = td.dog_id
                        join t_report_period trp
                          on trp.dog_id = td.dog_id
                         and trunc(trp.dates) <= pi_date
                         and trp.is_actual = 1
                        join t_report_sf trs
                          on trp.id = trs.id_repper
                        left join t_report_oplata tro
                          on tro.id_repper = trp.id
                        join t_organizations org
                          on org.org_id = trp.id_org
                       where td.is_enabled = 1
                         and (org.org_id in
                             (select tor.org_id
                                 from t_org_relations tor
                               connect by prior tor.org_id = tor.org_pid
                                start with tor.org_pid = pi_org_id) or
                             org.org_id = pi_org_id)
                       group by dog_P.prm_id,
                                org.org_id,
                                org.org_name,
                                td.dog_id
                       Order by dog_P.prm_id,
                                org.org_id,
                                org.org_name,
                                td.dog_id) t
               order by sign desc, t.prm_id, t.org_name)
      having Grouping(sign) = 0 and Grouping(prm_id) = 0
       Group by rollUp(sign, prm_Id, org_name)
       order by sign desc, prm_id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_inv_Obesp;
  -------------------------------------------------------------------
  function Change_ReportPeriod_Status(pi_status  in number,
                                      pi_rp_id   in number,
                                      pi_id_user in number,
                                      po_err_num out pls_integer,
                                      po_err_msg out t_Err_Msg) return number is
    l_old_status number;
    l_date_beg   date;
    l_date_end   date;
    l_org_id     number;
    l_dog_id     number;
    ex_raise exception;
  begin
    select rp.status, rp.dates, rp.datepo, rp.id_org, rp.dog_id
      into l_old_status, l_date_beg, l_date_end, l_org_id, l_dog_id
      from t_report_period rp
     where rp.id = pi_rp_id;
    if (not Security_pkg.Check_Rights_str('EISSD.REPORT.AGENT.CONFIRM', --Подтверждение отчёта агента
                                          l_org_id,
                                          pi_id_user,
                                          po_err_num,
                                          po_err_msg,
                                          1 = 1)) then
      return null;
    end if;
    if l_old_status in (1, 2, 5, 6) or
       (l_old_status = 3 and pi_status not in (4, 5)) or
       (l_old_status = 4 and pi_status not in (6)) then
      --Отчёт формируется,Сформирован прогнозный отчёт,Отчёт некорректен,Отчёт ошибочен
      raise ex_raise;
    end if;
    update t_report_period t
       set t.status = pi_status
     where t.id = pi_rp_id;

    update t_report_period_request t
       set t.status = pi_status
     where (t.dates, t.org_id, t.dog_id) =
           (select tt.dates, tt.id_org, tt.dog_id
              from t_report_period tt
             where tt.id = pi_rp_id);

    insert into T_REPORT_PERIOD_HST
      (ID_REPPER,
       DATE_BEGIN,
       DATE_END,
       ORG_ID,
       DOG_ID,
       DATE_CHANGE,
       ID_USER,
       status_old)
    values
      (pi_rp_id,
       l_date_beg,
       l_date_end,
       l_org_id,
       l_dog_id,
       sysdate,
       pi_id_user,
       l_old_status);
    return 1;
  Exception
    when ex_raise then
      if l_old_status in (2, 5, 6) then
        po_err_num := 1;
        po_err_msg := 'Предыдущий статус отчета является финальным и его невозможно изменить.';
      else
        po_err_num := 2;
        po_err_msg := 'Предыдущий статус отчета невозможно изменить на желаемый.';
      end if;
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  -------------------------------------------------------------------
  --Групповые операции над отчетностью агента.
  --Принимает список идентификаторов отчетных периодов, и статус, который необходимо установить указанным отчетным периодам.
  --Предусмотреть проверку возможности смены статусов.
  function Change_ReportPeriod_StatusList(pi_tab     in ARRAY_NUM_3,
                                          pi_id_user in number,
                                          po_err_num out pls_integer,
                                          po_err_msg out t_Err_Msg)
    return number is
    ex_raise exception;
    l_err_report number;
    l_err_status number;
    l_agent_name t_organizations.org_name%type;
    l_dog_number t_dogovor.dog_number%type;
    l_dates      t_report_period.dates%type;
  begin
    if (not Security_pkg.Check_User_Right_str('EISSD.REPORT.AGENT.CONFIRM', --Подтверждение отчёта агента
                                              pi_id_user,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;
    savepoint sp_begin;
    insert into T_REPORT_PERIOD_HST
      (ID_REPPER,
       DATE_BEGIN,
       DATE_END,
       ORG_ID,
       DOG_ID,
       DATE_CHANGE,
       ID_USER,
       status_old)
      select tpr.id,
             tpr.dates,
             tpr.datepo,
             tpr.id_org,
             tpr.dog_id,
             sysdate,
             pi_id_user,
             tpr.status
        from t_report_period tpr
        join table(pi_tab) tab
          on tab.number_1 = tpr.id;
    for ora in (select number_1, number_2 from table(pi_tab)) loop
      update t_report_period rp
         set rp.status = ora.number_2
       where rp.id = ora.number_1
         and (rp.status not in (1, 2, 5, 6) and
             ((rp.status = 3 and ora.number_2 in (4, 5, 6)) or
             (rp.status = 4 and ora.number_2 in (6))));
      if nvl(sql%rowcount, 0) = 0 then
        select org.org_name, d.dog_number, rp.dates
          into l_agent_name, l_dog_number, l_dates
          from t_report_period rp
          join t_organizations org
            on org.org_id = rp.id_org
          join t_dogovor d
            on d.dog_id = rp.dog_id
         where rp.id = ora.number_1;
        l_err_report := ora.number_1;
        l_err_status := ora.number_2;
        raise ex_raise;
      else
        update t_report_period_request rq
           set rq.status = ora.number_2
         where (rq.dates, rq.org_id, rq.dog_id) =
               (select t.dates, t.id_org, t.dog_id
                  from t_report_period t
                 where t.id = ora.number_1);
      end if;
    end loop;
    return 1;
  Exception
    when ex_raise then
      rollback to sp_begin;
      po_err_num := 1;
      po_err_msg := 'Предыдущий статус отчета агента ' || l_agent_name ||
                    '(№ договора ' || l_dog_number ||
                    ') за отчетный период ' || l_dates ||
                    ' невозможно изменить на желаемый. Удалите данный отчет из выборки либо поставьте корректный статус отчета и повторите операцию.';
      return null;
    when others then
      rollback to sp_begin;
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  ------------------------------------------------------------------------
  function RegistrationApplication(pi_date    in date,
                                   pi_org_id  in number,
                                   pi_dog_id  in number,
                                   pi_id_user in number,
                                   po_err_num out pls_integer,
                                   po_err_msg out t_Err_Msg) return number is
    l_check number := 0;
  begin
    if (not Security_pkg.Check_Rights_str('EISSD.REPORT.AGENT_NAGR.CREATE_REQUEST', -- Создание заявки на расчет агентского вознаграждения
                                          pi_org_id,
                                          pi_id_user,
                                          po_err_num,
                                          po_err_msg,
                                          1 = 1)) then
      return null;
    end if;
    if pi_dog_id is not null then
      insert into T_REPORT_PERIOD_REQUEST
        (ORG_ID, DOG_ID, DATES, STATUS, REQ_DATE)
      values
        (pi_org_id,
         pi_dog_id,
         trunc(pi_date, 'mm'),
         c_dic_rp_status_req_rebuild,
         sysdate);
      --50226
      update t_report_period t
         set t.status = 7
       where t.is_actual = 1
         and t.id_org = pi_org_id
         and t.dog_id = pi_dog_id
         and t.dates = trunc(pi_date, 'mm');
    else
      insert into T_REPORT_PERIOD_REQUEST
        (ORG_ID, DOG_ID, DATES, REQ_DATE, STATUS)
        select t.org_id,
               d.dog_id,
               pi_date,
               sysdate,
               c_dic_rp_status_req_rebuild
          from t_org_relations t, t_dogovor d
         where t.id = d.org_rel_id
           and t.org_id in (select tor.org_id
                              from t_org_relations tor
                             where tor.org_id > 0
                               and tor.org_reltype <> 1001
                            connect by prior tor.org_pid = tor.org_id
                             start with tor.org_id = pi_org_id);
      l_check := sql%rowcount;
      if l_check = 0 then
        po_err_num := 1;
        po_err_msg := 'Переданная организация не имеет договоров';
        return 0;
      end if;
      --50226
      update t_report_period t
         set t.status = 7
       where t.is_actual = 1
         and t.id_org in (select tor.org_id
                            from t_org_relations tor
                           where tor.org_id > 0
                             and tor.org_reltype <> 1001
                          connect by prior tor.org_pid = tor.org_id
                           start with tor.org_id = pi_org_id)
         and t.dates = trunc(pi_date, 'mm');
    end if;
    return 1;
  Exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  -------------------------------------------------------------------
  function GetListApplication( -- 41104
                              pi_date_from in date,
                              pi_date_to   in date,
                              --
                              pi_org_id  in number,
                              pi_dog_num in varchar2,
                              pi_status  in number,
                              pi_id_user in number,
                              po_err_num out pls_integer,
                              po_err_msg out t_Err_Msg) return sys_refcursor is
    res sys_refcursor;
  begin
    if (not Security_pkg.Check_User_Right_str('EISSD.REPORT.AGENT_NAGR.CREATE_REQUEST', -- Создание заявки на расчет агентского вознаграждения
                                              pi_id_user,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;
    open res for
      select org.org_name Org_Name,
             dog.dog_number Num_Dog,
             req.req_date - 2 / 24 dog_date,
             req.req_date - 2 / 24 req_date,
             st.name Status_Name,
             req.id id_request,
             rp.id id_repper
        from T_REPORT_PERIOD_REQUEST req
        left join t_organizations org
          on org.org_id = req.org_id
        left join t_dogovor dog
          on dog.dog_id = req.dog_id
        left join T_REPORT_PERIOD rp
          on rp.dates = trunc(req.dates, 'mm')
         and rp.id_org = req.org_id
         and rp.is_actual = 1
        left join t_dic_report_status st
          on st.id = rp.status
       where (pi_dog_num is null or dog.dog_number = pi_dog_num)
         and (pi_org_id is null or req.org_id = pi_org_id)
            -- 41104
         and (pi_date_from is null or req.dates >= pi_date_from)
         and (pi_date_to is null or req.dates <= pi_date_to)
         and (pi_status is null or req.status = pi_status);
    return res;
  Exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  -------------------------------------------------------------------
  function ExecutionApplication return number is
    res          sys_refcursor;
    po_err_msg   t_Err_Msg;
    po_err_num   pls_integer;
    l_repper     number;
    l_check      number;
    l_rep_filial number := 0;
    type t_rec is record(
      dates  date,
      datepo date,
      id_org number,
      dog_id number);
    rec t_rec;
  begin
    open res for
      select req.dates, last_day(req.dates) datepo, req.org_id, req.dog_id
        from T_REPORT_PERIOD_REQUEST req
       where req.rp_id is null
         for update nowait;
    loop
      fetch res
        into rec;
      exit when res%notfound;
      if rec.id_org in (2001270,
                        2001272,
                        2001279,
                        2001280,
                        2001433,
                        2001455,
                        2001454,
                        2004855,
                        2004866,
                        2004875,
                        2004884) then
        l_rep_filial := 1;
      end if;

      l_repper := FixReportPeriod(pi_dates      => rec.dates,
                                  pi_datepo     => rec.datepo,
                                  pi_id_org     => rec.id_org,
                                  pi_worker_id  => 777,
                                  pi_dog_id     => rec.dog_id,
                                  pi_rep_filial => l_rep_filial,
                                  po_err_num    => po_err_num,
                                  po_err_msg    => po_err_msg);
      update T_REPORT_PERIOD_REQUEST t
         set t.rp_id  = l_repper,
             t.status =
             (select t.status from t_report_period t where t.id = l_repper)
       where t.dates = rec.dates
         and t.org_id = rec.id_org
         and rec.dog_id = rec.dog_id;
    end loop;
    return 1;
  end;
  -------------------------------------------------------------------
  --изменение атрибутов счет-фактуры
  function ChangeInvoice(pi_id_report_sf  in number,
                         pi_date_sf       in date, --дата счет-фактуры
                         pi_date_ADOPTION in date, --дата принятия
                         pi_date_PAYMENT  in date, --дата оплаты
                         po_err_num       out pls_integer,
                         po_err_msg       out t_Err_Msg) return number is
  begin
    update t_report_sf sf
       set sf.date_sf       = pi_date_sf,
           sf.adoption_date = pi_date_ADOPTION,
           sf.payment_date  = pi_date_PAYMENT
     where sf.id_repper = pi_id_report_sf;
    return 1;
  Exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  --------------------------------------------------------------------------------
  -- 43379 Обертка для orgs.Get_FES_Bi_Id_Region без возвращаемых параметров,
  -- чтобы не возникала ошибка 'ORA-06572: Функция GET_FES_BI_ID_REGION
  -- имеет внешний аргумент'
  --------------------------------------------------------------------------------
  function Get_FES_Bi_Id_Region(pi_region_id in number) return number is
    po_err_num pls_integer;
    po_err_msg t_Err_Msg;
  begin
    return orgs.Get_FES_Bi_Id_Region(pi_region_id,
                                     0,
                                     po_err_num,
                                     po_err_msg);
  end Get_FES_Bi_Id_Region;
  -------------------------------------------------------------------
  --Акт оказанных услуг для Дист.продаж
  -------------------------------------------------------------------
  function ActServRendered_For_DistSales(pi_rp_id     in number,
                                         pi_worker_id in number,
                                         po_err_num   out number,
                                         po_msg       out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select sum(exec) as count_exec,
             max(stavka_exec) as cost_exec,
             sum(work1) as count_work,
             max(stavka_work) as cost_work,
             sum(del) as count_del,
             max(stavka_del) as cost_del,
             sum(del_ab) as count_del_ab,
             max(stavka_del_ab) as cost_del_ab,
             count(sum1) as count_all,
             sum(sum1) as summ,
             sum(sumwnds) - sum(sum1) as summNds,
             sum(sumwnds) as summWNds
        from (select distinct a.ab_id,
                              (case
                                when t.order_status = 0 then
                                 1
                                else
                                 0
                              end) exec,
                              (case
                                when t.order_status = 0 then
                                 a.stavka
                                else
                                 null
                              end) stavka_exec,
                              (case
                                when t.order_status = 4 and lw.id is null then
                                 1
                                else
                                 0
                              end) del,
                              (case
                                when t.order_status = 4 and lw.id is null then
                                 a.stavka
                                else
                                 null
                              end) stavka_del,
                              (case
                                when t.order_status = 4 and lw.id is not null then
                                 1
                                else
                                 0
                              end) del_ab,
                              (case
                                when t.order_status = 4 and lw.id is not null then
                                 a.stavka
                                else
                                 null
                              end) stavka_del_ab,
                              (case
                                when t.order_status not in (4, 0) then
                                 1
                                else
                                 0
                              end) work1,
                              (case
                                when t.order_status not in (4, 0) then
                                 a.stavka
                                else
                                 null
                              end) stavka_work,
                              a.sum as sum1,
                              a.sumwnds
                from t_report_abon_ords t
                join t_report_abonent a
                  on a.rp_id = t.rp_id
                 and a.ab_id = t.internal_id
                left join t_lira_withdrawal_reason lw
                  on lw.id = t.withdrawal_reason_id
                 and lw.type = 11
                 and lw.name like '%Отказ клиента%'
               where t.rp_id = pi_rp_id) al;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_msg     := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  -------------------------------------------------------------------
  --Реестр нарядов для отчета по дист продажам
  -------------------------------------------------------------------
  function ReestrOrder_For_DistSales(pi_rp_id     in number,
                                     pi_worker_id in number,
                                     po_err_num   out number,
                                     po_msg       out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select t.date_create as date_req,
             t.remote_id as remote_id, --номер наряда в лире
             t.order_status as status_id,
             dic.status_name as status,
             t.withdrawal_reason_id as withdrawal_reason_id,
             null as withdrawal_reason,
             t.date_exec as date_exec,
             to_date(to_char(t.date_engineer) || ' ' || t.time_engineer,
                     'dd.mm.yy hh24:mi:ss') as date_engineer,
             s.name as name_stavka
        from t_report_abon_ords t
        join t_dic_order_status dic
          on dic.status_id = t.order_status
        join t_report_abonent ra
          on ra.rp_id = t.rp_id
         and t.internal_id = ra.ab_id
        join t_stavka s
          on s.id = ra.kod_stavka
       where t.rp_id = pi_rp_id;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_msg     := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  -------------------------------------------------------------------
  ---66622 список заявок для выгрузки в отчете агента
  -------------------------------------------------------------------
  function Get_Req_for_Othet_Agenta(pi_org_id    in t_report_period.id_org%type,
                                    pi_dates     in t_report_period.dates%type,
                                    pi_worker_id in number,
                                    po_err_num   out number,
                                    po_msg       out varchar2)
    return sys_refcursor is
    res     sys_refcursor;
    l_rp_id number;
  begin
    select max(t.id)
      into l_rp_id
      from t_report_period t
     where trunc(t.dates) = trunc(pi_dates)
       and t.is_actual = 1
       and t.id_org = pi_org_id;
    open res for
      select contract_id,
             state_name,
             org_name,
             date_exec,
             telephone_number,
             internet_login,
             port_number,
             addr_city,
             addr_street,
             addr_building,
             addr_office,
             abonent,
             worker,
             mrf,
             filial,
             tariff,
             order_num,
             product_category,
             cast(collect(options) as string_tab) options
        from (select distinct r.id contract_id,
                              d.state_name,
                              o.org_name,
                              rs.date_change_state date_exec,
                              sd.initiator_phone telephone_number,
                              sd.initiator_number internet_login,
                              sd.dop_usl_number port_number,
                              ad.addr_city,
                              ad.addr_street,
                              ad.addr_building,
                              ad.addr_office,
                              case
                                when cl.client_type = 'P' then
                                 ps.person_lastname || ' ' ||
                                 ps.person_firstname || ' ' ||
                                 ps.person_middlename
                                else
                                 j.jur_name
                              end abonent,
                              up.person_lastname || ' ' ||
                              up.person_firstname || ' ' ||
                              up.person_middlename worker,
                              dm.name_mrf mrf,
                              reg.kl_name || ' ' || reg.kl_socr filial,
                              nvl(tar.title, tar_mrf.title) tariff,
                              rs.id order_num,
                              rs.product_category,
                              /*(listagg*/
                              (nvl(tar_c.title, mrf_opt.name_option) /*,
                                                                                                           ', ') WITHIN
                                                                                                   GROUP(order by nvl(tar_c.title,
                                                                                                             mrf_opt.name_option))*/
                              ) options
              --nvl(abs.title, nvl(pack.name, ls.name))
                from t_report_real_conn trrc
                join tr_request_service rs
                  on rs.id = trrc.id_conn
                join tr_request r
                  on r.id = rs.request_id
                join tr_service_product sp
                  on sp.service_id = rs.id
                join tr_request_product rp
                  on rp.id = sp.product_id
                 and rp.type_product = 1
                left join tr_request_service_detail sd
                  on sd.service_id = rs.id
                left join t_dic_request_state d
                  on d.state_id = rs.state_id
                join t_organizations o
                  on o.org_id = r.org_id
                left join t_dic_region reg
                  on reg.reg_id = r.region_id
                left join t_dic_mrf dm
                  on dm.id = reg.mrf_id
                left join t_address ad
                  on ad.addr_id = r.address_id
                join t_clients cl
                  on cl.client_id = r.client_id
                left join t_person ps
                  on ps.person_id = cl.fullinfo_id
                 and cl.client_type = 'P'
                left join t_juristic j
                  on j.juristic_id = cl.fullinfo_id
                 and cl.client_type = 'J'
                left join t_users us
                  on us.usr_id = r.worker_create
                left join t_person up
                  on up.person_id = us.usr_person_id
                left join CONTRACT_T_TARIFF_VERSION tar
                  on tar.id = rp.tar_id
                 and rp.type_tariff = 1
                left join t_mrf_tariff tar_mrf
                  on tar_mrf.id = rp.tar_id
                 and rp.type_tariff = 3
                left join tr_request_price pr
                  on pr.request_id = r.id
                 and pr.service_id = rs.id
                 and pr.is_actual = 1
                left join tr_request_price_options rpo
                  on rpo.req_price_id = pr.id
              ---
                left join tr_product_option o
                  on o.product_id = rp.id
                 and o.service_id = rs.id
                left join contract_t_tariff_option tar_opt
                  on tar_opt.id = o.option_id
                 and pr.type_tariff = 1
                 and tar_opt.alt_name not in
                     ('discount_doubleplay', 'discount_tripleplay')
                left join contract_t_tariff_option_cost tar_c
                  on tar_c.option_id = tar_opt.id
                 and tar_c.tar_ver_id = rp.tar_id
                 and pr.type_tariff = 1
                 and nvl(tar_c.tech_id, nvl(rs.tech_id, -1)) =
                     nvl(rs.tech_id, -1)
                left join t_mrf_tariff_option mrf_opt
                  on mrf_opt.id = o.option_id
                 and pr.type_tariff = 3
               where trrc.id_repper = l_rp_id)
       group by contract_id,
                state_name,
                org_name,
                date_exec,
                telephone_number,
                internet_login,
                port_number,
                addr_city,
                addr_street,
                addr_building,
                addr_office,
                abonent,
                worker,
                mrf,
                filial,
                tariff,
                order_num,
                product_category;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_msg     := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  -------------------------------------------------------------------
  --66222
  -------------------------------------------------------------------
  function Get_Data_for_gph_otchet(pi_rp_id     in number,
                                   pi_worker_id in number,
                                   po_err_num   out number,
                                   po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select distinct contract_id,
                      abonent,
                      dogovor_number,
                      dogovor_date,
                      addr_city,
                      addr_street,
                      addr_building,
                      addr_office,
                      tech_name,
                      tariff,
                      nvl(abon_sum_without_param,
                          nvl(abon_sum1, 0) + nvl(abon_sum2, 0)) as abon_sum,
                      stavka,
                      stavka_proc,
                      sum,
                      date_ch_tech,
                      accrual,
                      (case
                        when (s.sale_device is not null) then
                         9 -- Продажа оборудования
                        when ((select count(*)
                                 from t_stavka_req_fee f
                                where f.id_stavka = s.id) > 0 or
                             (select count(*)
                                 from t_stavka_req_state f
                                where f.id_stavka = s.id
                                  and f.TYPE_STATE = 0) > 0) and
                             category_id = 1 and nvl(s.type_operation,0) = 0 and
                             type_usluga = 1 then
                         1
                        when (select count(*)
                                from t_stavka_req_option sro
                               where sro.id_stavka = s.id
                                 and nvl(sro.type_option, 0) = 0) > 0 then
                         10
                        when (select count(*)
                                from t_stavka_req_option sro
                               where sro.id_stavka = s.id
                                 and sro.type_option = 1) > 0 then
                         15
                        when ((select count(*)
                                 from t_stavka_req_fee f
                                where f.id_stavka = s.id) > 0 or
                             (select count(*)
                                 from t_stavka_req_state f
                                where f.id_stavka = s.id
                                  and f.TYPE_STATE = 0) > 0) and
                             category_id = 5 and nvl(s.type_operation,0) = 0 and
                             type_usluga = 5 then
                         2
                        when ((select count(*)
                                 from t_stavka_req_fee f
                                where f.id_stavka = s.id) > 0 or
                             (select count(*)
                                 from t_stavka_req_state f
                                where f.id_stavka = s.id
                                  and f.TYPE_STATE = 0) > 0) and
                             category_id = 2 and nvl(s.type_operation,0)=0/*is_change_tech<> 1*/  and
                             type_usluga = 2 then
                         3
                        when /*s.is_change_tech <> 1 and*/
                         type_usluga = 8 then
                         12
                        when /*s.is_change_tech <> 1 and*/
                         type_usluga = 14 then
                         14
                        when nvl(s.type_operation,0) = 2/*is_change_tech =1*/ then
                         4
                      end) item,
                      type_usl
        from (select distinct rs.id contract_id,
                              case
                                when cl.client_type = 'P' then
                                 ps.person_lastname || ' ' ||
                                 ps.person_firstname || ' ' ||
                                 ps.person_middlename
                                else
                                 j.jur_name
                              end abonent,
                              tc.contract_num dogovor_number,
                              tc.contract_date dogovor_date,
                              ad.addr_city,
                              ad.addr_street,
                              ad.addr_building,
                              ad.addr_office,
                              (case
                                when tech.id = 56 then
                                 'DSL'
                                else
                                 tech.name
                              end) tech_name,
                              case
                                when stv.type_usluga not in (8, 14) then
                                 nvl(tarr.title, tar_mrf.title)
                                else
                                 nvl(tarr.title, tar_mrf.title) || ';; ' ||
                                 nvl(tarr2.title, tar_mrf2.title) || ';; ' ||
                                 nvl(tarr3.title, tar_mrf3.title)
                              end tariff,
                              max(trp.price_base_fee / 100) keep(dense_rank last order by trp.id) abon_sum1,
                              sum(tro.price_option_fee / 100) keep(dense_rank last order by trp.id) abon_sum2,
                              ra.stavka,
                              ra.stavka_proc,
                              ra.sum,
                              trunc(rs.date_change_state) date_ch_tech,
                              null accrual,
                              ra.kod_stavka,
                              ra.comments type_usl,
                              rs.product_category category_id,
                              sum(trp.price_base_fee) / 100 abon_sum_without_param
                from t_report_period t
                join t_report_abonent ra
                  on ra.rp_id = t.id
                 and ra.type_ab = 2
                left join t_stavka stv
                  on stv.id = ra.kod_stavka
                join tr_request_service rs
                  on rs.id = ra.ab_id
                join tr_request r
                  on r.id = rs.request_id
                join tr_service_product sp
                  on sp.service_id = rs.id
                join tr_request_product rp
                  on rp.id = sp.product_id
                 and rp.type_product = 1
                join t_clients cl
                  on cl.client_id = r.client_id
                left join t_person ps
                  on ps.person_id = cl.fullinfo_id
                 and cl.client_type = 'P'
                left join t_juristic j
                  on j.juristic_id = cl.fullinfo_id
                 and cl.client_type = 'J'
                left join t_address ad
                  on ad.addr_id = r.address_id
                left join t_dic_technology_connect tech
                  on tech.id = rs.tech_id
                join tr_request_product pr
                  on pr.request_id = r.id
                 and pr.type_product = 1
                join tr_service_product sp
                  on sp.product_id = pr.id
                left join tr_request_price trp
                  on trp.service_id = rs.id
                 and trp.is_actual = 1
                left join tr_request_price_options tro
                  on tro.req_price_id = trp.id
                left join tr_request_service trc2
                  on trc2.request_id = r.id
                 and trc2.id <> rs.id
                 and ((stv.type_usluga in (8) and
                     trc2.product_category in (1, 5)) or
                     (stv.type_usluga in (14) and trc2.product_category = 2))
                left join tr_service_product sp2
                  on sp2.service_id = trc2.id
                 and sp2.product_type = 1
                left join tr_request_product rp2
                  on rp2.id = sp2.product_id
                left join tr_request_service trc3
                  on trc3.request_id = r.id
                 and stv.type_usluga in (14)
                 and trc3.product_category = 5
                 and trc3.id <> rs.id
                left join tr_service_product sp3
                  on sp3.service_id = trc3.id
                 and sp3.product_type = 1
                left join tr_request_product rp3
                  on rp3.id = sp3.product_id

                left join CONTRACT_T_TARIFF_VERSION tarr
                  on tarr.id = rp.tar_id
                 and rp.type_tariff = 1
                left join t_mrf_tariff tar_mrf
                  on tar_mrf.id = rp.tar_id
                 and rp.type_tariff = 3
              --
                left join CONTRACT_T_TARIFF_VERSION tarr2
                  on tarr2.id = rp2.tar_id
                 and rp2.type_tariff = 1
                left join t_mrf_tariff tar_mrf2
                  on tar_mrf2.id = rp2.tar_id
                 and rp2.type_tariff = 3
              --
                left join CONTRACT_T_TARIFF_VERSION tarr3
                  on tarr3.id = rp3.tar_id
                 and rp3.type_tariff = 1
                left join t_mrf_tariff tar_mrf3
                  on tar_mrf3.id = rp3.tar_id
                 and rp3.type_tariff = 3
              --
                left join tr_contract_request cr
                  on cr.request_id = r.id
                left join tr_contract tc
                  on tc.id = cr.contract_id
               where t.id = pi_rp_id
                 and t.is_actual = 1
               group by rs.id,
                        case
                          when cl.client_type = 'P' then
                           ps.person_lastname || ' ' || ps.person_firstname || ' ' ||
                           ps.person_middlename
                          else
                           j.jur_name
                        end,
                        tc.contract_num,
                        tc.contract_date,
                        ad.addr_city,
                        ad.addr_street,
                        ad.addr_building,
                        ad.addr_office,
                        (case
                          when tech.id = 56 then
                           'DSL'
                          else
                           tech.name
                        end),
                        case
                          when stv.type_usluga not in (8, 14) then
                           nvl(tarr.title, tar_mrf.title)
                          else
                           nvl(tarr.title, tar_mrf.title) || ';; ' ||
                           nvl(tarr2.title, tar_mrf2.title) || ';; ' ||
                           nvl(tarr3.title, tar_mrf3.title)
                        end,
                        ra.stavka,
                        ra.stavka_proc,
                        ra.sum,
                        trunc(rs.date_change_state),
                        ra.kod_stavka,
                        ra.comments,
                        rs.product_category) tab
        join t_stavka s
          on s.id = tab.kod_stavka
      union
      select a.ab_id contract_id,
             case
               when cl.client_type = 'P' then
                ps.person_lastname || ' ' || ps.person_firstname || ' ' ||
                ps.person_middlename
               else
                j.jur_name
             end,
             null dogovor_number,
             a.ab_dog_date dogovor_date,
             null addr_city,
             null addr_street,
             null addr_building,
             null addr_office,
             null tech_name,
             tar.title tariff,
             a.equipment_cost / 100 abon_sum,
             (case
               when s.optional_equipment not in (4, 7003) then
               /*a.equipment_cost / 100 --*/
                ra.stavka
               else
               /*a.equipment_cost / 100 --*/
                round(ra.stavka / 1.18, 2)
             end) stavka,
             ra.stavka_proc,
             (case
               when s.optional_equipment not in (4, 7003) then
                ra.sum
               else
                ra.sum --round(ra.sum * 1.18, 2)
             end) sum,
             null date_ch_tech,
             null accrual,
             (case
               when s.optional_equipment not in (4, 7003) then
                5
               else
                9
             end) item,
             (case
               when s.optional_equipment not in (4, 7003) then
                null
               when s.optional_equipment in (4) then
                'USB-модем '
               else
                'GSM-телефон '
             end) || '(' || de.name || ')' type_usl
        from t_report_period t
        join t_report_abonent ra
          on ra.rp_id = t.id
        join t_stavka s
          on s.id = ra.kod_stavka
        join t_abonent a
          on a.ab_id = ra.ab_id
         and ra.type_ab = 1
        join t_clients cl
          on cl.client_id = a.client_id
        left join t_person ps
          on ps.person_id = cl.fullinfo_id
         and cl.client_type = 'P'
        left join t_juristic j
          on j.juristic_id = cl.fullinfo_id
         and cl.client_type = 'J'
        left join t_tarif_by_at_id tar
          on tar.at_id = ra.tar_id
        left join t_dic_equipment de
          on de.equipment_id = a.equipment_id
       where t.id = pi_rp_id
         and t.is_actual = 1
      union
      select null contract_id,
             ra.comments abonent,
             d.dog_number dogovor_number,
             d.dog_date dogovor_date,
             null addr_city,
             null addr_street,
             null addr_building,
             null addr_office,
             null tech_name,
             null tariff,
             null abon_sum,
             nvl(ra.stavka, 0) stavka,
             ra.stavka_proc,
             nvl(ra.sum, 0) sum,
             null date_ch_tech,
             null accrual,
             (case
               when nvl(s.new_agent, 0) > 0 then
                7
               else
                6
             end) item,
             null type_usl
        from t_report_period t
        join t_report_abonent ra
          on ra.rp_id = t.id
         and ra.type_ab = 4
        join t_org_relations rel
          on rel.org_id = ra.ab_id
        join t_dogovor d
          on d.org_rel_id = rel.id
        join t_stavka s
          on s.id = ra.kod_stavka
         and (nvl(s.new_agent, 0) > 0 or nvl(s.agent_instruction, 0) > 0)
       where t.id = pi_rp_id
         and t.is_actual = 1
      union
      select null contract_id,
             ra.comments abonent,
             null dogovor_number,
             null dogovor_date,
             null addr_city,
             null addr_street,
             null addr_building,
             null addr_office,
             null tech_name,
             null tariff,
             null abon_sum,
             max(nvl(ra.sum, 0)) stavka,
             ra.stavka_proc,
             sum(nvl(ra.sum, 0)) sum,
             null date_ch_tech,
             null accrual,
             8 item,
             null type_usl
        from t_report_period t
        join t_report_abonent ra
          on ra.rp_id = t.id
         and ra.type_ab = 4
        join t_org_relations rel
          on rel.org_id = ra.ab_id
        join t_dogovor d
          on d.org_rel_id = rel.id
        join t_stavka s
          on s.id = ra.kod_stavka
         and nvl(s.new_agent, 0) = 0
         and nvl(s.agent_instruction, 0) = 0
       where t.id = pi_rp_id
         and t.is_actual = 1
         and exists (select *
                from t_stavka_req_products srp
               where srp.stavka_id = s.id)
       group by ra.comments, ra.stavka_proc;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  -------------------------------------------------------------------
  function charge_activity(pi_org_id       in array_num_2,
                           pi_block        in number,
                           pi_org_relation in num_tab,
                           pi_period_beg   in date,
                           pi_period_end   in date,
                           pi_date         in date,
                           pi_type_date    in number, --0-один месяц активности и период подключения; 1-по выбранному периоду активности и месяцу подключения
                           pi_worker_id    in T_USERS.USR_ID%type,
                           pi_num_page     in number, -- номер страницы
                           pi_count_req    in number, -- кол-во записей на странице
                           pi_column       in number, -- Номер колонки для сортировки
                           pi_sorting      in number, -- 0-по возрастанияю, 1-по убыванию
                           po_all_count    out number, -- выводим общее кол-во записей, подходящих под условия
                           po_full_rep     out sys_refcursor,
                           po_err_num      out pls_integer,
                           po_err_msg      out varchar2) return sys_refcursor is
    res               sys_refcursor;
    org_tab           num_tab := num_tab();
    l_org_tab         num_tab;
    l_activ_dat_st    date;
    l_activ_dat_end   date;
    l_connect_dat_st  date;
    l_connect_dat_end date;
    l_col_list        abonent_activ_list_tab;
    l_order_asc       number;
    l_order_desc      number;
    l_max_num_page    number;
    l_num_page        number;
  begin
    logging_pkg.debug('pi_worker_id := ' || pi_worker_id || ';' || chr(10) ||
                      'pi_period_beg := ' || pi_period_beg || ';' ||
                      chr(10) || 'pi_period_end := ' || pi_period_end || ';' ||
                      chr(10) || 'pi_date := ' || pi_date || ';' ||
                      chr(10) || 'pi_type_date := ' || pi_type_date || ';' ||
                      chr(10) || ':pi_org_relation := ' ||
                      get_str_by_num_tab(pi_org_relation) || ';' ||
                      chr(10) || ':pi_org_id := ' ||
                      get_str_by_array_num_2(pi_org_id) || ';' || chr(10) ||
                      ':pi_block := ' || pi_block || ';' || chr(10) ||

                      ':pi_num_page := ' || pi_num_page || ';' || chr(10) ||
                      ':pi_count_req := ' || pi_count_req || ';' ||
                      chr(10) || ':pi_column := ' || pi_column || ';' ||
                      chr(10) || ':pi_sorting := ' || pi_sorting,
                      c_package || 'charge_activity');
    select t.number_1 bulk collect into l_org_tab from table(pi_org_id) t;

    if (not Security_pkg.Check_Rights_Orgs_str('EISSD.TMC_REPORTS.ABONENT_ACTIVITY_FULL',
                                               l_org_tab,
                                               pi_worker_id,
                                               po_err_num,
                                               po_err_msg,
                                               1 = 1,
                                               1 = 1)) then
      po_full_rep := null;
      return null;
    end if;

    If nvl(pi_sorting, 0) = 0 then
      l_order_asc := nvl(pi_column, 1);
    else
      l_order_desc := nvl(pi_column, 1);
    end If;

    if pi_type_date = 0 then
      --один месяц активности и период подключения;
      l_activ_dat_st  := trunc(pi_date);
      l_activ_dat_end := add_months(trunc(pi_date), 1) - 1 / 24 / 60 / 60;

      l_connect_dat_st  := pi_period_beg /*+ 2 / 24*/
       ;
      l_connect_dat_end := add_months(pi_period_end, 1) - 1 / 24 / 60 / 60 /*+
                                                               2 / 24*/
       ;
    else
      --1-по выбранному периоду активности и месяцу подключения
      l_connect_dat_st  := pi_date /*+ 2 / 24*/
       ;
      l_connect_dat_end := add_months(pi_date, 1) - 1 / 24 / 60 / 60 /*+
                                                               2 / 24*/
       ;

      l_activ_dat_st  := pi_period_beg;
      l_activ_dat_end := add_months(pi_period_end, 1) - 1 / 24 / 60 / 60;
    end if;

    org_tab := get_orgs_tab_for_multiset(pi_orgs         => pi_org_id,
                                         Pi_worker_id    => pi_worker_id,
                                         pi_block        => pi_block,
                                         pi_org_relation => pi_org_relation);

    open res for
      select period.dates,
             sum(count_all) count_all,
             sum(count_activ) count_activ,
             sum(amount_pay) amount_pay,
             period.visible
      -- visible - признак того, что строка вывыодится в верхней таблице (сводная информация по количеству подключенных/активных по месяцам)
      -- строки с visible = 0 используются для вывода итого в нижней таблице
        from (select dates, sum(visibility) visible
                from (Select add_months(trunc(pi_period_beg, 'mm'), level - 1) dates,
                             1 as visibility
                        from dual
                      Connect by level <=
                                 (months_between(pi_period_end, pi_period_beg) + 1)
                      union all
                      select trunc(pi_date, 'mm') as dates, 0 as visisbility
                        from dual)
               group by dates) period
        left join ( -- здесь мы считаем сумму активности абонентов - в ПЕРИОДЕ АКТИВНОСТИ (НЕ ПУТАТЬ С ПЕРИОДОМ ПОДКЛЮЧЕНИЯ)
                   select trunc(a.date_period_start, 'mm') months,
                           0 count_all,
                           /*count(rc.id)*/
                           0 count_activ,
                           sum(a.amount_pay) amount_pay
                     from t_report_real_conn rrc
                     join t_report_period rp
                       on rp.id = rrc.id_repper
                      and rrc.ci = 2
                      and rp.is_actual = 1
                     join t_organizations o
                       on o.org_id = rp.id_org
                     left join t_dic_region_info r
                       on r.reg_id = o.region_id
                     left join t_ab_charge_activity a
                       on (a.request_id = rrc.id_conn or
                          a.usl_id = rrc.id_conn)
                      and nvl(a.asr_id, 0) = nvl(r.charge_source_for_ac, 0)
                      and a.amount_pay <> 0
                      and a.date_period_start between l_activ_dat_st /*- 2 / 24*/
                          and l_activ_dat_end /*- 2 / 24*/
                    where rp.id_org in (select /*+ PRECOMPUTE_SUBQUERY */
                                         column_value org_id
                                          from table(org_tab))
                      and rp.dates between trunc(l_connect_dat_st) and
                          trunc(l_connect_dat_end)
                    group by trunc(a.date_period_start, 'mm')
                   union
                   -- здесь мы считаем количество подключенных и количество активных абонентов ЗА ПЕРИОД ПОДКЛЮЧЕНИЯ (НЕ ПУТАТЬ С ПЕРИОДОМ АКТИВНОСТИ)
                   select trunc(rp.dates) months,
                          count(distinct rrc.id_conn) count_all,
                          count(distinct a.request_id) count_activ,
                          0 amount_pay
                     from t_report_real_conn rrc
                     join t_report_period rp
                       on rp.id = rrc.id_repper
                      and rrc.ci = 2
                      and rp.is_actual = 1
                     join t_organizations o
                       on o.org_id = rp.id_org
                     left join t_dic_region_info r
                       on r.reg_id = o.region_id
                     left join t_ab_charge_activity a
                       on (a.request_id = rrc.id_conn or
                          a.usl_id = rrc.id_conn)
                      and a.amount_pay <> 0
                      and nvl(a.asr_id, 0) = nvl(r.charge_source_for_ac, 0)
                      and a.date_period_start between l_activ_dat_st /*- 2 / 24*/
                          and l_activ_dat_end /*- 2 / 24*/
                    where rp.id_org in (select /*+ PRECOMPUTE_SUBQUERY */
                                         column_value org_id
                                          from table(org_tab))
                      and rp.dates between trunc(l_connect_dat_st) and
                          trunc(l_connect_dat_end)
                    group by trunc(rp.dates)) tt
          on tt.months = period.dates
       group by period.dates, period.visible
       order by 1;

    select abonent_activ_list_type( /*ab_id*/contract_id,
                                   root_org_id,
                                   root_org_name,
                                   org_id,
                                   org_name,
                                   /*callsign*/
                                   order_num,
                                   /*imsi*/
                                   mrf_order_num,
                                   /*date_asr*/
                                   data_create_req,
                                   nvl(amount_pay, 0),
                                   /*amount_place*/
                                   null,
                                   /*tar_id*/
                                   null,
                                   /*tar_name*/
                                   null,
                                   person,
                                   /*abonent*/
                                   null,
                                   /*seller*/
                                   null,
                                   /*seller_emp_num*/
                                   null,
                                   /*data_activated*/
                                   null,
                                   /*date_act*/
                                   date_exec,
                                   /*ab_cost*/
                                   null,
                                   /*ab_paid*/
                                   usl_id,
                                   dates,
                                   rownum) bulk collect
      into l_col_list
      from (select distinct contract_id,
                            order_num,
                            mrf_order_num,
                            ROOT_ORG_ID,
                            ROOT_ORG_NAME,
                            ORG_ID,
                            ORG_NAME,
                            data_create_req,
                            person,
                            amount_pay,
                            dates,
                            date_exec,
                            usl_id
              from (select max(req.id) contract_id,
                           rs.id order_num,
                           rs.mrf_order_num,
                           orgr.org_id ROOT_ORG_ID,
                           (case
                             when mrf.name_mrf is null then
                              orgR.Org_Name
                             else
                              'МРФ ' || mrf.name_mrf
                           end) root_org_name,
                           o.org_id ORG_ID,
                           o.org_name ORG_NAME,
                           rs.date_create - 2 / 24 as data_create_req,
                           trim(p.person_lastname) || ' ' ||
                           trim(p.person_firstname) || ' ' ||
                           trim(p.person_middlename) person,
                           sum(a.amount_pay) amount_pay,
                           period.dates,
                           max(trunc(rs.date_change_state - 2 / 24)) as date_exec,
                           rs.id usl_id
                      from (Select add_months(trunc(pi_period_beg, 'mm'),
                                              level - 1) dates
                              from dual
                            Connect by level <=
                                       (months_between(l_activ_dat_end,
                                                       l_activ_dat_st) + 1)) period
                      join t_report_real_conn rrc
                        on rrc.ci = 2
                      join t_report_period trp
                        on trp.id = rrc.id_repper
                       and trp.is_actual = 1
                      join t_organizations orp
                        on orp.org_id = trp.id_org
                      left join t_dic_region_info ri
                        on ri.reg_id = orp.region_id
                      join tr_request_service rs on rs.id = rrc.id_conn
                      join tr_request req
                        on req.id = rs.request_id
                      left join t_clients cl
                        on cl.client_id = req.client_id
                       and cl.client_type = 'P'
                      left join t_person p
                        on p.person_id = cl.fullinfo_id
                      join t_organizations o
                        on o.org_id = req.org_id
                      left join t_organizations orgR
                        on orgR.Org_Id =
                           nvl(Orgs.Get_Root_Org_Or_Self(o.org_id), 0)
                    -----------
                      left join T_ORGANIZATIONS OOO
                        on o.org_id = ooo.org_id
                      left join t_dic_region reg1
                        on reg1.reg_id = ooo.region_id
                      left join t_dic_mrf mrf
                        on mrf.id = reg1.mrf_id
                       and orgR.Org_Id in (0, 1)
                    --------------
                      left join t_ab_charge_activity a
                        on (rs.id = a.request_id)
                       and ((pi_type_date = 1 and
                           trunc(a.date_period_start) = trunc(period.dates)) or
                           (pi_type_date = 0 and
                           a.date_period_start between l_activ_dat_st and
                           l_activ_dat_end))
                       and a.amount_pay <> 0
                       and nvl(a.asr_id, 0) = nvl(ri.charge_source_for_ac, 0)
                     where trp.id_org in
                           (select /*+ PRECOMPUTE_SUBQUERY */
                             column_value org_id
                              from table(org_tab))
                       and rs.product_category != 7
                       and trp.dates between trunc(l_connect_dat_st) and
                           trunc(l_connect_dat_end)
                     group by rs.id,
                              rs.mrf_order_num,
                              orgr.org_id,
                              (case
                                when mrf.name_mrf is null then
                                 orgR.Org_Name
                                else
                                 'МРФ ' || mrf.name_mrf
                              end),
                              o.org_id,
                              o.org_name,
                              rs.date_create,
                              trim(p.person_lastname) || ' ' ||
                              trim(p.person_firstname) || ' ' ||
                              trim(p.person_middlename),
                              --a.amount_pay,
                              period.dates,
                              rs.id

                    ) aa
             order by decode(l_order_asc,
                             null,
                             null,
                             1,
                             contract_id, --nvl(cp.pack_id, cc.contract_id),
                             2,
                             order_num,
                             3,
                             mrf_order_num,
                             6,
                             to_char(data_create_req, 'yyyy.mm.dd hh24:mi:ss'),
                             7,
                             person,
                             null) asc,
                      decode(l_order_desc,
                             null,
                             null,
                             1,
                             contract_id, --nvl(cp.pack_id, cc.contract_id),
                             2,
                             order_num,
                             3,
                             mrf_order_num,
                             6,
                             to_char(data_create_req, 'yyyy.mm.dd hh24:mi:ss'),
                             7,
                             person,
                             null) desc,
                      decode(pi_column, 2, null, order_num) asc,
                      usl_id,
                      dates asc);

    po_all_count := l_col_list.count;

    l_max_num_page := round(po_all_count / nvl(pi_count_req, 1));

    if (pi_num_page > l_max_num_page and l_max_num_page <> 0) then
      l_num_page := l_max_num_page + 1;
    else
      l_num_page := nvl(pi_num_page, 1);
    end if;

    if pi_type_date = 1 then

      open po_full_rep for
        select AB_ID         contract_id,
               ROOT_ORG_ID,
               ROOT_ORG_NAME,
               ORG_ID,
               ORG_NAME,
               CALLSIGN      order_num,
               IMSI          mrf_order_num,
               DATE_ASR      data_create,
               AMOUNT_PAY,
               person,
               dates,
               date_act      date_exec,
               ab_paid       usl_id,
               rn
          from table(l_col_list) l
         where rn between
               (l_num_page - 1) * nvl(pi_count_req, po_all_count) *
               (months_between(pi_period_end, pi_period_beg) + 1) + 1 and
               (l_num_page) * nvl(pi_count_req, po_all_count) *
               (months_between(pi_period_end, pi_period_beg) + 1)
         order by rn;

      po_all_count := po_all_count /
                      (months_between(pi_period_end, pi_period_beg) + 1);
    else
      open po_full_rep for
        select AB_ID         contract_id,
               ROOT_ORG_ID,
               ROOT_ORG_NAME,
               ORG_ID,
               ORG_NAME,
               CALLSIGN      order_num,
               IMSI          mrf_order_num,
               DATE_ASR      data_create,
               AMOUNT_PAY,
               person,
               dates,
               date_act      date_exec,
               ab_paid       usl_id,
               rn
          from table(l_col_list) l
         where rn between
               (l_num_page - 1) * nvl(pi_count_req, po_all_count) + 1 and
               (l_num_page) * nvl(pi_count_req, po_all_count)
         order by rn;
    end if;

    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error('pi_worker_id=' || pi_worker_id || chr(10) ||
                        po_err_num || po_err_msg,
                        'charge_activity');
      return null;
  end;
end REPORT_PERIOD;
/
