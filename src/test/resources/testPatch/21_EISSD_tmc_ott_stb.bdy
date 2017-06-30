CREATE OR REPLACE package body tmc_ott_stb is
  c_package constant varchar2(30) := 'TMC_OTT_STB';
  -- разделитель строк
  eof varchar2(6) := '<br>';
  c_work_start constant interval day to second := interval '0 00:00:00' day to
                                                  second;
  c_work_end   constant interval day to second := interval '0 23:59:00' day to
                                                  second;
  ---------------------------------------------------
  --получение списка моделей stb по региону
  ---------------------------------------------------
  function Get_dic_model_stb(pi_region_id in varchar2,
                             pi_tmc_type  in number,
                             pi_second_hand in number, -- признак б/у (null - все, 0 - новое, 1 - б/у)
                             pi_worker_id in number,
                             po_err_num   out number,
                             po_err_msg   out varchar2) return sys_refcursor is
    c_pr_name constant varchar2(65) := c_package || '.Get_dic_model_stb';
    res sys_refcursor;
    /****************************************/
    function getStrParam return varchar2 is
    begin
      return 'pi_region_id=' || pi_region_id || ';' 
             || 'pi_worker_id=' || pi_worker_id || ';' 
             || 'pi_tmc_type=' || pi_tmc_type || ';'
             || 'pi_second_hand=' || pi_second_hand;
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    open res for
      select distinct t.id,
                      t.model_stb_id,
                      s.stb_model,
                      t.is_second_hand,
                      dr.kl_region region_id,
                      t.retail_price,
                      t.retail_price_without_nds,
                      t.nomencl_number,
                      dr.kl_name || ' ' || dr.kl_socr reg_name
        from t_ott_stb_model_info t
        join t_stb_model s
          on s.id = t.model_stb_id
        join t_dic_region dr
          on dr.reg_id = t.region_id
       where t.date_end is null
         and (pi_region_id is null or dr.kl_region = pi_region_id)
         and (pi_second_hand is null or t.is_second_hand = pi_second_hand)
         and pi_tmc_type = s.priznak;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(getStrParam || ' ' || po_err_msg, c_pr_name);
      return null;
  end;
  ---------------------------------------------------
  --получение списка моделй стб по конкретной орг-ции
  ---------------------------------------------------
 
  function Get_model_stb_by_Org(pi_org_id      in number,
                                pi_dog_id      in number,
                                pi_tmc_type    in number,
                                pi_second_hand in number, -- признак б/у (null - все, 0 - новое, 1 - б/у)
                                pi_worker_id   in number,
                                po_err_num     out number,
                                po_err_msg     out varchar2)
    return sys_refcursor is
    c_pr_name constant varchar2(65) := c_package || '.Get_model_stb_by_Org';
    res       sys_refcursor;
    l_reg_tab num_tab := num_tab();
    /****************************************/
    function getStrParam return varchar2 is
    begin
      return 'pi_org_id=' || pi_org_id || ';' || 'pi_dog_id=' || pi_dog_id || ';' || 'pi_worker_id=' || pi_worker_id || ';' || 'pi_tmc_type=' || pi_tmc_type || ';' || 'pi_second_hand=' || pi_second_hand;
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    l_reg_tab := orgs.Get_Reg_by_Org(pi_org_id    => pi_org_id,
                                     pi_worker_id => pi_worker_id);
    open res for
      select distinct t.id,
                      t.model_stb_id,
                      s.stb_model,
                      t.region_id,
                      t.retail_price,
                      t.retail_price_without_nds,
                      t.nomencl_number,
                      t.is_second_hand,
                      mo.org_id org_id,
                      mo.dog_id dog_id,
                      nvl(mo.cost_buy,
                          round(d.percent_stb * t.retail_price / 100)) cost_buy,
                      nvl(mo.cost_buy_without_nds,
                          round(d.percent_stb * t.retail_price_without_nds / 100)) cost_buy_without_nds,
                      nvl(mo.cost_deposit,
                          round(d.percent_stb * t.retail_price / 100)) cost_deposit,
                      nvl(mo.cost_deposit_without_nds,
                          round(d.percent_stb * t.retail_price_without_nds / 100)) cost_deposit_without_nds,
                      r.kl_name || ' ' || r.kl_socr reg_name,
                      nvl(mo.is_enabled, 1) is_enabled,
                      mo.payment_is_oper,
                      mo.period_campaign_beg,
                      mo.period_campaign_end,
                      mo.type_delivery
        from t_ott_stb_model_info t
        join t_dic_region r
          on r.reg_id = t.region_id
        join t_stb_model s
          on s.id = t.model_stb_id
        left join t_dogovor d
          on d.dog_id = pi_dog_id
        left join t_ott_stb_model_org mo
          on mo.id_ott_stb = t.id
         and mo.org_id = pi_org_id
         and nvl(d.priznak_stb, 0) = 0
       where t.date_end is null
         and (pi_second_hand is null or t.is_second_hand = pi_second_hand)
         and mo.date_end is null
         and ((mo.org_id is not null and d.priznak_stb = 0) or
             (d.priznak_stb = 1 and
             t.region_id in (select * from table(l_reg_tab))))
         and (pi_dog_id is null or mo.dog_id is null or
             mo.dog_id = pi_dog_id)
         and pi_tmc_type = s.priznak;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(getStrParam || ' ' || po_err_msg, c_pr_name);
      return null;
  end;
  ---------------------------------------------------
  --сохранение стоимостей стб по договору и орг-ции
  ---------------------------------------------------
  procedure save_model_stb_by_Org(pi_org_id                in number,
                                  pi_dog_id                in number,
                                  pi_ott_Stb_Model_org_Tab in ott_Stb_Model_org_Tab,
                                  pi_worker_id             in number,
                                  po_err_num               out number,
                                  po_err_msg               out varchar2) is
    c_pr_name constant varchar2(65) := c_package ||
                                       '.save_model_stb_by_Org';
    /****************************************/
    function getStrParam return varchar2 is
      l_str varchar2(1800);
    begin
      begin
        select substr(listagg(' ID_OTT_STB=' || ID_OTT_STB || ',' ||
                              ' ORG_ID=' || ORG_ID || ',' || ' DOG_ID=' ||
                              DOG_ID || ',' || ' COST_BUY=' || COST_BUY || ',' ||
                              ' COST_BUY_WITHOUT_NDS=' ||
                              COST_BUY_WITHOUT_NDS || ',' ||
                              ' COST_DEPOSIT=' || COST_DEPOSIT || ',' ||
                              ' COST_DEPOSIT_WITHOUT_NDS=' ||
                              COST_DEPOSIT_WITHOUT_NDS || ';') within
                      group(order by id_ott_stb),
                      1,
                      1800)
          into l_str
          from table(pi_ott_Stb_Model_org_Tab);
      exception
        when others then
          null;
      end;
      return substr('pi_org_id=' || pi_org_id || ';' || 'pi_dog_id=' ||
                    pi_dog_id || ';' || 'pi_worker_id=' || pi_worker_id || ';' ||
                    'pi_ott_Stb_Model_org_Tab=' || l_str,
                    1,
                    2000);
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    savepoint sp_begin;
    update t_ott_Stb_Model_org t
       set t.date_end = trunc(sysdate), t.worker_change = pi_worker_id
     where t.org_id = pi_org_id
       and t.dog_id = pi_dog_id
       and t.date_end is null
       /*and t.id_ott_stb in
           (select distinct osmi.id
              from t_ott_stb_model_info osmi, t_stb_model sm
             where osmi.model_stb_id = sm.id
               and pi_tmc_type = sm.priznak)*/;
    insert into t_ott_Stb_Model_org
      (Id_Ott_Stb,
       Org_Id,
       Dog_Id,
       Date_Beg,
       Cost_Buy,
       Cost_Buy_Without_Nds,
       Cost_Deposit,
       Cost_Deposit_Without_Nds,
       worker_id,
       is_enabled,
       PAYMENT_IS_OPER,
       PERIOD_CAMPAIGN_BEG,
       PERIOD_CAMPAIGN_END,
       TYPE_DELIVERY)
      select distinct Id_Ott_Stb,
                      pi_org_id,
                      pi_dog_id,
                      trunc(sysdate),
                      Cost_Buy,
                      Cost_Buy_Without_Nds,
                      Cost_Deposit,
                      Cost_Deposit_Without_Nds,
                      pi_worker_id,
                      is_enabled,
                      PAYMENT_IS_OPER,
                      PERIOD_CAMPAIGN_BEG,
                      PERIOD_CAMPAIGN_END,
                      TYPE_DELIVERY
        from table(pi_ott_Stb_Model_org_Tab);
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(getStrParam || ' ' || po_err_msg, c_pr_name);
      rollback to sp_begin;
  end;
  ---------------------------------------------------
  --принятие тмц
  ---------------------------------------------------
  procedure Add_Ott_STB(pi_org_id    in number,
                        pi_model_id  in number,
                        pi_tmc_tab   in NUM_STR2_TAB, --num1 -пусто,str1-Серийный номер,str2-Mac-адрес
                        pi_tmc_type  in number,
                        pi_worker_id in number,
                        po_message   out varchar2,
                        po_count     out number,
                        po_err       out clob,
                        po_err_num   out number,
                        po_err_msg   out varchar2) is
    c_pr_name constant varchar2(65) := c_package || '.Add_Ott_STB';
    l_op_id      number;
    l_count      number;
    l_double_ser clob;
    l_double_mac clob;
    l_err        clob;
    l_right_str  t_rights.right_string_id%type;
    ex_wrong_tmc_type exception;
    l_msg varchar2(50);
    ex_length exception;
    /****************************************/
    function getStrParam return varchar2 is
      l_str varchar2(1800);
    begin
      begin
        for i in (select * from table(pi_tmc_tab)) loop
          l_str := substr(l_str ||
                          (' num1=' || i.num1 || ',' || ' Серийный номер=' ||
                          i.str1 || ',' || ' Mac-адрес=' || i.str2 || ';'),
                          1,
                          1800);
          if length(l_str) = 1800 then
            raise ex_length;
          end if;
        end loop;
      exception
        when ex_length then
          null;
        when others then
          null;
      end;
      return substr('pi_org_id=' || pi_org_id || ';' || 'pi_model_id=' ||
                    pi_model_id || ';' || 'pi_worker_id=' || pi_worker_id || ';' ||
                    'pi_tmc_type=' || pi_tmc_type || ';' || 'pi_tmc_tab=' ||
                    l_str,
                    1,
                    2000);
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    savepoint sp_begin;
    select max(r.right_string_id)
      into l_right_str
      from T_TMC_TYPE_RIGHTS t
      join t_rights r
        on r.right_id = t.right_id
     where t.tmc_type = pi_tmc_type
       and t.op_type = 18;
    case pi_tmc_type
      when 7004 then
        --l_right_str := 'EISSD.CONNECTIONS.OTT_STB.REGISTER';
        l_msg := 'приставок ОТТ-STB (IPTV 2.0): ';
      when 7005 then
        --l_right_str := 'EISSD.ACCOUNT_TMC.IPTV_DELIVERY.REGISTER';
        l_msg := 'IPTV: ';
      else
        --l_right_str := 'EISSD.ACCOUNT_TMC.IPTV_DELIVERY.REGISTER';
        select max(t.type_name) || ' '
          into l_msg
          from t_dic_tmc_type t
         where t.tmc_type = pi_tmc_type;
    end case;
    if (not Security_pkg.Check_Rights_str(l_right_str,
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      return;
    end if;
    if (is_org_usi(pi_org_id) = 0) then
      po_err_num := 1;
      po_err_msg := 'Организация не является принципалом';
      return;
    end if;
    -- 101341 listagg не влезал в строку
    for i in (select distinct t.serial_number
                from t_tmc_iptv t
                join table(pi_tmc_tab) tab
                  on tab.str1 = t.serial_number
                join t_tmc tt
                  on tt.tmc_id = t.tmc_id
                 and tt.is_deleted = 0
                join t_ott_stb_model_info ot
                  on ot.id = pi_model_id
               where pi_tmc_type = t.priznak
                 and not (t.mac_address = tab.str2 and ot.is_second_hand = 1 and
                     t.state_id = 2)) loop
      l_double_ser := l_double_ser || ', ' || i.serial_number ;
    end loop;
    for i in (select distinct t.mac_address
                from t_tmc_iptv t
                join table(pi_tmc_tab) tab
                  on tab.str2 = t.mac_address
                join t_tmc tt
                  on tt.tmc_id = t.tmc_id
                 and tt.is_deleted = 0
                join t_ott_stb_model_info ot
                  on ot.id = pi_model_id
               where pi_tmc_type = t.priznak
                 and not (t.serial_number = tab.str1 and ot.is_second_hand = 1 and
                     t.state_id = 2)) loop
      l_double_mac := l_double_mac || ', ' || i.mac_address;
    end loop;
    l_err := (case
               when l_double_ser is not null then
                ' Дублирование серийного номера: ' || substr(l_double_ser,2) || '.' || eof
               else
                ''
             end) || (case
               when l_double_mac is not null then
                ' Дублирование Mac-адреса: ' || substr(l_double_mac,2) || '.' || eof
               else
                ''
             end);
    -- Вставляем операцию прихода
    insert into t_tmc_operations
      (op_type, org_id, user_id, op_date, op_comment)
    values
      (18, pi_org_id, pi_worker_id, systimestamp, null)
    returning op_id into l_op_id;
  
    -- Вставляем ТМЦ
    insert all into t_tmc
      (tmc_id, tmc_type, tmc_tmp_cost, tmc_perm, org_id)
    values
      (SEQ_T_TMC.nextval, 7002, retail_price, /*5014*/ null, pi_org_id) into t_tmc_iptv
      (tmc_id,
       stb_model_id,
       serial_number,
       mac_address,
       priznak,
       state_id,
       is_second_hand)
    values
      (SEQ_T_TMC.nextval,
       model_stb_id,
       serial_num,
       mac_address,
       pi_tmc_type,
       1,
       p_second_hand) into t_org_tmc_status
      (tmc_id, org_id, status)
    values
      (SEQ_T_TMC.nextval, pi_org_id, 11) into t_tmc_operation_units
      (op_id,
       tmc_id,
       owner_id_1,
       st_sklad_1,
       error_id,
       state_id,
       tmc_op_cost)
    values
      (l_op_id, SEQ_T_TMC.nextval, pi_org_id, 11, 0, 1, retail_price)
      select str1 serial_num,
             str2 mac_address,
             ot.id model_stb_id,
             ot.is_second_hand p_second_hand,
             ot.retail_price / 100 retail_price --розничная стоимость
        from table(pi_tmc_tab) t
        join t_ott_stb_model_info ot
          on ot.id = pi_model_id
       where not exists
       (select 1
                from t_tmc_iptv ti
                join t_tmc tmc
                  on ti.tmc_id = tmc.tmc_id
               where (ti.serial_number = t.str1 or ti.mac_address = t.str2)
                 and not (ot.is_second_hand = 1 and ti.serial_number = t.str1 and
                     ti.mac_address = t.str2 and ti.state_id = 2)
                 and pi_tmc_type = ti.priznak
                 and tmc.is_deleted = 0);
    l_count := sql%rowcount;
    if l_count = 0 then
      rollback to sp_begin;
    end if;
    po_message := 'Принято ' || l_msg || to_char(l_count / 4) || ' шт.';
    po_err     := l_err;
    po_count   := l_count / 4;
  exception
    when ex_wrong_tmc_type then
      po_err_num := 1;
      po_err_msg := 'Неверный тип ТМЦ';
      logging_pkg.error(getStrParam || ' ' || po_err_msg, c_pr_name);
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(getStrParam || ' ' || po_err_msg, c_pr_name);
      rollback to sp_begin;
  end;
  
  ---------------------------------------------------
  --поиск комплекта ott_stb по маске серийника или мас-адресу
  ---------------------------------------------------
  function Search_TMC_Ott_Stb(pi_serial_number in t_tmc_iptv.serial_number%type,
                              pi_mac_address   in t_tmc_iptv.mac_address%type,
                              pi_state         in num_tab,
                              pi_org_id        in number,
                              pi_model_id      in number,
                              pi_tmc_type      in number,
                              pi_type_op       in number, --20 -перемещение,21-возврат, 22-продажа
                              pi_worker_id     in number,
                              po_err_num       out number,
                              po_err_msg       out varchar2)
    return sys_refcursor is
    c_pr_name constant varchar2(65) := c_package || '.Search_TMC_Ott_Stb';
    res sys_refcursor;
    /****************************************/
    function getStrParam return varchar2 is
    begin
      return 'pi_tmc_type=' || pi_tmc_type || ';' || 'pi_serial_number=' || pi_serial_number || ';' || 'pi_mac_address=' || pi_mac_address || ';' || 'pi_org_id=' || pi_org_id || ';' || 'pi_state=' || get_str_by_num_tab(pi_state) || ';' || 'pi_worker_id=' || pi_worker_id || ';' || 'pi_model_id=' || pi_model_id;
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    open res for
      select distinct t.tmc_id,
                      t.stb_model_id,
                      t.serial_number,
                      t.is_second_hand,
                      t.mac_address
        from t_tmc_iptv t
        join t_org_tmc_status ts
          on ts.tmc_id = t.tmc_id
         and ts.org_id = pi_org_id
        join t_tmc tt
          on t.tmc_id = tt.tmc_id        
        join mv_org_tree tree
          on tree.org_id = ts.org_id
        left join t_dogovor d
          on d.org_rel_id = tree.root_rel_id
        left join t_ott_stb_model_org mo
          on mo.id_ott_stb = t.stb_model_id
         and mo.org_id = /*pi_org_id*/tree.root_org_id --92 замечание по самовывозу
         and (pi_model_id is null or mo.id_ott_stb = pi_model_id)  
       where ((pi_serial_number is not null and
             upper(t.serial_number) like upper(pi_serial_number) || '%') or
             (pi_mac_address is not null and
             upper(t.mac_address) like upper(pi_mac_address) || '%'))
         and t.state_id in (select * from table(pi_state))
         and ((d.priznak_stb = 1) or d.dog_id is null or
             (mo.org_id is not null and d.priznak_stb = 0) or
             (pi_tmc_type = 7005 and d.priznak_stb = 0))
         and nvl(mo.date_end, sysdate) >= sysdate
         and (mo.is_enabled = 1 or nvl(mo.is_enabled, 0) = 0 or
             d.priznak_stb = 1)
         and (pi_tmc_type is null or
             (pi_tmc_type = t.priznak))
         and (pi_type_op <> 22 or
             (pi_type_op = 22 and
             (pi_model_id is null or
             (t.stb_model_id = pi_model_id and
             (tt.tmc_perm is null or
             (tt.tmc_perm in (5014, 5017, 5018, 5019, 5015) and
             d.dog_class_id in (10, 11, 12))))) -- доставка
             and (pi_tmc_type is null or
             (((tt.tmc_perm is null and d.dog_id is null) or
             tt.tmc_perm in (5014, 5015))))))
       order by t.serial_number;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(getStrParam || ' ' || po_err_msg, c_pr_name);
      return null;
  end;
  ---------------------------------------------------
  --для вывода по кнопке"Проверка" в перемещениях
  ---------------------------------------------------
  function Check_TMC_OTT_STB_tab(pi_org_id    in number,
                                 pi_dog_id    in number,
                                 pi_tmc_tab   in NUM_STR2_TAB, --num1 -тип тцм,str1-Серийный номер,str2-Mac-адрес
                                 pi_state     in num_tab,
                                 pi_type_op   in number, --20 -перемещение,21-возврат, 22-продажа
                                 pi_tmc_Perm  in number, --назначение
                                 pi_worker_id in number,
                                 po_err       out clob,
                                 po_err_num   out number,
                                 po_err_msg   out varchar2)
    return sys_refcursor is
    c_pr_name constant varchar2(65) := c_package ||
                                       '.Check_TMC_OTT_STB_tab';
    res    sys_refcursor;
    l_perm number;
    l_err  varchar2(4000);
    l_err1 varchar2(4000);
    l_err2 varchar2(4000);
    err    varchar2(4000);
    err1   varchar2(4000);
    err2   varchar2(4000);
    l_err3 varchar2(4000);
    l_err4 varchar2(4000);
    ex_length exception;
    pi_tmc_type number;
    /****************************************/
    function getStrParam return varchar2 is
      l_str varchar2(1800);
    begin
      begin
        for i in (select * from table(pi_tmc_tab)) loop
          l_str := substr(l_str ||
                          (' num1=' || i.num1 || ',' || ' Серийный номер=' ||
                          i.str1 || ',' || ' Mac-адрес=' || i.str2 || ';'),
                          1,
                          1800);
          if length(l_str) = 1800 then
            raise ex_length;
          end if;
        end loop;
      exception
        when ex_length then
          null;
        when others then
          null;
      end;
      return substr('pi_org_id=' || pi_org_id || ';' || 'pi_dog_id=' ||
                    pi_dog_id || ';' || 'pi_type_op=' || pi_type_op || ';' ||
                    'pi_state=' || get_str_by_num_tab(pi_state) || ';' ||
                    'pi_tmc_tab=' || l_str || ';' || 'pi_worker_id=' ||
                    pi_worker_id || ';' || 'pi_tmc_perm=' || pi_tmc_perm,
                    1,
                    2000);
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    if nvl(pi_type_op, 0) <> 20 then
      open res for
        select distinct tip.stb_model_id,
                        nvl(i.retail_price, tt.tmc_tmp_cost * 100) retail_price,
                        nvl(tip.cost_buy * 100,
                            nvl(mo.cost_buy,
                                round(d.percent_stb * i.retail_price / 100, 2))) cost_buy,
                        nvl(tip.cost_deposit * 100,
                            nvl(mo.cost_deposit,
                                round(d.percent_stb * i.retail_price / 100, 2))) cost_deposit,
                        dic.stb_model,
                        i.is_second_hand,
                        i.region_id,
                        reg.kl_name reg_name,
                        count(*) kolvo
          from table(pi_tmc_tab) tab
          join t_tmc_iptv tip
            on (tip.serial_number = tab.str1 or tip.mac_address = tab.str2)
           and tab.num1 = tip.priznak
          join t_tmc tt
            on tt.tmc_id = tip.tmc_id
          join t_org_tmc_status ts
            on ts.tmc_id = tip.tmc_id
              --and ts.status = 11
           and ts.org_id = pi_org_id
          left join t_dogovor d
            on d.dog_id = pi_dog_id
          left join t_org_relations r
            on r.id = d.org_rel_id
          left join t_organizations o
            on o.org_id = r.org_id
          left join t_ott_stb_model_info i
            on i.id = tip.stb_model_id
          join t_stb_model dic
            on dic.id = i.model_stb_id --tip.stb_model_id
          left join t_ott_stb_model_org mo
            on mo.dog_id = pi_dog_id
           and mo.date_end is null
           and mo.id_ott_stb = i.id
           and d.priznak_stb = 0
           and mo.type_delivery = case
                 when nvl(pi_tmc_Perm, tt.tmc_perm) in (5017, 5019) then
                  2
                 else
                  1
               end --добавила проверку направлений
          left join t_dic_region reg
            on reg.reg_id = nvl(i.region_id, o.region_id)
         where tip.state_id in (select * from table(pi_state))
           and ((d.priznak_stb = 1) or d.dog_id is null or
               (mo.org_id is not null and d.priznak_stb = 0) or
               (tab.num1 = 7005 and d.priznak_stb = 0))
           and nvl(mo.date_end, sysdate) >= sysdate
           and (mo.is_enabled = 1 or mo.is_enabled is null or
               d.priznak_stb = 1)
           and (tip.state_id = 2 and tt.tmc_perm = 5007 and
               nvl(pi_type_op, 0) in (21, 530) or tip.state_id != 2)
         group by tip.stb_model_id,
                  nvl(i.retail_price, tt.tmc_tmp_cost * 100),
                  nvl(tip.cost_buy * 100,
                      nvl(mo.cost_buy,
                          round(d.percent_stb * i.retail_price / 100, 2))),
                  nvl(tip.cost_deposit * 100,
                      nvl(mo.cost_deposit,
                          round(d.percent_stb * i.retail_price / 100, 2))),
                  dic.stb_model,
                  i.is_second_hand,
                  i.region_id,
                  reg.kl_name;
    else
      open res for
        select distinct tip.stb_model_id,
                        nvl(i.retail_price, tt.tmc_tmp_cost * 100) retail_price,
                        nvl(nvl(mo.cost_buy,
                                round(d.percent_stb * i.retail_price / 100,
                                      2)),
                            tip.cost_buy * 100) cost_buy,
                        nvl(nvl(mo.cost_deposit,
                                round(d.percent_stb * i.retail_price / 100,
                                      2)),
                            tip.cost_deposit * 100) cost_deposit,
                        dic.stb_model,
                        i.is_second_hand,
                        i.region_id,
                        reg.kl_name reg_name,
                        count(*) kolvo
          from table(pi_tmc_tab) tab
          join t_tmc_iptv tip
            on (tip.serial_number = tab.str1 or tip.mac_address = tab.str2)
           and tab.num1 = tip.priznak
          join t_tmc tt
            on tt.tmc_id = tip.tmc_id
          join t_org_tmc_status ts
            on ts.tmc_id = tip.tmc_id
           and ts.org_id = pi_org_id
          left join t_dogovor d
            on d.dog_id = pi_dog_id
          left join t_org_relations r
            on r.id = d.org_rel_id
          left join t_organizations o
            on o.org_id = r.org_id
          left join t_ott_stb_model_info i
            on i.id = tip.stb_model_id
          join t_stb_model dic
            on dic.id = i.model_stb_id --tip.stb_model_id
          left join t_ott_stb_model_org mo
            on mo.dog_id = pi_dog_id
           and mo.date_end is null
           and mo.id_ott_stb = i.id
           and d.priznak_stb = 0
           and mo.type_delivery = case
                 when nvl(pi_tmc_Perm, tt.tmc_perm) in (5017, 5019) then
                  2
                 else
                  1
               end
          left join t_dic_region reg
            on reg.reg_id = nvl(i.region_id, o.region_id)
         where tip.state_id in (select * from table(pi_state))
           and ((d.priznak_stb = 1) or d.dog_id is null or
               (mo.org_id is not null and d.priznak_stb = 0) or
               (tab.num1 = 7005 and d.priznak_stb = 0))
           and nvl(mo.date_end, sysdate) >= sysdate
           and (mo.is_enabled = 1 or mo.is_enabled is null or
               d.priznak_stb = 1)
         group by tip.stb_model_id,
                  nvl(i.retail_price, tt.tmc_tmp_cost * 100),
                  nvl(nvl(mo.cost_buy,
                          round(d.percent_stb * i.retail_price / 100, 2)),
                      tip.cost_buy * 100),
                  nvl(nvl(mo.cost_deposit,
                          round(d.percent_stb * i.retail_price / 100, 2)),
                      tip.cost_deposit * 100),
                  dic.stb_model,
                  i.is_second_hand,
                  i.region_id,
                  reg.kl_name;
    end if;
    ----
    select listagg(eof || 'серийный номер ' || serial_number ||
                   ', mac-адрес ' || mac_address,
                   '.') WITHIN GROUP(order by serial_number)
      into l_err
      from (select str1 serial_number, str2 mac_address
              from table(pi_tmc_tab) tab
            minus
            select str1 serial_number, str2 mac_address
              from table(pi_tmc_tab) tab
              join t_tmc_iptv tip
                on (tip.serial_number = tab.str1 or
                   tip.mac_address = tab.str2)
               and tab.num1 = tip.priznak
              join t_tmc tt
                on tt.tmc_id = tip.tmc_id
              join t_org_tmc_status ts
                on ts.tmc_id = tip.tmc_id
               and ts.org_id = pi_org_id
              left join t_dogovor d
                on d.dog_id = pi_dog_id
             where tip.state_id in (select * from table(pi_state)))
     where rownum < 56;
    ----
    select listagg(eof || 'серийный номер ' || serial_number ||
                   ', mac-адрес ' || mac_address,
                   '.') WITHIN GROUP(order by serial_number)
      into l_err1
      from (select str1 serial_number, str2 mac_address --tip.serial_number, tip.mac_address
              from table(pi_tmc_tab) tab
              join t_tmc_iptv tip
                on (tip.serial_number = tab.str1 or
                   tip.mac_address = tab.str2)
               and tab.num1 = tip.priznak
              join t_tmc tt
                on tt.tmc_id = tip.tmc_id
              join t_org_tmc_status ts
                on ts.tmc_id = tip.tmc_id
                  --and ts.status = 11
               and ts.org_id = pi_org_id
              left join t_dogovor d
                on d.dog_id = pi_dog_id
             where tip.state_id in (select * from table(pi_state))
            minus
            select str1 serial_number, str2 mac_address --tip.serial_number, tip.mac_address
              from table(pi_tmc_tab) tab
              join t_tmc_iptv tip
                on (tip.serial_number = tab.str1 or
                   tip.mac_address = tab.str2)
               and tab.num1 = tip.priznak
              join t_tmc tt
                on tt.tmc_id = tip.tmc_id
              join t_org_tmc_status ts
                on ts.tmc_id = tip.tmc_id
               and ts.org_id = pi_org_id
              left join t_dogovor d
                on d.dog_id = pi_dog_id
              left join t_org_relations r
                on r.id = d.org_rel_id
              left join t_organizations o
                on o.org_id = r.org_id
              left join t_ott_stb_model_info i
                on i.id = tip.stb_model_id
              join t_stb_model dic
                on dic.id = i.model_stb_id
              left join t_ott_stb_model_org mo
                on mo.dog_id = pi_dog_id
               and mo.date_end is null
               and mo.id_ott_stb = i.id
               and mo.type_delivery = case
                     when nvl(pi_tmc_Perm, tt.tmc_perm) in (5017, 5019) then
                      2
                     else
                      1
                   end
              left join t_dic_region reg
                on reg.reg_id = nvl(i.region_id, o.region_id)
             where ts.tmc_id is not null
               and (d.priznak_stb = 1 or d.dog_id is null or
                   (mo.is_enabled is not null and d.priznak_stb = 0))
               and nvl(mo.date_end, sysdate) >= sysdate)
     where rownum < 56;
    -- Анализ назначения
    select listagg(eof || 'серийный номер ' || serial_number ||
                   ', mac-адрес ' || mac_address,
                   '.') WITHIN GROUP(order by serial_number)
      into l_err2
      from (select str1 serial_number, str2 mac_address
              from table(pi_tmc_tab) tab
            minus
            select distinct serial_number, mac_address
              from (select str1 serial_number, str2 mac_address
                      from table(pi_tmc_tab) tab
                      join t_tmc_iptv tip
                        on (tip.serial_number = tab.str1 or
                           tip.mac_address = tab.str2)
                       and tab.num1 = tip.priznak
                      join t_tmc tt
                        on tt.tmc_id = tip.tmc_id
                      left join t_dogovor d
                        on d.dog_id = pi_dog_id
                      left join t_dogovor_prm dp
                        on d.dog_id = dp.dp_dog_id
                       and dp.dp_is_enabled = 1
                      left join t_perm_to_perm pp
                        on pp.perm_dog = dp.dp_prm_id
                     where (tt.tmc_perm is null or d.dog_id is null or
                           pp.direction = tt.tmc_perm)))
     where rownum < 56;
    -- Анализ назначения для продажи
    if pi_type_op = 22 then
      select listagg(eof || 'серийный номер ' || serial_number ||
                     ', mac-адрес ' || mac_address,
                     '.') WITHIN GROUP(order by serial_number)
        into l_err2
        from (select str1 serial_number, str2 mac_address
                from table(pi_tmc_tab) tab
              minus
              select distinct serial_number, mac_address
                from (select str1 serial_number, str2 mac_address
                        from table(pi_tmc_tab) tab
                        join t_tmc_iptv tip
                          on (tip.serial_number = tab.str1 or
                             tip.mac_address = tab.str2)
                         and tab.num1 = tip.priznak
                        join t_org_tmc_status ots
                          on ots.tmc_id = tip.tmc_id
                        join t_tmc tt
                          on tt.tmc_id = tip.tmc_id
                        left join t_dogovor d
                          on d.dog_id = pi_dog_id
                        left join t_dogovor_prm dp
                          on d.dog_id = dp.dp_dog_id
                         and dp.dp_is_enabled = 1
                      --left join t_perm_to_perm pp on pp.perm_dog = dp.dp_prm_id
                       where ((tt.tmc_perm is null and
                             is_org_usi(ots.org_id) = 1) or
                             5014 = tt.tmc_perm)))
       where rownum < 56; -- чистая продажа
    end if;
  
    --проверим назначение для тмц
    if pi_type_op = 20 and pi_tmc_Perm is not null then
      select listagg(eof || 'серийный номер ' || serial_number ||
                     ', mac-адрес ' || mac_address,
                     '.') WITHIN GROUP(order by serial_number)
        into l_err3
        from (select str1 serial_number, str2 mac_address
                from table(pi_tmc_tab) tab
              minus
              select distinct serial_number, mac_address
                from (select distinct serial_number, mac_address
                        from (select str1 serial_number, str2 mac_address
                                from table(pi_tmc_tab) tab
                                join t_tmc_iptv tip
                                  on (tip.serial_number = tab.str1 or
                                     tip.mac_address = tab.str2)
                                 and tab.num1 = tip.priznak
                                join T_DIC_TMC_TYPE_REL_PRM p
                                  on p.tmc_type = tab.num1
                                join t_perm_to_perm pp
                                  on pp.perm_dog = p.prm_id
                               where pp.direction = pi_tmc_Perm)))
       where rownum < 56;
    end if;
    -- проверка возможности обратного выкупа
    -- нет дубликатов в статусе "Принято"
    -- не было вознаграждений по продаже
    -- продажа была менее 1 месяца назад
    -- stepanov-km 09.03.2017
    select max(t.tmc_perm)
      into l_perm
      from t_tmc t
     where t.tmc_id in (select tip.tmc_id
                          from table(pi_tmc_tab) tab
                          join t_tmc_iptv tip
                            on (tip.serial_number = tab.str1 or
                               tip.mac_address = tab.str2)
                           and tab.num1 = tip.priznak);
    if (pi_type_op = 21 and l_perm = 5007) or
       (pi_type_op = 530 and pi_tmc_Perm = 5007) then
      select listagg(eof || 'серийный номер ' || serial_number ||
                     ', mac-адрес ' || mac_address,
                     '.') WITHIN GROUP(order by serial_number)
        into l_err4
        from (select str1 serial_number, str2 mac_address
                from table(pi_tmc_tab) tab
              minus
              select tab.serial_number, tab.mac_address
                     -- если одинаковые серийник и мак-адрес смотрим только ТМЦ с последней операцией
                       from (select max(tab1.tmc_id) keep(dense_rank last order by top.op_id) as tmc_id,
                                    tab1.mac_address,
                                    tab1.serial_number,
                                    tab1.priznak as tmc_type,
                                    max(ra.sum) as op_fee, -- сумма вознаграждений по продаже OTT-STB
                                    max(top.op_date) keep(dense_rank last order by top.op_id) as op_date,
                                    max(ip2.tmc_id) keep(dense_rank last order by top.op_id) as duplicate
                               from (select max(tun.owner_id_0) keep(dense_rank last order by tun.unit_id) owner_id_0,
                                            max(tun.op_id) keep(dense_rank last order by tun.unit_id) op_id,
                                            ip.tmc_id,
                                            ip.mac_address,
                                            ip.serial_number,
                                            ip.priznak,
                                            ip.is_second_hand,
                                            tt.tmc_tmp_cost
                                       from table(pi_tmc_tab) tt
                                       join t_tmc_iptv ip
                                         on (ip.serial_number = tt.str1 or
                                            ip.mac_address = tt.str2)
                                        and tt.num1 = ip.priznak
                                       left join t_tmc tt
                                         on tt.tmc_id = ip.tmc_id
                                       left join t_tmc_operation_units tun
                                         on ip.tmc_id = tun.tmc_id
                                        and ip.tmc_id is not null
                                      group by ip.tmc_id,
                                               ip.mac_address,
                                               ip.serial_number,
                                               ip.priznak,
                                               ip.is_second_hand,
                                               tt.tmc_tmp_cost) tab1
                               left join t_tmc_operations top
                                 on top.op_id = tab1.op_id
                                and top.op_type = 541
                               left join t_report_abonent ra
                                 on ra.ab_id = tab1.tmc_id
                               left join t_tmc_iptv ip2
                                 on tab1.serial_number = ip2.serial_number
                                and tab1.mac_address = ip2.mac_address
                                and ip2.tmc_id <> tab1.tmc_id
                                and ip2.priznak = tab1.priznak
                                and ip2.state_id = 1
                              group by tab1.mac_address,
                                       tab1.serial_number,
                                       tab1.priznak) tab
                      where tab.op_fee is null
                        and (months_between(sysdate, tab.op_date) < 1 or tab.op_date is null)
                        and duplicate is null
              );
    end if;
  
    if l_err is not null then
      err := 'Данные ТМЦ не найдены на складе организации в нужном статусе:' ||
             l_err;
    end if;
    if l_err1 is not null or l_err3 is not null then
      err1 := eof ||
              'Данные модели ТМЦ нельзя перемещать на указанную организацию-получателя:' ||
              nvl(l_err1, l_err3);
    end if;
    if l_err2 is not null then
      err2 := eof || 'Данные ТМЦ имеют другое направление перемещения:' ||
              l_err2;
    end if;
    if l_err4 is not null then
      l_err4 := eof ||
              'Для данных ТМЦ недоступна операция обратного выкупа:' ||
              l_err4;
    end if;    
    if l_err is not null or l_err1 is not null or l_err2 is not null or
       l_err3 is not null or l_err4 is not null then
      po_err := err || err1 || err2 || l_err4;
    end if;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(getStrParam || ' ' || po_err_msg, c_pr_name);
      return null;
  end;
  ---------------------------------------------------
  --перемещение OTT_STB
  ---------------------------------------------------
  Function Move_OTT_STB(pi_operation in new_operation,
                        -- pi_tmc_type  in number,
                        pi_worker_id in T_USERS.USR_ID%type,
                        po_msg       out CLOB,
                        po_err_num   out pls_integer,
                        po_err_msg   out Varchar2) return number is
    c_pr_name constant varchar2(65) := c_package || '.MOVE_OTT_STB';
    l_operation       new_operation;
    l_tmc_op_tab      tmc_op_tab;
    l_tmc_op_type     tmc_op_type;
    l_tmc_type        number;
    l_ont_ser         varchar2(32);
    l_cost            number;
    l_perm            number;
    l_tmc_dog         number;
    l_op_type         number;
    l_owner           number;
    cnt               number := 0;
    l_tmc_id          number;
    l_tmc_cancel      num_tab; -- тмц досупные для отмены продажи
    l_Tmc_op_new      tmc_Op_tab := tmc_op_tab(); -- коллекция для отмены продажи
    l_op_id           number;
    l_Tmc_Collect     tmc_Op_tab; -- Коллекция Имси
    l_move            number;
    l_org_previos     number;
    l_dog_id          number;
    l_cost_retail     number;
    l_cost_buy        number;
    l_cost_deposit    number;
    l_count_wrong_tmc number; -- количество тмц не доступных для возврата
    l_right_str       string_tab;
    ex_wrong_tmc_type       exception;
    ex_not_available_return exception;
    l_msg varchar2(2000);
    --l_dog_perm number;
    /****************************************/
    function getStrParam return varchar2 is
      l_str varchar2(1800);
    begin
      begin
        select substr(LISTAGG(' tmc_id=' || tmc_id || '; tmc_type_Id=' ||
                              tmc_type_Id || '; real_owner_id=' ||
                              real_owner_id || '; St_Sklad=' || St_Sklad ||
                              '; imsi=' || imsi || '; callsign=' ||
                              callsign || '; Tmc_Op_Cost=' || Tmc_Op_Cost ||
                              '; tmc_Perm=' || tmc_Perm || '; tmc_Dog=' ||
                              tmc_Dog || '; iccid=' || iccid) WITHIN
                      GROUP(ORDER BY tmc_id),
                      1,
                      1800)
          into l_str
          from table(pi_operation.opunits);
      exception
        when others then
          null;
      end;
      return substr('pi_operation.opunits=' || l_str ||
                    ' pi_operation.operationId=' ||
                    pi_operation.operationId || ' pi_operation.Optype=' ||
                    pi_operation.Optype || ' pi_operation.org_Id=' ||
                    pi_operation.org_Id || ' pi_operation.userId=' ||
                    pi_operation.userId || ' pi_operation.webWorkerId=' ||
                    pi_operation.webWorkerId || ' pi_operation.ownerId_0=' ||
                    pi_operation.ownerId_0 || ' pi_operation.ownerId_1=' ||
                    pi_operation.ownerId_1 || ' pi_operation.tmcType=' ||
                    pi_operation.tmcType || ' pi_operation.dog_id=' ||
                    pi_operation.dog_id || ' pi_operation.move_prm=' ||
                    pi_operation.move_prm || ';' || 'pi_worker_id=' ||
                    pi_worker_id,
                    1,
                    2000);
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.info(getStrParam, c_pr_name);
    savepoint sp_begin;
    l_operation  := pi_operation;
    l_op_type    := pi_operation.optype;
    l_tmc_op_tab := tmc_op_tab();
    select r.right_string_id bulk collect
      into l_right_str
      from T_TMC_TYPE_RIGHTS t
      join t_rights r
        on r.right_id = t.right_id
     where t.tmc_type = pi_operation.tmcType -- pi_tmc_type
       and t.op_type in (20, 21); --перемещение,возврат
  
    case pi_operation.tmcType -- pi_tmc_type
      when 7004 then
        --l_right_str := string_tab('EISSD.ACCOUNT_TMC.OTT_STB.MOVE', 'EISSD.ACCOUNT_TMC.OTT_STB.BACK');
        l_msg := 'Комплекты ОТТ-STB (IPTV 2.0) ';
      when 7005 then
        --l_right_str := string_tab('EISSD.ACCOUNT_TMC.IPTV_DELIVERY.MOVE', 'EISSD.ACCOUNT_TMC.IPTV_DELIVERY.BACK');
        l_msg := 'IPTV ';
      else
        --l_right_str := string_tab('EISSD.ACCOUNT_TMC.IPTV_DELIVERY.MOVE', 'EISSD.ACCOUNT_TMC.IPTV_DELIVERY.BACK');
        select max(t.type_name) || ' '
          into l_msg
          from t_dic_tmc_type t
         where t.tmc_type = pi_operation.tmcType /*pi_tmc_type*/
        ;
    end case;
  
    if l_op_type = 20 then
      l_owner := pi_operation.ownerid_0;
    else
      l_owner := pi_operation.ownerid_1;
    end if;
    -- права на выполнение операции
    if (not Security_pkg.Check_Any_Rights_str(l_right_str, --Перемещение IPTV
                                              l_owner,
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg,
                                              false,
                                              true)) then
      return null;
    end if;
  
    for i in (select distinct ip.tmc_id,
                              t.tmc_tmp_cost,
                              t.org_id,
                              ip.stb_model_id
                from t_tmc_iptv ip
                join t_tmc t
                  on t.tmc_id = ip.tmc_id
                 and (ip.state_id <> 2 or l_op_type <> 20)
                join t_org_tmc_status ots
                  on ots.tmc_id = t.tmc_id
              --and ots.status = 11
                join table(pi_operation.opunits) tab
                  on (tab.imsi = ip.serial_number or
                     tab.iccid = ip.mac_address)
                 and ip.priznak = pi_operation.tmcType -- pi_tmc_type
               where t.org_id in (l_owner)
                 and t.is_deleted = 0) loop
      cnt      := cnt + 1;
      l_tmc_id := i.tmc_id;
    
      l_tmc_type := 7002;
      l_ont_ser  := i.tmc_id;
      l_cost := case
                  when pi_operation.Optype = 20 and pi_operation.tmcType /* pi_tmc_type */
                       not in (7005) and (nvl(pi_operation.dog_id, -1) > 0 or
                       is_org_usi(i.org_id) = 1) then
                   TMC.Get_Model_Cost(pi_tmc_id       => i.tmc_id,
                                      pi_stb_model_id => i.stb_model_id,
                                      pi_org_id       => i.org_id,
                                      pi_dog_id       => pi_operation.dog_id,
                                      pi_type_cost    => 1,
                                      pi_type_op      => pi_operation.Optype)
                  else
                   i.tmc_tmp_cost
                end;
      l_perm     := pi_operation.move_prm;
    
      l_tmc_dog := pi_operation.dog_id;
      if l_tmc_dog is not null then
        begin
          select distinct pp.direction
            into l_perm
            from t_dogovor td
            join t_dogovor_prm dp
              on td.dog_id = dp.dp_dog_id
            join t_perm_to_perm pp
              on pp.perm_dog = dp.dp_prm_id
            join t_dic_tmc_type_rel_prm rel
              on rel.prm_id = dp.dp_prm_id
           where td.dog_id = l_tmc_dog
             and dp.dp_is_enabled = 1
             and rel.tmc_type = pi_operation.tmcType;
        exception
          when no_data_found then
            null;
          when others then
            null;
        end;
      end if;
      if is_org_usi(pi_operation.ownerId_0) = 1 and
         pi_operation.Optype = 21 then
        l_perm := null;
      else
        if pi_operation.move_prm is not null then
          l_perm := pi_operation.move_prm;
        else
          if l_op_type = 20 and is_org_usi(pi_operation.ownerId_1) = 0 and
             is_org_usi(pi_operation.ownerId_0) = 0 then
            --перемещение с договорной на договорную
            select t.tmc_perm
              into l_perm
              from t_tmc t
             where t.tmc_id = l_tmc_id;
          end if;
        end if;
      end if;
      l_tmc_op_type := tmc_op_type(l_tmc_id,
                                   l_tmc_type,
                                   null,
                                   null,
                                   null,
                                   11,
                                   null,
                                   l_ont_ser,
                                   null,
                                   l_cost,
                                   l_perm,
                                   l_tmc_dog,
                                   null,
                                   null,
                                   null);
      l_tmc_op_tab.extend;
      l_tmc_op_tab(l_tmc_op_tab.count) := l_tmc_op_type;
    end loop;
    l_operation.opunits  := l_tmc_op_tab;
    l_operation.move_prm := l_perm;
  
    if cnt = 0 then
      po_err_num := 1;
      po_err_msg := l_msg || 'не найдены на складе организации.';
      return null;
    else
      if l_op_type = 20 then
        l_move := tmc.move_tmc(l_operation,
                               pi_worker_id,
                               po_msg,
                               po_err_num,
                               po_err_msg);
        if l_move > 0 and pi_operation.tmcType /*pi_tmc_type*/
           not in (7005) then
          for ora_record in (select ti.tmc_id, ti.stb_model_id, tm.org_id
                               from t_tmc_iptv ti
                               join t_tmc tm
                                 on tm.tmc_id = ti.tmc_id
                              where ti.tmc_id in
                                    (select tt.tmc_id
                                       From Table(l_tmc_op_tab) tt)
                                and ti.priznak = pi_operation.tmcType -- pi_tmc_type
                             ) loop
            l_cost_retail := TMC.Get_Model_Cost(pi_tmc_id       => ora_record.tmc_id,
                                                pi_stb_model_id => ora_record.stb_model_id,
                                                pi_org_id       => ora_record.org_id,
                                                pi_dog_id       => l_operation.dog_id,
                                                pi_type_cost    => 1,
                                                pi_type_op      => pi_operation.Optype);
            update t_tmc t
               set t.tmc_tmp_cost = l_cost_retail, t.tmc_perm = l_perm
             where t.tmc_id = ora_record.tmc_id;
          
            if l_operation.dog_id is not null then
              if l_operation.move_prm in (5007) then
                l_cost_buy := TMC.get_Model_Cost(pi_tmc_id       => ora_record.tmc_id,
                                                 pi_stb_model_id => ora_record.stb_model_id,
                                                 pi_org_id       => ora_record.org_id,
                                                 pi_dog_id       => l_operation.dog_id,
                                                 pi_type_cost    => 2,
                                                 pi_type_op      => pi_operation.Optype);
              elsif l_operation.move_prm = 5014 then
                l_cost_deposit := TMC.get_Model_Cost(pi_tmc_id       => ora_record.tmc_id,
                                                     pi_stb_model_id => ora_record.stb_model_id,
                                                     pi_org_id       => ora_record.org_id,
                                                     pi_dog_id       => l_operation.dog_id,
                                                     pi_type_cost    => 3,
                                                     pi_type_op      => pi_operation.Optype);
              end if;
              update t_tmc_iptv t
                 set t.cost_buy     = l_cost_buy,
                     t.cost_deposit = l_cost_deposit
               where t.tmc_id = ora_record.tmc_id;
            end if;
          end loop;
        
          if l_perm = 5007 then
            --если Направление использования OTT-STB: продажа,регистрируем операцию продажи дилеру
            l_Tmc_Collect := pi_operation.FilUnitsAll;
          
            select max(tou.owner_id_0) keep(dense_rank last order by tou.unit_id),
                   max(tto.op_dog_id) keep(dense_rank last order by tou.unit_id)
              into l_org_previos, l_dog_id
              from t_tmc_operations tto
              join t_tmc_operation_units tou
                on tto.op_id = tou.op_id
             where tto.op_type = 20
               and tou.tmc_id in
                   (select tt.tmc_id From Table(l_tmc_op_tab) tt);
            insert into t_tmc_operations
              (op_type, op_date, org_id, user_id, op_dog_id)
            values
              (541,
               pi_operation.opDate,
               pi_operation.org_Id,
               pi_worker_id,
               nvl(l_dog_id, pi_operation.dog_id))
            returning op_id into l_op_id;
            --
            Insert into t_tmc_operation_units
              (op_id,
               tmc_id,
               tar_id_0,
               tar_id_1,
               callsign_0,
               callsign_1,
               owner_id_0,
               owner_id_1,
               st_sklad_0,
               st_sklad_1,
               Error_id,
               imsi_num,
               tmc_op_Cost,
               sim_type,
               sim_perm,
               state_id)
              (Select l_Op_Id,
                      t.tmc_id,
                      tariff_id,
                      tariff_id,
                      callsign,
                      callsign,
                      l_org_previos,
                      pi_operation.ownerId_1,
                      nvl(t.St_Sklad, ts.status),
                      nvl(t.St_Sklad, ts.status),
                      0,
                      imsi,
                      Tmc_Op_Cost,
                      null,
                      /*pi_operation.move_prm*/
                      l_perm,
                      2 --продан
                 From Table(l_tmc_op_tab) t
                 join t_tmc_iptv ip
                   on ip.tmc_id = t.tmc_id
                 join t_tmc tt
                   on tt.tmc_id = ip.tmc_id
                 join t_org_tmc_status ts
                   on ts.tmc_id = ip.tmc_id);
            Update t_tmc_iptv t
               set t.state_id = 2
             Where t.tmc_id in
                   (select tt.tmc_id From Table(l_tmc_op_tab) tt);
          end if;
        end if;
      else
        select max(t.tmc_perm)
          into l_perm
          from t_tmc t
         where t.tmc_id in (select tt.tmc_id From Table(l_tmc_op_tab) tt);
      
        if l_perm = 5007 then        
          -- если одинаковые серийник и мак-адрес выбираем только ТМЦ с последней датой операции
          select tab1.tmc_id bulk collect
            into l_tmc_cancel
            from (select max(tun.owner_id_0) keep(dense_rank last order by tun.unit_id) owner_id_0,
                         max(tun.op_id) keep(dense_rank last order by tun.unit_id) op_id,
                         max(ip.tmc_id) keep(dense_rank last order by tun.unit_id) tmc_id,
                         ip.mac_address,
                         ip.serial_number
                    from table(l_tmc_op_tab) tt
                    join t_tmc_iptv ip
                      on tt.tmc_id = ip.tmc_id
                  --and ip.priznak not in (7005)
                    left join t_tmc tt
                      on tt.tmc_id = ip.tmc_id
                    left join t_tmc_operation_units tun
                      on ip.tmc_id = tun.tmc_id
                     and ip.tmc_id is not null
                   where tt.is_deleted = 0
                   group by ip.mac_address, ip.serial_number) tab1
            join t_tmc_iptv ip
              on tab1.tmc_id = ip.tmc_id
            left join t_tmc tt
              on tt.tmc_id = ip.tmc_id
            left join t_tmc_operations top
              on top.op_id = tab1.op_id
             and top.op_type = 541
             and tab1.owner_id_0 =
                 nvl(l_operation.org_Id, l_operation.ownerId_0);
          -- формируем новую коллекцию без дублирующихся тмц, которые были проданы не последними
          for i in 1 .. l_tmc_op_tab.Count loop
            select count(1)
              into l_count_wrong_tmc
              from dual
             where l_tmc_op_tab(i)
             .tmc_id in (select * from table(l_tmc_cancel));
            if l_count_wrong_tmc > 0 then
              l_tmc_op_new.extend;
              l_tmc_op_new(l_tmc_op_new.count) := l_tmc_op_tab(i);
            end if;
          end loop;
          l_operation.opunits := l_tmc_op_new;
        end if;
      
        l_move := tmc.move_tmc(l_operation,
                               pi_worker_id,
                               po_msg,
                               po_err_num,
                               po_err_msg);
      
        if /*pi_operation.move_prm*/
         l_perm = 5007 then
          -- stepanov-km 28.02.2017 если Направление использования OTT-STB: продажа
          -- регистрируем операцию отмены продажи
        
          -- вставляем операцию отмены продажи
          select max(tou.owner_id_1) keep(dense_rank last order by tou.unit_id),
                 max(tto.op_dog_id) keep(dense_rank last order by tou.unit_id)
            into l_org_previos, l_dog_id
            from t_tmc_operations tto
            join t_tmc_operation_units tou
              on tto.op_id = tou.op_id
           where tto.op_type = 21
             and tou.tmc_id in
                 (select tt.tmc_id From Table(l_tmc_op_new) tt);
          -- Вставляем операцию     
          /*insert into t_tmc_operations
            (op_type, op_date, org_id, user_id, op_dog_id)
          values
            (24,
             pi_operation.opDate,
             pi_operation.org_Id,
             pi_worker_id,
             nvl(l_dog_id, pi_operation.dog_id))
          returning op_id into l_op_id;
          -- Вставляем юнит
          Insert into t_tmc_operation_units
            (op_id,
             tmc_id,
             owner_id_0,
             owner_id_1,
             st_sklad_0,
             st_sklad_1,
             Error_id,
             imsi_num,
             tmc_op_Cost,
             sim_type,
             sim_perm,
             state_id)
            (Select l_Op_Id,
                    t.tmc_id,
                    pi_operation.ownerId_1,
                    l_org_previos,
                    12,
                    11,
                    0,
                    ip.mac_address,
                    Tmc_Op_Cost,
                    null,
                    null,
                    1 
               From Table(l_tmc_op_new) t
               join t_tmc_iptv ip
                 on ip.tmc_id = t.tmc_id
               join t_tmc tt
                 on tt.tmc_id = ip.tmc_id);                       
          Update t_tmc_iptv t
             set t.state_id = 1
           Where t.tmc_id in (select tt.tmc_id From Table(l_tmc_op_new) tt);    */
          --если Направление использования OTT-STB: продажа,регистрируем операцию обратного выкупа
          l_Tmc_Collect := pi_operation.FilUnitsAll;
        
          insert into t_tmc_operations
            (op_type, op_date, org_id, user_id, op_dog_id)
          values
            (542,
             pi_operation.opDate,
             pi_operation.org_Id,
             pi_worker_id,
             nvl(l_dog_id, pi_operation.dog_id))
          returning op_id into l_op_id;
          --
          Insert into t_tmc_operation_units
            (op_id,
             tmc_id,
             tar_id_0,
             tar_id_1,
             callsign_0,
             callsign_1,
             owner_id_0,
             owner_id_1,
             st_sklad_0,
             st_sklad_1,
             Error_id,
             imsi_num,
             tmc_op_Cost,
             sim_type,
             sim_perm,
             state_id)
            (Select l_Op_Id,
                    t.tmc_id,
                    tariff_id,
                    tariff_id,
                    callsign,
                    callsign,
                    pi_operation.ownerId_1,
                    l_org_previos,
                    nvl(t.St_Sklad, ts.status),
                    nvl(t.St_Sklad, ts.status),
                    0,
                    imsi,
                    Tmc_Op_Cost,
                    null,
                    /*pi_operation.move_prm*/
                    /*l_perm*/
                    null,
                    1 -- принято
               From Table(l_tmc_op_new) t
               join t_tmc_iptv ip
                 on ip.tmc_id = t.tmc_id
               join t_tmc tt
                 on tt.tmc_id = ip.tmc_id
               join t_org_tmc_status ts
                 on ts.tmc_id = ip.tmc_id);
          Update t_tmc_iptv t
             set t.state_id = 1
           Where t.tmc_id in (select tt.tmc_id From Table(l_tmc_op_new) tt);
        end if;
        /*if l_move > 0 then
          po_err_msg := null;
        end if;*/
        return l_move;
      end if;
      /*if l_move > 0 then
        po_err_msg := null;
      end if;*/
      return l_move;
    end if;
  exception
    when ex_wrong_tmc_type then
      po_err_num := 1;
      po_err_msg := 'Неверный тип ТМЦ';
      rollback to sp_begin;
      return null;
    when ex_not_available_return then
      po_err_num := 2;
      po_err_msg := l_msg;
      rollback to sp_begin;
      return null;
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg, c_pr_name);
      rollback to sp_begin;
      return null;
  end;
  ---------------------------------------------------
  --3.1.9.  Удаление OTT-STB со склада
  ---------------------------------------------------
  procedure Delete_OTT_STB(pi_operation in new_operation,
                           -- pi_tmc_type  in number,
                           pi_worker_id in T_USERS.USR_ID%type,
                           po_msg       out CLOB,
                           po_back_msg  out CLOB,
                           po_err_num   out pls_integer,
                           po_err_msg   out Varchar2) is
    c_pr_name constant varchar2(65) := c_package || '.DELETE_OTT_STB';
    l_operation   new_operation;
    cnt           number := 0;
    l_tmc_op_tab  tmc_op_tab;
    l_tmc_op_type tmc_op_type;
    --l_op_type     number;
    l_tmc_id   number;
    l_tmc_type number;
    l_cost     number;
    l_perm     number;
    l_tmc_dog  number;
    ex_wrong_tmc_type exception;
    l_msg varchar2(50);
    ex_length exception;
    /****************************************/
    function getStrParam return varchar2 is
      l_str varchar2(1800);
    begin
      begin
        for i in (select * from table(pi_operation.opunits)) loop
          l_str := substr(l_str || 'tmc_id=' || i.tmc_id || 'tmc_type_Id=' ||
                          i.tmc_type_Id || 'real_owner_id=' ||
                          i.real_owner_id || 'tariff_id=' || i.tariff_id ||
                          'active_state=' || i.active_state || 'St_Sklad=' ||
                          i.St_Sklad || 'imsi=' || i.imsi ||
                          'Sim_In_Process=' || i.Sim_In_Process ||
                          'callsign=' || i.callsign || 'Tmc_Op_Cost=' ||
                          i.Tmc_Op_Cost || 'tmc_Perm=' || i.tmc_Perm ||
                          'tmc_Dog=' || i.tmc_Dog || 'SIM_TYPE=' ||
                          i.SIM_TYPE || 'iccid=' || i.iccid || ';',
                          1,
                          2000);
          if length(l_str) = 2000 then
            raise ex_length;
          end if;
        end loop;
      exception
        when ex_length then
          null;
        when others then
          null;
      end;
      return substr('pi_operation.operationId' || pi_operation.operationId ||
                    ',pi_operation.Optype' || pi_operation.Optype ||
                    ',pi_operation.org_Id' || pi_operation.org_Id ||
                    ',pi_operation.userId' || pi_operation.userId ||
                    ',pi_operation.webWorkerId' ||
                    pi_operation.webWorkerId || ',pi_operation.opDate' ||
                    pi_operation.opDate || ',pi_operation.tariffId' ||
                    pi_operation.tariffId || ',pi_operation.ownerId_0' ||
                    pi_operation.ownerId_0 || ',pi_operation.ownerId_1 ' ||
                    pi_operation.ownerId_1 || ',pi_operation.stockStatus' ||
                    pi_operation.stockStatus || ',opunits .tmc_Op_tab' ||
                    l_str || ',pi_operation.tmcType' ||
                    pi_operation.tmcType || ',pi_operation.op_comment' ||
                    pi_operation.op_comment || ',pi_operation.dog_id  ' ||
                    pi_operation.dog_id || ',pi_operation.move_prm' ||
                    pi_operation.move_prm || ';',
                    1,
                    2000);
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.info(getStrParam, c_pr_name);
    savepoint sp_begin;
    case pi_operation.tmcType -- pi_tmc_type
      when 7004 then
        l_msg := 'Комплекты ОТТ-STB (IPTV 2.0) ';
      when 7005 then
        l_msg := 'IPTV ';
      else
        select max(t.type_name) || ' '
          into l_msg
          from t_dic_tmc_type t
         where t.tmc_type = pi_operation.tmcType /*pi_tmc_type*/;
    end case;
    l_operation := pi_operation;
    --l_op_type    := pi_operation.optype;
    l_tmc_op_tab := tmc_op_tab();
    for i in (select distinct ip.tmc_id,
                              t.tmc_tmp_cost,
                              ots.org_id,
                              ots.status
                from t_tmc_iptv ip
                join t_tmc t
                  on t.tmc_id = ip.tmc_id
                join t_org_tmc_status ots
                  on ots.tmc_id = t.tmc_id
              --and ots.status = 11
                join table(pi_operation.opunits) tab
                  on (tab.imsi = ip.serial_number or
                     tab.iccid = ip.mac_address)
                 and pi_operation.tmcType /*pi_tmc_type*/ = ip.priznak
               where t.org_id = pi_operation.org_Id
                 and t.is_deleted = 0) loop
      cnt      := cnt + 1;
      l_tmc_id := i.tmc_id;

      l_tmc_type := 7002;
      l_cost     := i.tmc_tmp_cost;
      null;
      l_perm        := pi_operation.move_prm;
      l_tmc_dog     := pi_operation.dog_id;
      l_tmc_op_type := tmc_op_type(l_tmc_id,
                                   l_tmc_type,
                                   i.org_id,
                                   null,
                                   null,
                                   i.status,
                                   null,
                                   null,
                                   null,
                                   l_cost,
                                   l_perm,
                                   l_tmc_dog,
                                   null,
                                   null,
                                   null);
      l_tmc_op_tab.extend;
      l_tmc_op_tab(l_tmc_op_tab.count) := l_tmc_op_type;
    end loop;
    l_operation.opunits := l_tmc_op_tab;

    if cnt = 0 then
      po_err_num := 1;
      po_err_msg := l_msg || 'не найдены на складе организации.';
      return;
    else
      tmc_sim.del_tmc_after_revision(l_operation,
                                     pi_worker_id,
                                     po_back_msg,
                                     po_err_num,
                                     po_err_msg);
    end if;

    if po_err_msg is not null then
      po_msg := 'Были ошибки.  ' || po_err_msg;
    else
      po_msg := 'Ошибок не было. Удалено ' || pi_operation.opunits.count;
    end if;
  exception
    when ex_wrong_tmc_type then
      po_err_num := 1;
      po_err_msg := 'Неверный тип ТМЦ';
      rollback to sp_begin;
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg, c_pr_name);
      rollback to sp_begin;
  end;
  ---------------------------------------------------
  --3.1.11. 
  -- pi_operation.Optype = 541 - Возврат работоспособного OTT-STB клиентом (Удаленный функционал)
  -- pi_operation.Optype = 24 - Отмена продажи 
  -- pi_operation.Optype = 21 - Отмена продажи (вызов из возврата Move_OTT_STB)
  ---------------------------------------------------
  procedure Cancel_Sell_OTT_STB(pi_operation  in new_operation,
                                pi_seller_id  in number,
                                pi_op_comment in t_tmc_operations.op_comment%type,
                                pi_worker_id  in T_USERS.USR_ID%type,
                                po_msg        out CLOB,
                                po_err_num    out pls_integer,
                                po_err_msg    out Varchar2) is
    c_pr_name constant varchar2(65) := c_package || '.CANCEL_SELL_OTT_STB';
    --l_dog_id t_org_tmc_status.dog_id%type;
    l_op_id number;
    -- по счетам
    rec_acc_2      acc_operations.t_rec_account;
    rec_acc_1      acc_operations.t_rec_account;
    dummy          T_ACC_TRANSFER.TR_ID%type;
    l_cost_retail  number;
    l_cost_deposit number;
    l_cnt          number := 0;
    l_dog_id       number;
    l_dog_class    number;
    l_is_org_usi   number;
    l_msg          varchar2(200);
    l_sum_rp       number;
    /****************************************/
    function getStrParam return varchar2 is
      l_str varchar2(1800);
    begin
      begin
        select substr(listagg('tmc_id=' || tmc_id || 'tmc_type_Id=' ||
                              tmc_type_Id || 'real_owner_id=' ||
                              real_owner_id || 'tariff_id=' || tariff_id ||
                              'active_state=' || active_state ||
                              'St_Sklad=' || St_Sklad || 'imsi=' || imsi ||
                              'Sim_In_Process=' || Sim_In_Process ||
                              'callsign=' || callsign || 'Tmc_Op_Cost=' ||
                              Tmc_Op_Cost || 'tmc_Perm=' || tmc_Perm ||
                              'tmc_Dog=' || tmc_Dog || 'SIM_TYPE=' ||
                              SIM_TYPE || 'iccid=' || iccid || ';') within
                      group(order by tmc_id),
                      1,
                      2000)
          into l_str
          from table(pi_operation.opunits);
      exception
        when others then
          null;
      end;
      return substr('pi_operation.operationId' || pi_operation.operationId ||
                    ',pi_operation.Optype' || pi_operation.Optype ||
                    ',pi_operation.org_Id' || pi_operation.org_Id ||
                    ',pi_operation.userId' || pi_operation.userId ||
                    ',pi_operation.webWorkerId' ||
                    pi_operation.webWorkerId || ',pi_operation.opDate' ||
                    pi_operation.opDate || ',pi_operation.tariffId' ||
                    pi_operation.tariffId || ',pi_operation.ownerId_0' ||
                    pi_operation.ownerId_0 || ',pi_operation.ownerId_1 ' ||
                    pi_operation.ownerId_1 || ',pi_operation.stockStatus' ||
                    pi_operation.stockStatus || ',opunits .tmc_Op_tab' ||
                    l_str || ',pi_operation.tmcType' ||
                    pi_operation.tmcType || ',pi_operation.op_comment' ||
                    pi_operation.op_comment || ',pi_operation.dog_id  ' ||
                    pi_operation.dog_id || ',pi_operation.move_prm' ||
                    pi_operation.move_prm,
                    1,
                    2000);
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.info(getStrParam, c_pr_name);
    savepoint sp_begin;
  
    if (not Security_pkg.Check_User_Right_str('EISSD.TMC.CANCEL_SELL',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return;
    end if;
  
    l_is_org_usi := is_org_usi(pi_operation.org_Id);
  
    savepoint sp_begin;
    select max(d.dog_id) keep(dense_rank last order by d.dog_class_id),
           max(d.dog_class_id)
      into l_dog_id, l_dog_class
      from mv_org_tree r
      join t_dogovor d
        on d.org_rel_id in (r.root_rel_id, r.id)
       and d.is_enabled = 1
     where r.org_id = pi_operation.org_Id
       and l_is_org_usi = 0;
  
    for ora_1 in (select distinct tab1.owner_id_0,
                                  top.op_id,
                                  top.op_dog_id,
                                  tab1.tmc_id,
                                  tab1.mac_address,
                                  tab1.serial_number,
                                  tmc_tmp_cost,
                                  tab1.imsi,
                                  tab1.iccid,
                                  tab1.stb_model_id,
                                  trunc(top.op_date, 'mm') op_date,
                                  tab1.state_id,
                                  top.channel_id,
                                  op_r.request_id,
                                  op_r.is_ex_works,
                                  td.dog_class_id,
                                  tab1.is_second_hand,
                                  max(ra.sum) as op_fee,
                                  count(ip2.tmc_id) as count_duplicate,
                                  months_between(sysdate, top.op_date) m_between
                    from (select max(tun.owner_id_0) keep(dense_rank last order by tun.unit_id) owner_id_0,
                                 max(tun.op_id) keep(dense_rank last order by tun.unit_id) op_id,
                                 ip.tmc_id,
                                 ip.mac_address,
                                 ip.priznak,
                                 tt.tmc_tmp_cost,
                                 tab.imsi,
                                 tab.iccid,
                                 ip.stb_model_id,
                                 ip.serial_number,
                                 ip.state_id,
                                 ip.is_second_hand
                            from table(pi_operation.opunits) tab
                            left join t_tmc_iptv ip
                              on ((tab.imsi = ip.serial_number or
                                 tab.iccid = ip.mac_address) and
                                 ip.priznak = tab.tmc_type_Id and
                                 tab.tmc_id is null)
                              or (tab.tmc_id = ip.tmc_id)
                          --and ip.priznak not in (7005)
                            left join t_tmc tt
                              on tt.tmc_id = ip.tmc_id
                            left join t_tmc_operation_units tun
                              on ip.tmc_id = tun.tmc_id
                             and ip.tmc_id is not null
                           where tt.is_deleted = 0
                           group by ip.tmc_id,
                                    ip.mac_address,
                                    ip.priznak,
                                    ip.serial_number,
                                    tt.tmc_tmp_cost,
                                    tab.imsi,
                                    tab.iccid,
                                    ip.stb_model_id,
                                    ip.state_id,
                                    ip.is_second_hand) tab1
                    left join t_tmc_operations top
                      on top.op_id = tab1.op_id
                     and top.op_type = 22
                     and tab1.owner_id_0 =
                         nvl(pi_operation.org_Id, pi_operation.ownerId_0)
                    left join T_TMC_OPERATION_REQUEST op_r
                      on op_r.oper_id = top.op_id
                    left join t_dogovor td
                      on td.dog_id = top.op_dog_id
                    left join t_report_abonent ra
                      on ra.ab_id = tab1.tmc_id
                    left join t_tmc_iptv ip2
                      on tab1.serial_number = ip2.serial_number
                     and tab1.mac_address = ip2.mac_address
                     and ip2.tmc_id <> tab1.tmc_id
                     and ip2.priznak = tab1.priznak
                     and ip2.state_id = 1
                  /*where (td.dog_id is null or td.dog_class_id <> 11)*/
                   group by tab1.owner_id_0,
                            top.op_id,
                            top.op_dog_id,
                            tab1.tmc_id,
                            tab1.mac_address,
                            tab1.serial_number,
                            tmc_tmp_cost,
                            tab1.imsi,
                            tab1.iccid,
                            tab1.stb_model_id,
                            trunc(top.op_date, 'mm'),
                            tab1.state_id,
                            top.channel_id,
                            op_r.request_id,
                            op_r.is_ex_works,
                            td.dog_class_id,
                            tab1.is_second_hand,
                            months_between(sysdate, top.op_date)) loop
      if ora_1.op_id is not null and ora_1.op_fee is null and
         ora_1.m_between < 1 and ora_1.count_duplicate = 0 then
        -- Вставляем операцию отмены продажи/возврата работоспособного оборудования клиентом
        insert into t_tmc_operations
          (op_type,
           org_id,
           user_id,
           op_date,
           op_dog_id,
           op_comment,
           seller_id,
           CHANNEL_ID)
        values
          (24, --case pi_operation.Optype when 541 then 24 else 545 end,
           pi_operation.org_id,
           pi_worker_id,
           systimestamp,
           l_dog_id,
           pi_op_comment,
           pi_seller_id,
           ora_1.channel_id)
        returning op_id into l_op_id;
      
        -- Вставляем юнит
        insert into t_tmc_operation_units
          (op_id,
           tmc_id,
           owner_id_0,
           owner_id_1,
           error_id,
           st_sklad_0,
           st_sklad_1,
           imsi_num,
           tmc_op_cost,
           sim_perm,
           state_id)
        values
          (l_op_id,
           ora_1.tmc_id,
           null,
           ora_1.owner_id_0,
           0,
           12,
           11,
           ora_1.mac_address,
           ora_1.tmc_tmp_cost,
           pi_operation.move_prm,
           case when nvl(ora_1.dog_class_id, l_dog_class) in (11, 12) or
           pi_operation.Optype = 24 then 1 else 3 end);
        -- Добавляем на склад организации
        begin
          insert into t_org_tmc_status
            (tmc_id, org_id, status, income_date, dog_id)
          values
            (ora_1.tmc_id,
             ora_1.owner_id_0,
             11,
             sysdate,
             nvl(pi_operation.dog_id, l_dog_id));
        exception
          when dup_val_on_index then
            po_err_msg := po_err_msg ||
                          'Невозможно отменить операцию. ТМЦ уже имеется на складе организации.';
        end;
        -- добавляем привязку sim(ruim) карты к организации
        update t_tmc t
           set t.org_id = ora_1.owner_id_0
         where t.org_id is null
           and t.tmc_id = ora_1.tmc_id;
        if (sql%notfound) then
          po_err_msg := po_err_msg ||
                        'Невозможно отменить операцию. У ТМЦ уже имеется привязка к организации.';
        end if;
        update t_tmc_iptv t
           set t.state_id = case
                              when nvl(ora_1.dog_class_id, l_dog_class) in
                                   (11, 12) or pi_operation.Optype = 24 then
                               1
                              else
                               3
                            end
         where t.tmc_id = ora_1.tmc_id;
        l_msg := case
                   when l_cnt = 0 then
                    'Зарегистрированы операции отмены продажи:' || eof
                   else
                    ''
                 end;
        po_msg := l_msg || po_msg || 'серийный номер ' || ora_1.imsi ||
                  ', mac-адрес ' || ora_1.iccid || '.' || eof;
        l_cnt  := l_cnt + 1;
        if ora_1.op_dog_id is not null and
           ora_1.op_date < trunc(sysdate, 'mm') and
           pi_operation.Optype <> 24 then
          select sum(a.sum)
            into l_sum_rp
            from t_report_period t
            join t_report_abonent a
              on a.rp_id = t.id
           where t.is_actual = 1
             and a.type_ab = 7
             and a.ab_id = ora_1.tmc_id
             and ora_1.op_date <> trunc(sysdate, 'mm');
          insert into t_corrections
            (c_date, org_id, summ, reason, perm, dog_id, corr_type)
          values
            (trunc(sysdate, 'mm'),
             ora_1.owner_id_0,
             -l_sum_rp,
             'Возврат ОТТ-STB (IPTV 2.0) клиентом, SN: ' ||
             ora_1.serial_number,
             5014,
             ora_1.op_dog_id,
             1);
        end if;
        if l_dog_id is not null then
          --движение по счетам
          select (TMC.Get_Model_Cost(ora_1.tmc_id,
                                     ora_1.stb_model_id,
                                     pi_operation.org_Id,
                                     l_dog_id,
                                     3,
                                     pi_operation.Optype)) --залог
            into l_Cost_deposit
            from t_tmc_iptv ip
           where ip.tmc_id = Ora_1.tmc_id;
          --c списанные на резерв
          rec_acc_1 := Acc_operations.Get_Org_Rel_Account_Rec(null,
                                                              null,
                                                              Acc_operations.c_acc_type_adv,
                                                              l_dog_id);
          rec_acc_2 := Acc_operations.Get_Org_Rel_Account_Rec(null,
                                                              null,
                                                              Acc_operations.c_acc_type_res,
                                                              l_dog_id);
        
          dummy := acc_operations.Transfer_Funds(sysdate,
                                                 rec_acc_1,
                                                 rec_acc_2,
                                                 acc_operations.c_op_type_sell_res, --(?)
                                                 l_Cost_deposit,
                                                 l_op_id,
                                                 null,
                                                 pi_worker_id,
                                                 0);
          select (TMC.Get_Model_Cost(ora_1.tmc_id,
                                     ora_1.stb_model_id,
                                     pi_operation.org_Id,
                                     l_dog_id,
                                     1,
                                     pi_operation.Optype)) --розница
            into l_Cost_retail
            from t_tmc_iptv ip
           where ip.tmc_id = Ora_1.tmc_id;
          if l_Cost_retail - l_Cost_deposit > 0 then
            --c Списанные средства (авансы полученные) на лицевой
            rec_acc_2 := Acc_operations.Get_Org_Rel_Account_Rec(null,
                                                                null,
                                                                Acc_operations.c_acc_type_lic,
                                                                l_dog_id);
          
            dummy := acc_operations.Transfer_Funds(sysdate,
                                                   rec_acc_1,
                                                   rec_acc_2,
                                                   acc_operations.c_op_type_sell_lic, --(?)
                                                   l_Cost_retail -
                                                   l_Cost_deposit,
                                                   l_op_id,
                                                   null,
                                                   pi_worker_id,
                                                   0);
          end if;
        
        end if;
        if ora_1.request_id is not null then
          insert into T_TMC_OPERATION_REQUEST
            (oper_id, request_id, IS_EX_WORKS)
          values
            (l_op_id, ora_1.request_id, ora_1.IS_EX_WORKS);
        end if;
      else
        if ora_1.op_id is null then
          po_err_msg := po_err_msg ||
                        ' Операция продажи по заданной организации не найдена в системе. ';
        end if;
        if ora_1.op_fee is not null then
          po_err_msg := po_err_msg ||
                        ' По операции продажи уже были посчитаны вознаграждения. ';
        end if;
        if ora_1.m_between >= 1 then
          po_err_msg := po_err_msg ||
                        ' Операция продажи совершена более 30 дней назад.';
        end if;
        if ora_1.count_duplicate > 0 then
          po_err_msg := po_err_msg ||
                        ' В системе имеется дубликат  по серийному номеру и mac-адресу со статусом "Принято".';
        end if;
        po_err_msg := po_err_msg ||
                      ' Отмена продажи невозможна. Cерийный номер ' ||
                      ora_1.imsi || ', mac-адрес ' || ora_1.iccid || '.' || eof;
      end if;
    
    end loop;
    if po_err_msg is null and l_cnt > 0 then
      po_msg := 'Ошибок не было. Зарегистрировано операций отмены продаж ' ||
                pi_operation.opunits.count || ' шт.';
    elsif l_cnt = 0 then
      po_msg := 'Операции продажи по всем указанным ТМЦ в указанной организации не найдены в системе.';
    end if;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg, c_pr_name);
      rollback to sp_begin;
  end;
  
  ---------------------------------------------------
  -- Проверка возможности отмены продажи OTT-STB 
  --          для пользователей с правом «Продажи: отмена продажи оборудования»;
  --          если с даты продажи прошло менее 30 дней;
  --          если по продаже еще не было посчитано вознаграждение.  
  -- Возвращает курсор:
  --            tmc_id
  --            mac_address
  --            serial_number
  --            tmc_type
  --            is_second_hand
  --            op_fee - сумма начисленных вознаграждений (null - вознаграждений не начилсялось)
  --            op_date  - дата открытой операции продажи
  --------------------------------------------------- 
  function get_tmc_OTT_STB_by_id(pi_tmc_id    in number,
                                 pi_worker_id in T_USERS.USR_ID%type,
                                 po_err_num   out pls_integer,
                                 po_err_msg   out Varchar2)
    return sys_refcursor is
  
    res         sys_refcursor;
    l_right_str t_rights.right_string_id%type;
  
    c_pr_name constant varchar2(65) := c_package || '.get_tmc_OTT_STB';
    l_date_op date;
  
    /****************************************/
    function getStrParam return varchar2 is
    begin
      return 'pi_tmc_id=' || pi_tmc_id || ';' || 'pi_worker_id=' || pi_worker_id;
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    open res for
      select tab1.tmc_id,
             tab1.mac_address,
             tab1.serial_number,
             tab1.is_second_hand,
             tab1.priznak as tmc_type,
             max(ra.sum) as op_fee, -- сумма вознаграждений по продаже OTT-STB
             max(top.op_date) keep(dense_rank last order by top.op_id) as op_date,
             count(ip2.tmc_id) as count_duplicate
        from (select max(tun.owner_id_0) keep(dense_rank last order by tun.unit_id) owner_id_0,
                     max(tun.op_id) keep(dense_rank last order by tun.unit_id) op_id,
                     ip.tmc_id,
                     ip.mac_address,
                     ip.serial_number,
                     ip.priznak,
                     ip.is_second_hand,
                     tt.tmc_tmp_cost
                from t_tmc_iptv ip
                left join t_tmc tt
                  on tt.tmc_id = ip.tmc_id
                left join t_tmc_operation_units tun
                  on ip.tmc_id = tun.tmc_id
                 and ip.tmc_id is not null
               where ip.tmc_id = pi_tmc_id
               group by ip.tmc_id,
                        ip.mac_address,
                        ip.serial_number,
                        ip.priznak,
                        ip.is_second_hand,
                        tt.tmc_tmp_cost) tab1
        left join t_tmc_operations top
          on top.op_id = tab1.op_id
         and top.op_type = 22
        left join t_report_abonent ra
          on ra.ab_id = tab1.tmc_id
        left join t_tmc_iptv ip2
          on tab1.serial_number = ip2.serial_number
         and tab1.mac_address = ip2.mac_address
         and ip2.tmc_id <> tab1.tmc_id
         and ip2.priznak = tab1.priznak
         and ip2.state_id = 1
       group by tab1.tmc_id,
                tab1.mac_address,
                tab1.serial_number,
                tab1.priznak,
                tab1.is_second_hand;
  
    return res;
  
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg, c_pr_name);
      return null;
  end;
  
  function get_tmc_OTT_STB(pi_serial_number  in varchar2,
                           pi_mac_address    in varchar2,
                           pi_tmc_type       in number,
                           pi_is_second_hand in number,
                           pi_worker_id      in T_USERS.USR_ID%type,
                           po_err_num        out pls_integer,
                           po_err_msg        out Varchar2)
    return sys_refcursor is
  
    res         sys_refcursor;
    l_right_str t_rights.right_string_id%type;
  
    c_pr_name constant varchar2(65) := c_package ||
                                       '.get_tmc_OTT_STB';
    l_date_op date;   

    /****************************************/
    function getStrParam return varchar2 is
    begin
      return 'pi_serial_number=' || pi_serial_number || ';' 
             || 'pi_mac_address=' || pi_mac_address || ';' 
             || 'pi_tmc_type=' || pi_tmc_type || ';' 
             || 'pi_is_second_hand=' || pi_is_second_hand || ';' 
             || 'pi_worker_id=' || pi_worker_id;
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    open res for
      select tab1.tmc_id,
             tab1.mac_address,
             tab1.serial_number,
             tab1.is_second_hand,
             tab1.priznak as tmc_type,
             max(ra.sum) as op_fee, -- сумма вознаграждений по продаже OTT-STB
             max(top.op_date) keep(dense_rank last order by top.op_id) as op_date,
             count(ip2.tmc_id) as count_duplicate
        from (select max(tun.owner_id_0) keep(dense_rank last order by tun.unit_id) owner_id_0,
                     max(tun.op_id) keep(dense_rank last order by tun.unit_id) op_id,
                     ip.tmc_id,
                     ip.mac_address,
                     ip.serial_number,
                     ip.priznak,
                     ip.is_second_hand,
                     tt.tmc_tmp_cost
                from t_tmc_iptv ip
                left join t_tmc tt
                  on tt.tmc_id = ip.tmc_id
                left join t_tmc_operation_units tun
                  on ip.tmc_id = tun.tmc_id
                 and ip.tmc_id is not null
               where (pi_serial_number = ip.serial_number or
                     pi_mac_address = ip.mac_address)
                 and ip.priznak = pi_tmc_type
                 and ip.state_id = 2
                 and ip.is_second_hand = pi_is_second_hand
               group by ip.tmc_id,
                        ip.mac_address,
                        ip.serial_number,
                        ip.priznak,
                        ip.is_second_hand,
                        tt.tmc_tmp_cost) tab1
        left join t_tmc_operations top
          on top.op_id = tab1.op_id
         and top.op_type = 22
        left join t_report_abonent ra
          on ra.ab_id = tab1.tmc_id
        left join t_tmc_iptv ip2
          on pi_serial_number = ip2.serial_number 
         and pi_mac_address = ip2.mac_address
         and ip2.tmc_id <> tab1.tmc_id
         and ip2.state_id = 1          
       group by tab1.tmc_id,
                tab1.mac_address,
                tab1.serial_number,
                tab1.priznak,
                tab1.is_second_hand;
  
    return res;
  
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg, c_pr_name);
      return null;
  end;
  ---------------------------------------------------
  --3.1.10. Продажа OTT-STB (клиенту)
  ---------------------------------------------------
  procedure Sell_OTT_STB(Pi_Org_Id     in Number,
                         pi_tmc_tab    in NUM_STR2_TAB, --num1 -tmc_id,str1-Серийный номер,str2-Mac-адрес,
                         pi_seller_id  in number,
                         pi_op_comment in t_tmc_operations.op_comment%type,
                         pi_channel_id in number,
                         Pi_Worker     in Number,
                         po_msg        out clob,
                         po_error      out clob,
                         po_Err_Num    out pls_integer,
                         po_Err_msg    Out Varchar2) is
    c_pr_name constant varchar2(65) := c_package || '.SELL_OTT_STB';
    l_tmc_tab num_tab := num_tab();
    L_Op_Id   number; -- ID Операции продажи.
    -- по счетам
    rec_acc_1          acc_operations.t_rec_account;
    rec_acc_2          acc_operations.t_rec_account;
    dummy              T_ACC_TRANSFER.TR_ID%type;
    l_dog_id           number;
    l_Tmc_Cost_retail  number;
    l_Tmc_Cost_deposit number;
    l_cnt              number := 0;
    l_cost_retail      number;
    l_dog_class_id     number;
    ex_no_model exception;
    l_is_org_usi number;
    /****************************************/
    function getStrParam return varchar2 is
      l_str varchar2(1800);
    begin
      begin
        select substr(listagg(' Серийный номер=' || str1 || ',' ||
                              ' Mac-адрес=' || str2 ||','||num1 || ';') within
                      group(order by str1),
                      1,
                      1800)
          into l_str
          from table(pi_tmc_tab);
      exception
        when others then
          null;
      end;
      return substr('pi_org_id=' || pi_org_id || ';' || 'pi_seller_id=' ||
                    pi_seller_id || ';' || 'pi_op_comment=' ||
                    pi_op_comment || ';' || 'pi_tmc_tab=' || l_str || ';' ||
                    'pi_worker_id=' || Pi_Worker,
                    1,
                    2000);
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    savepoint sp_begin;
    if pi_channel_id is null then 
      po_Err_Num:=1;
      po_Err_msg:='Не выбран канал продаж';
      rollback to sp_begin;
      return;
    end if;  
    l_is_org_usi:= is_org_usi(pi_org_id); 
    -- Проверка наличия оборудования на складе
    select listagg(eof || 'Количество оборудования "' || sm.stb_model ||
                   '" на складе организации меньше количества моделей в рамках текущей продажи') WITHIN GROUP(order by sm.stb_model)
      into po_msg
      from ( -- Количество пришедших моделей
            select tip.stb_model_id, count(distinct tab.str1) cnt
              from table(pi_tmc_tab) tab
              join t_tmc_iptv tip
                on (tip.serial_number = tab.str1 or
                   tip.mac_address = tab.str2)
               and tip.priznak not in (7005)
               and tip.priznak = tab.num1
             group by tip.stb_model_id) tmc
      join ( -- Количество на складе различных моделей
            select t.stb_model_id, count(ots.tmc_id) cnt
              from t_organizations o
              join t_org_tmc_status ots
                on ots.org_id = o.org_id
               and ots.status = 11
              join t_tmc_iptv t
                on t.tmc_id = ots.tmc_id
               and t.priznak not in (7005)
              join t_tmc tt
                on t.tmc_id = tt.tmc_id
             where o.org_id = pi_org_id
               and (l_is_org_usi=1 or tt.tmc_perm = 5014)
             group by t.stb_model_id) sklad
        on tmc.stb_model_id = sklad.stb_model_id
      left join ( -- Количество зарезервированного
                 select t.model_id stb_model_id, sum(t.cnt) cnt
                   from t_tmc_ex_works_reserv t
                  where nvl(t.date_end, sysdate) >= sysdate
                    and t.org_id = Pi_Org_Id
                    and /*is_org_usi(t.org_id)*/l_is_org_usi = 1
                  group by t.model_id) reserv
        on sklad.stb_model_id = reserv.stb_model_id
      join t_ott_stb_model_info osmi
        on osmi.id = tmc.stb_model_id
      join t_stb_model sm
        on sm.id = osmi.model_stb_id
     where sklad.cnt - nvl(reserv.cnt, 0) < tmc.cnt;
    if po_msg is not null then
      raise ex_no_model;
    end if;

    --ищем идентификаторы тмц
    select distinct tip.tmc_id bulk collect
      into l_tmc_tab
      from table(pi_tmc_tab) tab
      join t_tmc_iptv tip
        on (tip.serial_number = tab.str1 or tip.mac_address = tab.str2)
       and tip.priznak not in (7005)
       and tip.priznak = tab.num1
      join t_tmc tt
        on tt.tmc_id = tip.tmc_id
       and tt.is_deleted = 0
      join t_org_tmc_status ts
        on ts.tmc_id = tip.tmc_id
          --and ts.status = 11
       and ts.org_id = pi_org_id;

    select max(d.dog_id), max(d.dog_class_id)
      into l_dog_id, l_dog_class_id
      from mv_org_tree r
      join t_dogovor d
        on d.org_rel_id in (r.root_rel_id, r.id)
          --and d.dog_class_id = 10
       and d.is_enabled = 1
     where r.org_id = Pi_Org_Id
       and l_is_org_usi = 0;
    /*if l_dog_id is not null and l_dog_class_id = 11 then
      po_err_num := 1;
      po_err_msg := 'Операция с данным типом договора запрещена!';
      return;
    END IF;*/
    For Ora_1 in (Select tt.tmc_id,
                         p.serial_number,
                         p.mac_address,
                         p.stb_model_id
                    from t_tmc tt
                    join t_tmc_iptv p
                      on p.tmc_id = tt.tmc_id
                   Where tt.is_deleted = 0
                     and tt.tmc_id in
                         (Select Column_Value from Table(l_tmc_tab))) loop
      if nvl(ACC_OPERATIONS.GetDogByTmcId(Ora_1.tmc_id, Pi_Org_Id),
             l_dog_id) is null then
        l_cost_retail := TMC.Get_Model_Cost(pi_tmc_id       => Ora_1.tmc_id,
                                            pi_stb_model_id => Ora_1.stb_model_id,
                                            pi_org_id       => Pi_Org_Id,
                                            pi_dog_id       => null,
                                            pi_type_cost    => 1,
                                            pi_type_op      => 20);
        update t_tmc t
           set t.tmc_tmp_cost = nvl(l_cost_retail, t.tmc_tmp_cost)
         where t.tmc_id = Ora_1.tmc_id;

      end if;
      --вставляем операцию продажи
      Insert Into t_tmc_operations
        (op_type,
         org_id,
         user_id,
         op_date,
         op_dog_id,
         op_comment,
         seller_id,
         channel_id)
      Values
        (22,
         Pi_Org_Id,
         pi_worker,
         systimestamp,
         nvl(ACC_OPERATIONS.GetDogByTmcId(Ora_1.tmc_id, Pi_Org_Id),
             l_dog_id),
         pi_op_comment,
         pi_seller_id,
         pi_channel_id)
      Returning Op_Id into L_Op_Id;

      Insert Into t_tmc_operation_units
        (op_id,
         tmc_id,
         owner_id_0,
         st_sklad_0,
         st_sklad_1,
         error_id,
         imsi_num,
         state_id,
         tmc_op_cost)
        Select L_Op_Id,
               tt.tmc_id,
               tos.org_id,
               tos.status,
               12,
               0,
               ip.mac_address,
               2,
               tt.tmc_tmp_cost
          from t_tmc tt
          join t_org_tmc_status tos
            on tos.tmc_id = tt.tmc_id
           and tos.org_id = Pi_Org_Id
          join t_tmc_iptv ip
            on ip.tmc_id = tt.tmc_id
         where tt.tmc_id = Ora_1.tmc_id;
      --удаляем тмц со склада
      delete from t_org_tmc_status tos where tos.tmc_id = Ora_1.tmc_id;
      --
      Update t_tmc t set t.org_id = Null Where t.tmc_id = Ora_1.tmc_id;
      --
      Update t_tmc_iptv t set t.state_id = 2 Where t.tmc_id = Ora_1.tmc_id;
      if l_dog_id is not null then
        --движение по счетам
        select (TMC.Get_Model_Cost(ip.tmc_id,
                                   ip.stb_model_id,
                                   Pi_Org_Id,
                                   l_dog_id,
                                   3,
                                   22)) --залог
          into l_Tmc_Cost_deposit
          from t_tmc_iptv ip
         where ip.tmc_id = Ora_1.tmc_id;
        --c резервного на списанные
        rec_acc_1 := Acc_operations.Get_Org_Rel_Account_Rec(null,
                                                            null,
                                                            Acc_operations.c_acc_type_res,
                                                            l_dog_id);
        rec_acc_2 := Acc_operations.Get_Org_Rel_Account_Rec(null,
                                                            null,
                                                            Acc_operations.c_acc_type_adv,
                                                            l_dog_id);

        dummy := acc_operations.Transfer_Funds(sysdate,
                                               rec_acc_1,
                                               rec_acc_2,
                                               acc_operations.c_op_type_tr_res_adv, --(?)
                                               l_Tmc_Cost_deposit,
                                               l_op_id,
                                               null,
                                               pi_worker,
                                               0);
        select (TMC.Get_Model_Cost(ip.tmc_id,
                                   ip.stb_model_id,
                                   Pi_Org_Id,
                                   l_dog_id,
                                   1,
                                   22)) --розница
          into l_Tmc_Cost_retail
          from t_tmc_iptv ip
         where ip.tmc_id = Ora_1.tmc_id;
        if l_Tmc_Cost_retail - l_Tmc_Cost_deposit > 0 then
          --c лицевого на Списанные средства (авансы полученные)
          rec_acc_1 := Acc_operations.Get_Org_Rel_Account_Rec(null,
                                                              null,
                                                              Acc_operations.c_acc_type_lic,
                                                              l_dog_id);

          dummy := acc_operations.Transfer_Funds(sysdate,
                                                 rec_acc_1,
                                                 rec_acc_2,
                                                 acc_operations.c_op_type_tr_lic_adv, --(?)
                                                 l_Tmc_Cost_retail -
                                                 l_Tmc_Cost_deposit,
                                                 l_op_id,
                                                 null,
                                                 pi_worker,
                                                 0);
        end if;
      end if;
      l_cnt := l_cnt + 1;
      /*po_msg := po_msg || 'Серийный номер: ' || ora_1.serial_number ||
      ' Mac-адрес: ' || ora_1.mac_address || '; ' || eof;*/
    end loop;
    po_msg := po_msg || 'Продано: ' || l_cnt ||
              ' шт.';
  exception
    when ex_no_model then
      po_err_num := sqlcode;
      po_err_msg := po_msg;
      logging_pkg.error(po_err_msg, c_pr_name);
      rollback to sp_begin;
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg, c_pr_name);
      rollback to sp_begin;
  end;
  ---------------------------------------------------
  procedure Sell_tmc_delivery(Pi_Org_Id     in Number,
                              pi_tmc_tab    in NUM_STR2_TAB, --num1 -tmc_id,str1-Серийный номер,str2-Mac-адрес,
                              pi_channel_id in number,
                              Pi_Worker     in Number,
                              pi_seller_id  in number,
                              pi_request_id in number,
                              po_Err_Num    out pls_integer,
                              po_Err_msg    Out Varchar2) is
    c_pr_name constant varchar2(65) := c_package || '.Sell_tmc_delivery';
    l_tmc_tab num_tab := num_tab();
    L_Op_Id   number; -- ID Операции продажи.
    -- по счетам
    rec_acc_1 acc_operations.t_rec_account;
    rec_acc_2 acc_operations.t_rec_account;
    dummy     T_ACC_TRANSFER.TR_ID%type;
    l_dog_id  number;
    --l_Tmc_Cost_retail  number;
    l_Tmc_Cost_deposit number;
    --l_cnt              number := 0;
    --l_cost_retail number;
    /****************************************/
    function getStrParam return varchar2 is
      l_str varchar2(1800);
    begin
      begin
        select substr(listagg(' num1=' || num1 || ',' || ' Серийный номер=' || str1 || ',' ||
                              ' Mac-адрес=' || str2 || ';') within
                      group(order by num1),
                      1,
                      1800)
          into l_str
          from table(pi_tmc_tab);
      exception
        when others then
          null;
      end;
      return substr('pi_org_id=' || pi_org_id || ';' || 'pi_tmc_tab=' ||
                    l_str || ';' || 'pi_worker_id=' || Pi_Worker ||
                    'pi_channel_id=' || pi_channel_id,
                    1,
                    2000);
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    savepoint sp_begin;
    
    if pi_channel_id is null then 
      po_Err_Num:=1;
      po_Err_msg:='Не выбран канал продаж';
      rollback to sp_begin;
      return;
    end if; 
    
    --ищем идентификаторы тмц
    select distinct tip.tmc_id bulk collect
      into l_tmc_tab
      from table(pi_tmc_tab) tab
      join t_tmc_iptv tip
        on (tip.serial_number = tab.str1 or tip.mac_address = tab.str2)
       and tip.priznak = tab.num1
      join t_tmc tt
        on tt.tmc_id = tip.tmc_id
       and tt.is_deleted = 0
      join t_org_tmc_status ts
        on ts.tmc_id = tip.tmc_id
          --and ts.status = 11
       and ts.org_id = pi_org_id;
  
    if l_tmc_tab is null or l_tmc_tab.count != pi_tmc_tab.count then
      po_err_num := 1;      
      for qq in (select distinct tab.str1 serial_number, tab.str2 mac_address
                   from table(pi_tmc_tab) tab
                   join t_tmc_iptv tip
                     on (tip.serial_number = tab.str1 or
                        tip.mac_address = tab.str2)
                    and tip.priznak = tab.num1
                   left join t_tmc tt
                     on tt.tmc_id = tip.tmc_id
                    and tt.is_deleted = 0
                   left join t_org_tmc_status ts
                     on ts.tmc_id = tip.tmc_id
                    and ts.org_id = pi_org_id
                  where ts.tmc_id is null) loop
        po_Err_msg := substr(po_Err_msg ||
                             'Оборудование с серийным номером: ' ||
                             qq.serial_number || ' и Мас-адресом: ' ||
                             qq.mac_address ||
                             ' отсутствует на складе организации'||eof,
                             0,
                             4000);
      end loop;
      rollback to sp_begin;
    end if;
  
    select max(d.dog_id)
      into l_dog_id
      from mv_org_tree r
      join t_dogovor d
        on d.org_rel_id in (r.root_rel_id, r.id)
       and d.dog_class_id = 11
       and d.is_enabled = 1
     where r.org_id = Pi_Org_Id;
  
    For Ora_1 in (Select tt.tmc_id,
                         p.serial_number,
                         p.mac_address,
                         p.stb_model_id
                    from t_tmc tt
                    join t_tmc_iptv p
                      on p.tmc_id = tt.tmc_id
                   Where tt.is_deleted = 0
                     and tt.tmc_id in
                         (Select Column_Value from Table(l_tmc_tab))) loop
      /*if nvl(ACC_OPERATIONS.GetDogByTmcId(Ora_1.tmc_id, Pi_Org_Id), l_dog_id) is null then
        l_cost_retail := Get_Model_Cost(pi_tmc_id       => Ora_1.tmc_id,
                                        pi_stb_model_id => Ora_1.stb_model_id,
                                        pi_org_id       => Pi_Org_Id,
                                        pi_dog_id       => null,
                                        pi_type_cost    => 1,
                                        pi_type_op      => 20);
        update t_tmc t
           set t.tmc_tmp_cost = l_cost_retail
         where t.tmc_id = Ora_1.tmc_id;
      end if;*/
      --вставляем операцию продажи
      Insert Into t_tmc_operations
        (op_type, org_id, user_id, op_date, op_dog_id, channel_id,seller_id)
      Values
        (22,
         Pi_Org_Id,
         pi_worker,
         systimestamp,
         nvl(ACC_OPERATIONS.GetDogByTmcId(Ora_1.tmc_id, Pi_Org_Id),
             l_dog_id),
         pi_channel_id,
         pi_seller_id)
      Returning Op_Id into L_Op_Id;
    
      Insert Into t_tmc_operation_units
        (op_id,
         tmc_id,
         owner_id_0,
         st_sklad_0,
         st_sklad_1,
         error_id,
         imsi_num,
         state_id,
         tmc_op_cost)
        Select L_Op_Id,
               tt.tmc_id,
               tos.org_id,
               tos.status,
               12,
               0,
               ip.mac_address,
               2,
               tt.tmc_tmp_cost
          from t_tmc tt
          join t_org_tmc_status tos
            on tos.tmc_id = tt.tmc_id
           and tos.org_id = Pi_Org_Id
          join t_tmc_iptv ip
            on ip.tmc_id = tt.tmc_id
         where tt.tmc_id = Ora_1.tmc_id;
         
      --добавим связь операции с заявкой
      insert into T_TMC_OPERATION_REQUEST
        (oper_id, request_id, IS_EX_WORKS)
        select L_Op_Id, d.id, d.is_ex_works
          from t_request_delivery d
         where d.id = pi_request_id;
        
      --удаляем тмц со склада
      delete from t_org_tmc_status tos where tos.tmc_id = Ora_1.tmc_id;
      --
      Update t_tmc t set t.org_id = Null Where t.tmc_id = Ora_1.tmc_id;
      --
      Update t_tmc_iptv t set t.state_id = 2 Where t.tmc_id = Ora_1.tmc_id;
      if l_dog_id is not null then
        --движение по счетам
        select (TMC.Get_Model_Cost(ip.tmc_id,
                                   ip.stb_model_id,
                                   Pi_Org_Id,
                                   l_dog_id,
                                   1,
                                   22)) --залог
          into l_Tmc_Cost_deposit
          from t_tmc_iptv ip
         where ip.tmc_id = Ora_1.tmc_id;
        --c резервного на списанные
        rec_acc_1 := Acc_operations.Get_Org_Rel_Account_Rec(null,
                                                            null,
                                                            Acc_operations.c_acc_type_res,
                                                            l_dog_id);
        rec_acc_2 := Acc_operations.Get_Org_Rel_Account_Rec(null,
                                                            null,
                                                            Acc_operations.c_acc_type_cost,
                                                            l_dog_id);
      
        dummy := acc_operations.Transfer_Funds(sysdate,
                                               rec_acc_1,
                                               rec_acc_2,
                                               acc_operations.c_op_type_tr_res_cost, --(?)
                                               l_Tmc_Cost_deposit,
                                               l_op_id,
                                               null,
                                               pi_worker,
                                               0);
        /*select (tmc_ott_stb.Get_Model_Cost(ip.tmc_id,
                                           ip.stb_model_id,
                                           Pi_Org_Id,
                                           l_dog_id,
                                           1,
                                           22)) --розница
          into l_Tmc_Cost_retail
          from t_tmc_iptv ip
         where ip.tmc_id = Ora_1.tmc_id;
        if l_Tmc_Cost_retail - l_Tmc_Cost_deposit > 0 then
          --c лицевого на Списанные средства (авансы полученные)
          rec_acc_1 := Acc_operations.Get_Org_Rel_Account_Rec(null,
                                                              null,
                                                              Acc_operations.c_acc_type_lic,
                                                              l_dog_id);
        
          dummy := acc_operations.Transfer_Funds(sysdate,
                                                 rec_acc_1,
                                                 rec_acc_2,
                                                 acc_operations.c_op_type_tr_lic_adv, --(?)
                                                 l_Tmc_Cost_retail -
                                                 l_Tmc_Cost_deposit,
                                                 l_op_id,
                                                 null,
                                                 pi_worker,
                                                 0);
        end if;*/
      end if;
    end loop;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg, c_pr_name);
      rollback to sp_begin;
  end;
  ---------------------------------------------------
  -- Список операций OTT-STB
  ---------------------------------------------------
/*  function get_op_list(pi_worker_id    in number,
                       pi_org_id       in array_num_2, -- Организация
                       pi_block        in number,
                       pi_org_relation in num_tab,
                       pi_date_from    in date, -- Период выполнения операции (МСК) от. Поля обязательны для заполнения. Необходимо ограничить выбор периода тремя месяцами.
                       pi_date_to      in date, -- Период выполнения операции (МСК) до. Поля обязательны для заполнения. Необходимо ограничить выбор периода тремя месяцами.
                       pi_op_type      in num_tab, -- Поле «Тип операции» с мультивыбором вариантов: Продажа OTT-STB клиенту, Возврат работоспособного OTT-STB клиентом. По умолчанию отмечены все варианты. Если не отмечен ни один вариант, то в список должны попадать все типы операций.
                       pi_serial_num   in varchar2, -- Серийный номер. Поле для ввода текстового значения, до 32 символов. Допустим ввод любых цифр 0-9, букв английского алфавита A-Z и специальных символов: - и /. Поле не обязательно для заполнения. При вводе значения в данное поле, фильтры «Организация», «Период выполнения операции (МСК)», «Тип операции» не должны учитываться.
                       pi_mac_address  in varchar2, -- Поле «Mac-адрес». Поле для ввода из диапазона: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, A, B, C, D, E, F. Допустим ввод 12 символов. Поле не обязательно для заполнения. При вводе значения в данное поле, фильтры «Организация», «Период выполнения операции (МСК)», «Тип операции» не должны учитываться.
                       pi_tmc_type     in number, -- Тип оборудования
                       pi_channel_id   in num_tab, -- Канал продаж
                       pi_num_page     in number, -- номер страницы
                       pi_count_req    in number, -- кол-во записей на странице
                       pi_column       in number, -- Номер колонки для сортировки
                       pi_sorting      in number, -- 0-по возрастанияю, 1-по убыванию                      
                       po_all_count    out number, -- выводим общее кол-во записей, подходящих под условия
                       po_err_num      out number,
                       po_err_msg      out varchar2) return sys_refcursor is 
begin
  return get_op_list(pi_worker_id      => pi_worker_id,
                       pi_org_id       => pi_org_id, -- Организация
                       pi_block        => pi_block,
                       pi_org_relation => pi_org_relation,
                       pi_date_from    => pi_date_from, -- Период выполнения операции (МСК) от. Поля обязательны для заполнения. Необходимо ограничить выбор периода тремя месяцами.
                       pi_date_to      => pi_date_to, -- Период выполнения операции (МСК) до. Поля обязательны для заполнения. Необходимо ограничить выбор периода тремя месяцами.
                       pi_op_type      => pi_op_type, -- Поле «Тип операции» с мультивыбором вариантов: Продажа OTT-STB клиенту, Возврат работоспособного OTT-STB клиентом. По умолчанию отмечены все варианты. Если не отмечен ни один вариант, то в список должны попадать все типы операций.
                       pi_serial_num   => pi_serial_num, -- Серийный номер. Поле для ввода текстового значения, до 32 символов. Допустим ввод любых цифр 0-9, букв английского алфавита A-Z и специальных символов: - и /. Поле не обязательно для заполнения. При вводе значения в данное поле, фильтры «Организация», «Период выполнения операции (МСК)», «Тип операции» не должны учитываться.
                       pi_mac_address  => pi_mac_address, -- Поле «Mac-адрес». Поле для ввода из диапазона: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, A, B, C, D, E, F. Допустим ввод 12 символов. Поле не обязательно для заполнения. При вводе значения в данное поле, фильтры «Организация», «Период выполнения операции (МСК)», «Тип операции» не должны учитываться.
                       pi_tmc_type     => pi_tmc_type, -- Тип оборудования
                       pi_channel_id   => pi_channel_id, -- Канал продаж
                       pi_num_page     => pi_num_page, -- номер страницы
                       pi_count_req    => pi_count_req, -- кол-во записей на странице
                       pi_column       => pi_column, -- Номер колонки для сортировки
                       pi_sorting      => pi_sorting, -- 0-по возрастанияю, 1-по убыванию
                       pi_second_hand  => null, -- фильтр по состоянию ТМЦ (null - все, 1 - б/у, 0 - не б/у)
                       po_all_count    => po_all_count, -- выводим общее кол-во записей, подходящих под условия
                       po_err_num      => po_err_num,
                       po_err_msg      => po_err_msg);

end;    */                   
  function get_op_list(pi_worker_id    in number,
                       pi_org_id       in array_num_2, -- Организация
                       pi_block        in number,
                       pi_org_relation in num_tab,
                       pi_date_from    in date, -- Период выполнения операции (МСК) от. Поля обязательны для заполнения. Необходимо ограничить выбор периода тремя месяцами.
                       pi_date_to      in date, -- Период выполнения операции (МСК) до. Поля обязательны для заполнения. Необходимо ограничить выбор периода тремя месяцами.
                       pi_op_type      in num_tab, -- Поле «Тип операции» с мультивыбором вариантов: Продажа OTT-STB клиенту, Возврат работоспособного OTT-STB клиентом. По умолчанию отмечены все варианты. Если не отмечен ни один вариант, то в список должны попадать все типы операций.
                       pi_serial_num   in varchar2, -- Серийный номер. Поле для ввода текстового значения, до 32 символов. Допустим ввод любых цифр 0-9, букв английского алфавита A-Z и специальных символов: - и /. Поле не обязательно для заполнения. При вводе значения в данное поле, фильтры «Организация», «Период выполнения операции (МСК)», «Тип операции» не должны учитываться.
                       pi_mac_address  in varchar2, -- Поле «Mac-адрес». Поле для ввода из диапазона: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, A, B, C, D, E, F. Допустим ввод 12 символов. Поле не обязательно для заполнения. При вводе значения в данное поле, фильтры «Организация», «Период выполнения операции (МСК)», «Тип операции» не должны учитываться.
                       -- Тип оборудования теперь не может быть пустым, т.к. пункта "Все" теперь нет
                       pi_tmc_type          in num_tab, -- Тип оборудования (сим всегда взаимоисключающая с остальными типами ТМЦ)
                       pi_channel_id        in num_tab, -- Канал продаж
                       pi_num_page          in number, -- номер страницы
                       pi_count_req         in number, -- кол-во записей на странице
                       pi_column            in number, -- Номер колонки для сортировки
                       pi_sorting           in number, -- 0-по возрастанияю, 1-по убыванию
                       pi_second_hand       in number, -- фильтр по состоянию ТМЦ (null - все, 1 - б/у, 0 - новое)
                       pi_sim_callsign_city in varchar2, -- Городской номер
                       po_all_count         out number, -- выводим общее кол-во записей, подходящих под условия
                       po_err_num           out number,
                       po_err_msg           out varchar2)
    return sys_refcursor is
    c_pr_name constant varchar2(65) := c_package || '.GET_OP_LIST';
    res            sys_refcursor;
    l_org_tab      num_tab := num_tab();
    l_user_org_tab num_tab := num_tab();
    l_count        number;
    l_op_type      number;
    l_op_list      OP_LIST_TAB;
    l_max_num_page number;
    l_num_page     number;
    l_channel      number;
    l_is_sim       number;
  begin
    logging_pkg.debug('pi_org_id: ' ||
                      get_str_by_array_num_2(pi_org_id, 5) || 'pi_block: ' ||
                      pi_block || chr(10) || 'pi_org_relation: ' ||
                      get_str_by_num_tab(pi_org_relation) || chr(10) ||
                      'pi_date_from: ' || pi_date_from || chr(10) ||
                      'pi_date_to: ' || pi_date_to || chr(10) ||
                      'pi_op_type: ' || get_str_by_num_tab(pi_op_type) ||
                      chr(10) || 'pi_serial_num: ' || pi_serial_num ||
                      chr(10) || 'pi_mac_address: ' || pi_mac_address ||
                      chr(10) || 'pi_tmc_type: ' ||
                      get_str_by_num_tab(pi_tmc_type) || chr(10) ||
                      'pi_channel_id: ' ||
                      get_str_by_num_tab(pi_channel_id) || chr(10) ||
                      'pi_num_page: ' || pi_num_page || chr(10) ||
                      'pi_count_req: ' || pi_count_req || chr(10) ||
                      'pi_column: ' || pi_column || chr(10) ||
                      'pi_sorting: ' || pi_sorting || chr(10) ||
                      'pi_second_hand: ' || pi_second_hand || chr(10) ||
                      'po_all_count: ' || po_all_count,
                      c_pr_name);
    if pi_date_from is null or pi_date_to is null then
      po_err_num := 1;
      po_err_msg := 'Не задан период';
      return null;
    elsif add_months(pi_date_from, 3) < pi_date_to then
      po_err_num := 1;
      po_err_msg := 'Период не может быть больше трех месяцев';
      return null;
    end if;
  
    if (not Security_pkg.Check_User_Right_str('EISSD.CONNECTIONS.OTT_STB.GET_LIST',
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
  
    l_user_org_tab := ORGS.get_user_orgs_by_prm(pi_worker_id => pi_worker_id,
                                                pi_rel_tab   => null,
                                                pi_prm_tab   => null /*num_tab(2011,
                                                                                                                                                                                                                                                                                                                                                                                                                        2012,
                                                                                                                                                                                                                                                                                                                                                                                                                        9678,
                                                                                                                                                                                                                                                                                                                                                                                                                        9668,
                                                                                                                                                                                                                                                                                                                                                                                                                        9669)*/,
                                                po_err_num   => po_err_num,
                                                po_err_msg   => po_err_msg);
    if l_count = 0 then
      l_org_tab := l_user_org_tab;
    else
      l_org_tab := intersects(l_org_tab, l_user_org_tab);
    end if;
  
    if pi_op_type is null or pi_op_type.count = 0 then
      l_op_type := 0;
    end if;
  
    if pi_channel_id is null or pi_channel_id.count = 0 then
      l_channel := 0;
    end if;
  
    select count(*)
      into l_is_sim
      from table(pi_tmc_type) t
     where t.column_value = 8;
  
    if l_is_sim = 0 then
      select OP_LIST_TYPE(op_id,
                          op_type_id,
                          op_type,
                          op_date,
                          root_org_name,
                          org_id,
                          org_name,
                          stb_model,
                          is_second_hand,
                          serial_number,
                          mac_address,
                          retail_price,
                          person_lastname,
                          person_firstname,
                          person_middlename,
                          usr_login,
                          op_comment,
                          seller_id,
                          seller_fio,
                          seller_login,
                          tmc_id,
                          tmc_type,
                          request_id,
                          channel_id,
                          channel_name,
                          sim_callsign_city,
                          rownum) bulk collect
        into l_op_list
        from (select distinct op.op_id,
                              op.op_type as op_type_id,
                              case
                                when op.op_type = 22 and rd.request_id is null then
                                 'Продажа оборудования клиенту'
                              -- Функционал  'Возврат работоспособного оборудования клиентом' удален 
                              -- Задача № 112155 - 2238.02 Продажа БУ оборудования 
                                when op.op_type = 24 then
                                 'Отмена продажи'
                                when op.op_type = 22 and rd.is_ex_works = 0 then
                                 'Доставка курьером'
                                else
                                 'Самовывоз'
                              end op_type, -- Тип операции
                              op.op_date, -- Дата и время выполнения операции.
                              (nvl(orgR.Org_Name, o.Org_Name)) as root_org_name, -- Головная организация.
                              o.org_id,
                              o.org_name, -- Подчиненная организация. В поле выводится организация, совершившая операцию.
                              rd.request_id,
                              sm.stb_model, -- Модель. В поле выводится модель OTT-STB.
                              osm.is_second_hand, -- Состояние модели. Б/У или не Б/У
                              t.tmc_id,
                              t.serial_number, --  Серийный номер. В поле выводится серийный номер OTT-STB.
                              t.mac_address, -- Mac-адрес. В поле выводится Mac-адрес OTT-STB.
                              nvl(tt.tmc_tmp_cost, 0) * 100 retail_price, -- Розничная стоимость (с НДС). В поле выводится «розничная стоимость» OTT-STB.
                              p.person_lastname, -- Пользователь. В поле выводится пользователь, совершивший операцию.
                              p.person_firstname,
                              p.person_middlename,
                              u.usr_login, -- Пользователь. В поле выводится пользователь, совершивший операцию.
                              op.op_comment,
                              op.seller_id as seller_id,
                              -- Продавец. В поле выводится продавец, выбранный на интерфейсе при продаже или возврате.
                              /*nvl(trim(pd.person_lastname || ' ' ||
                              pd.person_firstname || ' ' ||
                              pd.person_middlename),
                              ps.person_lastname || ' ' ||
                              ps.person_firstname || ' ' ||
                              ps.person_middlename) seller_fio,*/
                              ps.person_lastname || ' ' ||
                              ps.person_firstname || ' ' ||
                              ps.person_middlename seller_fio,
                              us.usr_login seller_login,
                              t.priznak tmc_type,
                              op.channel_id,
                              dc.name channel_name,
                              null sim_callsign_city
                from t_tmc_iptv t
                join t_tmc tt
                  on t.tmc_id = tt.tmc_id
                join t_tmc_operation_units tou
                  on tt.tmc_id = tou.tmc_id
                 and nvl(tou.error_id, 0) = 0
                join t_tmc_operations op
                  on op.op_id = tou.op_id
                join t_organizations o
                  on o.org_id = op.org_id
                join t_ott_stb_model_info osm
                  on osm.id = t.stb_model_id
                join t_stb_model sm
                  on sm.id = osm.model_stb_id
                join t_users u
                  on op.user_id = u.usr_id
                join t_person p
                  on p.person_id = u.usr_person_id
                left join t_organizations orgR
                  on orgR.org_id = /*o.root_org_id2*/
                     Orgs.Get_Root_Org_Or_Self(o.org_id)
                left join t_users us
                  on op.seller_id = us.usr_id
                left join t_person ps
                  on ps.person_id = us.usr_person_id
              /*left join t_req_deliv_equip rde
                on rde.tmc_id = t.tmc_id
              left join t_request_delivery rd
                on rd.id = rde.req_id*/
                left join t_tmc_operation_request rd
                  on rd.oper_id = op.op_id
                left join t_dic_channels dc
                  on dc.channel_id = op.channel_id -- nvl(rd.channel_id, op.channel_id)
                left join t_dogovor td
                  on td.dog_id = op.op_dog_id
              -- Пользователь, исполнившый заявку, для доставки и самовывоза
              /*left join t_users ud
                on rd.change_worker_id = ud.usr_id
              left join t_person pd
                on pd.person_id = ud.usr_person_id*/
               where t.priznak in (select * from table(pi_tmc_type))
                 and (pi_serial_num is null or
                     t.serial_number = pi_serial_num)
                 and (pi_mac_address is null or
                     t.mac_address = pi_mac_address)
                 and (pi_second_hand is null or
                     t.is_second_hand = pi_second_hand)
                    -- Чтобы, если веб косячит, у нас все равно правильно выводилось
                 and op.op_type in (24, 22)
                    -- Оля Медведева сказала не выводить сюда операции по договору доставки
                 and (td.dog_id is null or td.dog_class_id in (10, 11, 12))
                 and (((pi_serial_num is not null or
                     pi_mac_address is not null) or
                     (op.org_id in (select * from table(l_org_tab)) and
                     trunc(op.op_date - 2 / 24) between pi_date_from and
                     pi_date_to and (l_op_type = 0 or
                     
                     case
                       when op.op_type = 22 and rd.request_id is null then
                        1
                       when op.op_type = 24 then
                        5
                       when op.op_type = 22 and rd.is_ex_works = 0 then
                        3
                       else
                        4
                     end
                     
                     in (select * from table(pi_op_type))))))
                    -- Из ФТ: для самовывоза и доставки должен быть статус Продан
                    -- stepanov-km 21.02.17 выводим отмены для заявок согласно ЧТЗ по сервису 2238.02 
                    -- and (rd.request_id is null or /*t.state_id = 2*/op.op_type  = 22)
                    -- Не выводим отмены для заявок, замечание 30
                    -- stepanov-km 21.02.17 выводим отмены для заявок согласно ЧТЗ по сервису 2238.02 
                    -- and (rd.request_id is null or op.op_type <> 24)
                 and (l_channel = 0 or rd.request_id is null or
                     op.channel_id in (select * from table(pi_channel_id)))
               order by decode(pi_sorting, 0, op.op_date, null) asc,
                        decode(pi_sorting, 1, op.op_date, null) desc);
    else
      -- По sim-картам
      select OP_LIST_TYPE(op_id,
                          op_type_id,
                          op_type,
                          op_date,
                          root_org_name,
                          org_id,
                          org_name,
                          stb_model,
                          is_second_hand,
                          serial_number,
                          mac_address,
                          retail_price,
                          person_lastname,
                          person_firstname,
                          person_middlename,
                          usr_login,
                          op_comment,
                          seller_id,
                          seller_fio,
                          seller_login,
                          tmc_id,
                          tmc_type,
                          request_id,
                          channel_id,
                          channel_name,
                          sim_callsign_city,
                          rownum) bulk collect
        into l_op_list
        from (select distinct op.op_id,
                              op.op_type as op_type_id,
                              case
                                when op.op_type = 22 and
                                     rd.request_id is not null then
                                 'Доставка курьером'
                                when op.op_type = 24 then
                                 'Отмена продажи'
                              end op_type, -- Тип операции
                              op.op_date, -- Дата и время выполнения операции.
                              (nvl(orgR.Org_Name, o.Org_Name)) as root_org_name, -- Головная организация.
                              o.org_id,
                              o.org_name, -- Подчиненная организация. В поле выводится организация, совершившая операцию.
                              rd.request_id,
                              tat.title stb_model, -- Тариф
                              t.tmc_id,
                              t.sim_imsi || '/' || t.sim_iccid serial_number, --  IMSI/ICCID
                              tc.federal_callsign mac_address, -- позывной
                              tc.city_callsign sim_callsign_city, -- городской номер
                              get_tariff_cost(tat.id,
                                              1,
                                              decode(tc.city_callsign,
                                                     null,
                                                     9001,
                                                     9002),
                                              tc.color) * 100 retail_price, -- Розничная стоимость (с НДС). В поле выводится «розничная стоимость» OTT-STB.
                              p.person_lastname, -- Пользователь. В поле выводится пользователь, совершивший операцию.
                              p.person_firstname,
                              p.person_middlename,
                              u.usr_login, -- Пользователь. В поле выводится пользователь, совершивший операцию.
                              op.op_comment,
                              op.seller_id as seller_id,
                              -- Продавец. В поле выводится продавец, выбранный на интерфейсе при продаже или возврате.
                              ps.person_lastname || ' ' ||
                              ps.person_firstname || ' ' ||
                              ps.person_middlename seller_fio,
                              us.usr_login seller_login,
                              8 tmc_type,
                              op.channel_id,
                              dc.name channel_name,
                              null is_second_hand
                from t_tmc_sim t
                join t_tmc tt
                  on t.tmc_id = tt.tmc_id
                join t_tmc_operation_units tou
                  on tt.tmc_id = tou.tmc_id
                 and nvl(tou.error_id, 0) = 0
                join t_tmc_operations op
                  on op.op_id = tou.op_id
                join t_organizations o
                  on o.org_id = op.org_id
                join t_tarif_by_at_id tat
                  on tat.at_id = t.tar_id
                join t_users u
                  on op.user_id = u.usr_id
                join t_person p
                  on p.person_id = u.usr_person_id
                left join t_organizations orgR
                  on orgR.org_id = Orgs.Get_Root_Org_Or_Self(o.org_id)
                left join t_users us
                  on op.seller_id = us.usr_id
                left join t_person ps
                  on ps.person_id = us.usr_person_id
                join t_req_deliv_equip rde
                  on rde.tmc_id = t.tmc_id
                join t_callsign tc
                  on tc.tmc_id = rde.callsign_id
                /*left*/ join t_tmc_operation_request rd
                  on rd.oper_id = op.op_id
                left join t_dic_channels dc
                  on dc.channel_id = op.channel_id
                left join t_dogovor td
                  on td.dog_id = op.op_dog_id
               where (pi_serial_num is null or t.sim_imsi = pi_serial_num or
                     t.sim_iccid = pi_serial_num)
                 and (pi_mac_address is null or
                     tc.federal_callsign = pi_mac_address)
                 and (pi_sim_callsign_city is null or
                     tc.city_callsign = pi_sim_callsign_city)
                 and op.op_type in (24, 22)
                 and (td.dog_id is null or td.dog_class_id in (11))
                 and (((pi_serial_num is not null or
                     pi_mac_address is not null) or
                     (op.org_id in (select * from table(l_org_tab)) and
                     trunc(op.op_date - 2 / 24) between pi_date_from and
                     pi_date_to and (l_op_type = 0 or
                     
                     case
                       when op.op_type = 22 and rd.request_id is not null then
                        3
                       when op.op_type = 24 then
                        5
                     end
                     
                     in (select * from table(pi_op_type))))))
                 and (l_channel = 0 or rd.request_id is null or
                     op.channel_id in (select * from table(pi_channel_id)))
               order by decode(pi_sorting, 0, op.op_date, null) asc,
                        decode(pi_sorting, 1, op.op_date, null) desc);
      null;
    end if;
  
    po_all_count := l_op_list.count;
  
    l_max_num_page := round(po_all_count / nvl(pi_count_req, 1));
  
    if (pi_num_page > l_max_num_page and l_max_num_page <> 0) then
      l_num_page := l_max_num_page + 1;
    else
      l_num_page := nvl(pi_num_page, 1);
    end if;
  
    open res for
      select op_id,
             op_type_id,
             op_type,
             op_date - 2 / 24 op_date,
             root_org_name,
             t.org_id,
             org_name,
             stb_model,
             is_second_hand,
             serial_number,
             mac_address,
             retail_price,
             person_lastname,
             person_firstname,
             person_middlename,
             t.usr_login,
             op_comment,
             seller_id,
             seller_fio,
             seller_login,
             request_id,
             tmc_id,
             tmc_type,
             channel_id,
             channel_name,
             sim_callsign_city,
             u.employee_number,
             rn
        from table(l_op_list) t
        LEFT JOIN t_users u ON u.usr_id = t.seller_id
       where rn between
             (l_num_page - 1) * nvl(pi_count_req, po_all_count) + 1 and
             (l_num_page) * nvl(pi_count_req, po_all_count)
       order by rn;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg, c_pr_name);
      if res%isopen then
        close res;
      end if;
      return null;
  end get_op_list;
  ----------------------------------------------------------------------
  -- Список cопроводительных документы на продажу/выкуп OTT-STB
  ----------------------------------------------------------------------
  function get_list_documents_ott_stb(pi_worker_id in number,
                                      pi_tmc_type  in number, -- Тип ТМЦ
                                      pi_op_type   in num_tab, -- Тип операции 20 - Продажа OTT-STB дилеру, 541  Обратный выкуп OTT-STB у дилера (посылаем 21)
                                      pi_date_from in date, -- Начальная дата(МСК)
                                      pi_date_to   in date, -- Конечная дата(МСК)
                                      pi_org_pid   in number, -- Курирующая организация
                                      pi_org_id    in number, -- Курируемая организация
                                      po_err_num   out number,
                                      po_err_msg   out varchar2)
    return sys_refcursor is
    res       sys_refcursor;
    l_op_type number;
    c_pr_name constant varchar2(65) := c_package ||
                                       '.GET_LIST_DOCUMENTS_OTT_STB';
    l_org_tab num_tab := num_tab();
  begin
    logging_pkg.debug('pi_worker_id := ' || pi_worker_id || chr(10) ||
                      'pi_tmc_type := ' || pi_tmc_type || chr(10) ||
                      'pi_op_tab := ' || get_str_by_num_tab(pi_op_type) ||
                      chr(10) || 'pi_date_start := ' || pi_date_from ||
                      chr(10) || 'pi_date_end := ' || pi_date_to ||
                      chr(10) || 'pi_org_pid := ' || pi_org_pid || chr(10) ||
                      'pi_org_id := ' || pi_org_id,
                      c_pr_name);
    if (not Security_pkg.Check_User_Right_str('EISSD.REPORT.OTT_STB.ACCOMPANYING_DOCUMENTS',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;
    if pi_op_type is null or pi_op_type.count = 0 then
      l_op_type := 0;
    end if;
    l_org_tab := get_orgs_tab_for_multiset(pi_orgs         => null,
                                           pi_worker_id    => pi_worker_id,
                                           pi_block        => 1,
                                           pi_org_relation => null);
    open res for
      select distinct op.op_id,
                      op.op_date - 2 / 24 as op_date,
                      case
                        when op.op_type = 20 then
                         'Продажа ' || dic.type_name || ' дилеру'
                        else
                         'Обратный выкуп ' || dic.type_name || ' у дилера'
                      end op_type,
                      td.num_doc doc_num,
                      nvl(orgR.Org_Name, o.org_name) as root_org_name,
                      o.org_name,
                      p.person_lastname || ' ' || p.person_firstname || ' ' ||
                      p.person_middlename author,
                      td.id_document,
                      o.org_id,
                      nvl(orgr.org_id, o.org_id) org_pid,
                      count(tou.tmc_id) amount
        from t_tmc_iptv t
        join t_tmc tt
          on t.tmc_id = tt.tmc_id
        join t_tmc_operation_units tou
          on tt.tmc_id = tou.tmc_id
        join t_tmc_operations op
          on op.op_id = tou.op_id
        join t_organizations o
          on o.org_id =
             decode(op.op_type, 20, tou.owner_id_1, tou.owner_id_0)
        left join mv_org_tree rel
          on rel.org_id = o.org_id
         and rel.org_pid <> -1
         and rel.root_org_pid <> -1
         and rel.org_reltype not in (1005, 1006, 1009)
        left join t_organizations orgR
          on orgR.org_id = rel.org_pid
        left join t_tmc_op_unit_doc toud
          on tou.unit_id = toud.unit_id
        left join t_documentum td
          on toud.id_document = td.id_document
        left join t_users u
          on td.worker_id = u.usr_id
        left join t_person p
          on p.person_id = u.usr_person_id
        left join t_dogovor dog
          on op.op_dog_id = dog.dog_id
        left join t_dogovor_prm dp
          on dp.dp_dog_id = dog.dog_id
         and dp.dp_is_enabled = 1
        join T_DIC_TMC_TYPE_REL_PRM rp
          on rp.tmc_type = t.priznak
        join t_perm_to_perm pp
          on pp.perm_dog = rp.prm_id
        left join t_dic_tmc_type dic
          on dic.tmc_type = t.priznak
       where t.priznak = pi_tmc_type
            /*тип ТМЦ пока не анализируем*/
         and ((l_op_type = 0 and op.op_type in (20, 21)) or
             op.op_type in (select * from table(pi_op_type)))
         and (pi_date_from is null or
             trunc(op.op_date - 2 / 24) >= pi_date_from)
         and (pi_date_to is null or trunc(op.op_date - 2 / 24) < pi_date_to)
         and decode(op.op_type, 20, td.owner_id_0, td.owner_id_1) =
             pi_org_pid
         and (pi_org_id is null or
             decode(op.op_type, 20, td.owner_id_1, td.owner_id_0) =
             pi_org_id)
         and (dog.dog_id is null or
             (pp.direction = 5007 and pp.perm_dog = dp.dp_prm_id))
         and (td.owner_id_1 in (select * from table(l_org_tab)) or
             td.owner_id_0 in (select * from table(l_org_tab)))
       group by op.op_id,
                op.op_date /* - 2 / 24*/,
                case
                  when op.op_type = 20 then
                   'Продажа ' || dic.type_name || ' дилеру'
                  else
                   'Обратный выкуп ' || dic.type_name || ' у дилера'
                end,
                td.num_doc,
                nvl(orgR.Org_Name, o.org_name),
                o.org_name,
                p.person_lastname || ' ' || p.person_firstname || ' ' ||
                p.person_middlename,
                td.id_document,
                o.org_id,
                nvl(orgr.org_id, o.org_id)
       order by op.op_date - 2 / 24;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg, c_pr_name);
      if res%isopen then
        close res;
      end if;
      return null;
  end get_list_documents_ott_stb;
  -------------------------------------------------------------------------------------
  ------------------------ Получение существующих статусов ТМЦ ------------------------
  -------------------------------------------------------------------------------------
  function get_tmc_for_revision(pi_operation in new_operation,
                                -- pi_tmc_type  in number, -- Тип ТМЦ
                                pi_worker_id in t_users.usr_id%type,
                                po_err_num   out pls_integer,
                                po_err_msg   out varchar2)
    return sys_refcursor is
    l_owner     number;
    res         sys_refcursor;
    l_right_str varchar2(50);
    ex_length exception;
    function getStrParam return varchar2 is
      l_exp varchar2(2000);
    begin
      begin
        for i in (select * from table(pi_operation.opunits)) loop
          l_exp := substr(l_exp || 'tmc_id=' || i.tmc_id || 'tmc_type_Id=' ||
                          i.tmc_type_Id || 'real_owner_id=' ||
                          i.real_owner_id || 'tariff_id=' || i.tariff_id ||
                          'active_state=' || i.active_state || 'St_Sklad=' ||
                          i.St_Sklad || 'imsi=' || i.imsi ||
                          'Sim_In_Process=' || i.Sim_In_Process ||
                          'callsign=' || i.callsign || 'Tmc_Op_Cost=' ||
                          i.Tmc_Op_Cost || 'tmc_Perm=' || i.tmc_Perm ||
                          'tmc_Dog=' || i.tmc_Dog || 'SIM_TYPE=' ||
                          i.SIM_TYPE || 'iccid=' || i.iccid || ';',
                          1,
                          2000);
          if length(l_exp) = 2000 then
            raise ex_length;
          end if;
        end loop;
      exception
        when ex_length then
          null;
        when others then
          null;
      end;
      return substr('pi_operation.operationId' || pi_operation.operationId || '; ' ||
                    ',pi_operation.Optype' || pi_operation.Optype || '; ' ||
                    ',pi_operation.org_Id' || pi_operation.org_Id || '; ' ||
                    ',pi_operation.userId' || pi_operation.userId || '; ' ||
                    ',pi_operation.webWorkerId' ||
                    pi_operation.webWorkerId || '; ' ||
                    ',pi_operation.opDate' || pi_operation.opDate || '; ' ||
                    ',pi_operation.tariffId' || pi_operation.tariffId || '; ' ||
                    ',pi_operation.ownerId_0' || pi_operation.ownerId_0 || '; ' ||
                    ',pi_operation.ownerId_1 ' || pi_operation.ownerId_1 || '; ' ||
                    ',pi_operation.stockStatus' ||
                    pi_operation.stockStatus || '; ' ||
                    ',opunits .tmc_Op_tab' || l_exp || '; ' ||
                    ',pi_operation.tmcType' || pi_operation.tmcType || '; ' ||
                    ',pi_operation.op_comment' || pi_operation.op_comment || '; ' ||
                    ',pi_operation.dog_id  ' || pi_operation.dog_id || '; ' ||
                    ',pi_operation.move_prm' || pi_operation.move_prm,
                    1,
                    2000);
    end getStrParam;
  begin
    logging_pkg.debug(getStrParam, c_package || '.get_tmc_for_revision');
    select max(r.right_string_id)
      into l_right_str
      from T_TMC_TYPE_RIGHTS t
      join t_rights r
        on r.right_id = t.right_id
     where t.tmc_type = pi_operation.tmcType -- pi_tmc_type
       and t.op_type = 23;
  
    -- Проверим права на выполнение операции
    if (not Security_pkg.Check_User_Right_str(l_right_str,
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;
    l_owner := pi_operation.ownerid_0;
  
    open res for
    -- stepanov-km 21.02.2017 выводятся все ТМЦ удовлетворяющие условиям
      select distinct iptv.tmc_id,
                      dr.kl_name || ' ' || dr.kl_socr region,
                      tab.imsi,
                      tab.iccid,
                      smod.stb_model,
                      /*osmi.*/
                      t.tmc_tmp_cost retail_price,
                      osmi.is_second_hand,
                      org.org_name,
                      decode(dv.dv_name,
                             null,
                             decode(t.tmc_id,
                                    null,
                                    'Не зарегистрирована в системе',
                                    'Не найдена на складе организации'),
                             dv.dv_name) status,
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
                             null) state,
                      decode(dv.dv_name, null, 0, 1) is_enabled
        from table(pi_operation.opunits) tab
        left join t_tmc_iptv iptv
          on tab.imsi = iptv.serial_number
         and tab.iccid = iptv.mac_address
         and iptv.priznak = pi_operation.tmcType -- pi_tmc_type
        left join t_tmc t
          on t.tmc_id = iptv.tmc_id
        left join t_org_tmc_status ots
          on ots.tmc_id = t.tmc_id
         and ots.org_id = l_owner
        left join t_ott_stb_model_info osmi
          on osmi.id = iptv.stb_model_id
         and iptv.priznak = pi_operation.tmcType -- pi_tmc_type
        left join t_stb_model smod
          on smod.id = osmi.model_stb_id
        Left Join t_Organizations org
          on org.Org_Id = t.org_id
        left join t_dic_values dv
          on dv.dv_id = ots.status
        left join t_dic_region dr
          on osmi.region_id = dr.reg_id
       order by 11 desc;
    return res;
  end get_tmc_for_revision;
  -------------------------------------------------------------------------------------
  --------------------------- Ревизия комплектов OTT-STB ------------------------------
  -------------------------------------------------------------------------------------
  function revise_ott_stb(pi_operation in new_operation,
                          pi_status    in number,
                          pi_worker_id in number,
                          po_err_num   out pls_integer,
                          po_err_msg   out varchar2) return sys_refcursor is
    l_operation    new_operation;
    l_tmc_op_tab   tmc_op_tab;
    l_tmc_collect  tmc_Op_tab;
    l_tmc_err      tmc_op_tab := tmc_op_tab(); -- ошибочные ТМЦ
    l_Cnt_Valid    number;
    l_dog_sale     number;
    l_dog_realiz   number;
    l_dog_deliv    number;
    l_dog_ex_works number;
    res            sys_refcursor;
    l_region_id    number;
    l_org_pid      number;
    l_err_num      number;
    l_err_msg      varchar2(2000);
    l_msg          varchar2(2000);
    l_op_back      number;
    l_Tmc_Op_Type  tmc_op_type;
    l_Op_Id        number;
    del_operation  new_operation;
    l_err          clob;
    ex_no_rights exception;
    ex_err_back  exception;
    ex_length    exception;
    l_back_msg  CLOB;
    l_exp       varchar(2000);
    l_right_str varchar2(50);
  begin
    begin
      for i in (select * from table(pi_operation.opunits)) loop
        l_exp := substr(l_exp || 'tmc_id=' || i.tmc_id || 'tmc_type_Id=' ||
                        i.tmc_type_Id || 'real_owner_id=' ||
                        i.real_owner_id || 'tariff_id=' || i.tariff_id ||
                        'active_state=' || i.active_state || 'St_Sklad=' ||
                        i.St_Sklad || 'imsi=' || i.imsi ||
                        'Sim_In_Process=' || i.Sim_In_Process ||
                        'callsign=' || i.callsign || 'Tmc_Op_Cost=' ||
                        i.Tmc_Op_Cost || 'tmc_Perm=' || i.tmc_Perm ||
                        'tmc_Dog=' || i.tmc_Dog || 'SIM_TYPE=' ||
                        i.SIM_TYPE || 'iccid=' || i.iccid || ';',
                        1,
                        2000);
        if length(l_exp) = 2000 then
          raise ex_length;
        end if;
      end loop;
    exception
      when ex_length then
        null;
      when others then
        null;
    end;
    logging_pkg.debug(substr(',pi_operation.operationId' ||
                             pi_operation.operationId ||
                             ',pi_operation.Optype' || pi_operation.Optype ||
                             ',pi_operation.org_Id' || pi_operation.org_Id ||
                             ',pi_operation.userId' || pi_operation.userId ||
                             ',pi_operation.webWorkerId' ||
                             pi_operation.webWorkerId ||
                             ',pi_operation.opDate' || pi_operation.opDate ||
                             ',pi_operation.tariffId' ||
                             pi_operation.tariffId ||
                             ',pi_operation.ownerId_0' ||
                             pi_operation.ownerId_0 ||
                             ',pi_operation.ownerId_1 ' ||
                             pi_operation.ownerId_1 ||
                             ',pi_operation.stockStatus' ||
                             pi_operation.stockStatus ||
                             ',opunits .tmc_Op_tab' || l_exp ||
                             ',pi_operation.tmcType' ||
                             pi_operation.tmcType ||
                             ',pi_operation.op_comment' ||
                             pi_operation.op_comment ||
                             ',pi_operation.dog_id  ' ||
                             pi_operation.dog_id ||
                             ',pi_operation.move_prm' ||
                             pi_operation.move_prm,
                             1,
                             2000),
                      'revise_ott_stb');
    select max(r.right_string_id)
      into l_right_str
      from T_TMC_TYPE_RIGHTS t
      join t_rights r
        on r.right_id = t.right_id
     where t.tmc_type = pi_operation.tmcType
       and t.op_type = 23;
  
    -- Проверим права на выполнение операции
    if (not Security_pkg.Check_User_Right_str(l_right_str,
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;
  
    l_Tmc_Collect := pi_operation.FilUnitsAll;
    l_Tmc_Collect := pi_operation.InsTextError(l_Tmc_Collect);
  
    l_Cnt_Valid := 0;
    For i in 1 .. l_Tmc_Collect.count loop
      If l_Tmc_Collect(i).tmc_id is Null then
        l_Tmc_Collect(i).message := 'Не зарегистрирована в системе.';
      ElsIf l_Tmc_Collect(i).St_Sklad is Null then
        l_Tmc_Collect(i).message := 'Продана.';
      ElsIf Trim(l_Tmc_Collect(i).message) is Null then
        l_Cnt_Valid := l_Cnt_Valid + 1;
      else
        null;
      End If;
    End Loop;
  
    -- Результат
    If l_Cnt_Valid = 0 then
      Open Res for
        Select dr.kl_name || ' ' || dr.kl_socr region,
               iptv.serial_number imsi,
               iptv.mac_address iccid,
               smod.stb_model,
               iptv.is_second_hand,
               /*osmi.*/
               tt.tmc_tmp_cost retail_price,
               org.org_name,
               decode(dv.dv_name,
                      null,
                      decode(t.tmc_id,
                             null,
                             'Не зарегистрирована в системе',
                             'Не найдена на складе организации'),
                      dv.dv_name) status,
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
                      null) state,
               t.message
          from Table(l_Tmc_Collect) t
          left Join t_Organizations org
            on org.Org_Id = t.real_owner_id
          left join t_tmc_iptv iptv
            on (t.tmc_id = iptv.tmc_id)
            or (t.tmc_id is null
           and t.imsi = iptv.serial_number
           and t.iccid = iptv.mac_address
           and iptv.priznak = pi_operation.tmcType)          
/*            on t.imsi = iptv.serial_number
           and t.iccid = iptv.mac_address
           and iptv.priznak = pi_operation.tmcType*/
          left join t_tmc tt
            on tt.tmc_id = iptv.tmc_id
          left join t_org_tmc_status ots
            on ots.tmc_id = tt.tmc_id
          left join t_ott_stb_model_info osmi
            on osmi.id = iptv.stb_model_id
           and iptv.priznak = pi_operation.tmcType 
          left join t_stb_model smod
            on smod.id = osmi.model_stb_id
          Left Join t_Organizations org
            on org.Org_Id = tt.org_id
          left join t_dic_values dv
            on dv.dv_id = ots.status
          left join t_dic_region dr
            on osmi.region_id = dr.reg_id;
      return res;
    End If;
  
    for i in 1 .. l_Tmc_Collect.Count loop
      savepoint sp_revision_loop;
      begin
        If Trim(l_Tmc_Collect(i).message) Is Null then
          -- Проверка прав
          if l_Tmc_Collect(i).tmc_dog is not null then
            select sum(decode(pp.direction, 5007, pp.direction, null)),
                   sum(decode(pp.direction, 5014, pp.direction, null)),
                   sum(decode(pp.direction, 5017, pp.direction, null)),
                   sum(decode(pp.direction, 5019, pp.direction, null))
              into l_dog_sale, l_dog_realiz, l_dog_deliv, l_dog_ex_works
              from t_dogovor_prm dp
              join t_perm_to_perm pp
                on pp.perm_dog = dp.dp_prm_id
             where dp.dp_dog_id = l_Tmc_Collect(i).tmc_dog
               and dp.dp_is_enabled = 1;
          end if;
          if pi_status = 13 then
            -- Утеря
            if not ((l_Tmc_Collect(i)
                .active_state in (1, 3) and
                 is_org_usi(l_Tmc_Collect(i).real_owner_id) = 1) or
                (l_Tmc_Collect(i)
                .active_state in (1, 3) and
                 (l_dog_realiz is not null or l_dog_deliv is not null or
                 l_dog_ex_works is not null))) then
              logging_pkg.debug('Нельзя проводить утерю ' || ' ' ||
                                l_dog_realiz || ' ' || l_Tmc_Collect(i)
                                .active_state,
                                'revise_ott_stb');
              raise ex_no_rights;
            end if;
          
            -- Сама операция ревизии
            insert into t_tmc_operations
              (op_type, org_id, user_id, op_date, op_comment, Op_Dog_id)
            values
              (23,
               l_Tmc_Collect(i).real_owner_id,
               pi_worker_id,
               sysdate,
               null,
               null)
            returning op_id into l_op_id;
            Insert into t_tmc_operation_units t
              (op_id,
               tmc_id,
               tar_id_0,
               tar_id_1,
               callsign_0,
               callsign_1,
               owner_id_0,
               owner_id_1,
               st_sklad_0,
               st_sklad_1,
               Error_id,
               imsi_num,
               Tmc_Op_Cost,
               sim_type,
               sim_perm,
               state_id)
            Values
              (l_Op_Id,
               l_Tmc_Collect        (i).tmc_id,
               l_Tmc_Collect        (i).tariff_id,
               l_Tmc_Collect        (i).tariff_id,
               l_Tmc_Collect        (i).callsign,
               l_Tmc_Collect        (i).callsign,
               l_Tmc_Collect        (i).real_owner_id,
               l_Tmc_Collect        (i).real_owner_id,
               l_Tmc_Collect        (i).St_Sklad,
               13,
               0,
               l_Tmc_Collect        (i).imsi,
               l_Tmc_Collect        (i).Tmc_Op_Cost,
               null,
               pi_operation.move_prm,
               5);
          
            -- Движения по счетам
            if is_org_usi(l_Tmc_Collect(i).real_owner_id) = 0 then
              acc_operations.Change_Acc_For_Tmc_Lost(l_op_id,
                                                     l_Tmc_Collect(i)
                                                     .real_owner_id,
                                                     null,
                                                     sysdate,
                                                     l_Tmc_Collect(i).tmc_id,
                                                     pi_worker_id,
                                                     l_Tmc_Collect(i)
                                                     .tmc_dog,
                                                     null);
            end if;
          
            -- Меняем складской статус
            update t_org_tmc_status ots
               set ots.status = 13
             where ots.tmc_id = l_Tmc_Collect(i).tmc_id;
          
            -- Меняем статус на "Утерян"
            update t_tmc_iptv iptv
               set iptv.state_id = 5
             where iptv.tmc_id = l_Tmc_Collect(i).tmc_id;
          
            -- Удаляем ТМЦ
            -- ниже
          
            l_Tmc_Collect(i).message := 'Списано по причине: УТЕРЯ';
            l_Tmc_Collect(i).St_Sklad := 13;
          else
            -- 14 Брак
            if not ((l_Tmc_Collect(i)
                .active_state in (1, 3) and
                 is_org_usi(l_Tmc_Collect(i).real_owner_id) = 1) or
                (l_Tmc_Collect(i)
                .active_state = 2 and l_dog_sale is not null) or
                (l_Tmc_Collect(i)
                .active_state in (1, 3) and
                 (l_dog_realiz is not null or l_dog_deliv is not null or
                 l_dog_ex_works is not null))) then
              raise ex_no_rights;
            end if;
            -- Если с правами все OK
            -- Если организация не принципал
            if is_org_usi(l_Tmc_Collect(i).real_owner_id) = 0 then
              -- Возвращаем на склад принципала, операция "Возврат бракованного OTT-STB дилером"
              select o.region_id
                into l_region_id
                from t_organizations o
               where o.org_id = l_Tmc_Collect(i).real_owner_id;
              if l_region_id > 0 and l_dog_realiz is null then
                -- по аутсорсингу неправильно определять вышестоящую по региону
                l_org_pid := Orgs.Get_FES_Bi_Id_Region(l_region_id,
                                                       0,
                                                       l_err_num,
                                                       l_err_msg);
              else
                --l_org_pid := 0;
                select max(t.root_org_pid) keep(dense_rank first order by r.reg_id)--t.root_org_pid
                  into l_org_pid
                  from mv_org_tree t
                  left join t_dic_region r
                    on r.org_id = t.root_org_pid
                 where t.org_id = l_Tmc_Collect(i).real_owner_id
                   and t.root_org_pid <> -1;
              end if;
            
              l_Tmc_Op_Tab  := tmc_op_tab();
              l_Tmc_Op_Type := tmc_op_type(l_Tmc_Collect        (i).tmc_id,
                                           7002,
                                           null,
                                           null,
                                           null,
                                           11,
                                           l_Tmc_Collect        (i).imsi,
                                           null,
                                           null,
                                           l_Tmc_Collect        (i)
                                           .Tmc_Op_Cost,
                                           pi_operation.move_prm,
                                           l_Tmc_Collect        (i).tmc_dog,
                                           null,
                                           null,
                                           l_Tmc_Collect        (i).iccid);
              l_Tmc_Op_Tab.Extend;
              l_Tmc_Op_Tab(l_Tmc_Op_Tab.Count) := l_Tmc_Op_Type;
            
              l_operation := new_operation(operationId => Null,
                                           Optype      => 543,
                                           org_Id      => l_Tmc_Collect(i)
                                                          .real_owner_id,
                                           userId      => pi_worker_id,
                                           webWorkerId => pi_worker_id,
                                           opDate      => sysdate,
                                           tariffId    => null,
                                           ownerId_0   => l_org_pid,
                                           ownerId_1   => null,
                                           stockStatus => Null,
                                           opunits     => l_Tmc_Op_Tab,
                                           tmcType     => 7002,
                                           op_comment  => null,
                                           dog_id      => null,
                                           move_prm    => pi_operation.move_prm);
              l_op_back   := replace(Tmc.Back_TMC(l_operation,
                                                  pi_worker_id,
                                                  l_msg,
                                                  l_err_num,
                                                  l_err_msg),
                                     ';',
                                     '');
              if nvl(l_op_back, -1) <= 0 then
                raise ex_err_back;
              end if;
            end if;
            -- Сама операция ревизии
            insert into t_tmc_operations
              (op_type, org_id, user_id, op_date, op_comment, Op_Dog_id)
            values
              (23,
               nvl(l_org_pid, l_Tmc_Collect(i).real_owner_id),
               pi_worker_id,
               sysdate,
               null,
               null)
            returning op_id into l_op_id;
            Insert into t_tmc_operation_units t
              (op_id,
               tmc_id,
               tar_id_0,
               tar_id_1,
               callsign_0,
               callsign_1,
               owner_id_0,
               owner_id_1,
               st_sklad_0,
               st_sklad_1,
               Error_id,
               imsi_num,
               Tmc_Op_Cost,
               sim_type,
               sim_perm,
               state_id)
            Values
              (l_Op_Id,
               l_Tmc_Collect(i).tmc_id,
               l_Tmc_Collect(i).tariff_id,
               l_Tmc_Collect(i).tariff_id,
               l_Tmc_Collect(i).callsign,
               l_Tmc_Collect(i).callsign,
               nvl(l_org_pid, l_Tmc_Collect(i).real_owner_id),
               nvl(l_org_pid, l_Tmc_Collect(i).real_owner_id),
               l_Tmc_Collect(i).St_Sklad,
               14,
               0,
               l_Tmc_Collect(i).imsi,
               l_Tmc_Collect(i).Tmc_Op_Cost,
               null,
               pi_operation.move_prm,
               4);
          
            -- Меняем складской статус
            update t_org_tmc_status ots
               set ots.status = 14
             where ots.tmc_id = l_Tmc_Collect(i).tmc_id;
          
            -- Меняем статус на "Брак"
            update t_tmc_iptv iptv
               set iptv.state_id = 4
             where iptv.tmc_id = l_Tmc_Collect(i).tmc_id;
          
            l_Tmc_Collect(i).message := 'Списано по причине: БРАК';
            l_Tmc_Collect(i).St_Sklad := 14;
          end if;
        Else
          l_tmc_err.extend;
          l_tmc_err(l_tmc_err.count) := l_Tmc_Collect(i);
          l_Tmc_Collect.Delete(i);
        End If;
      exception
        when ex_no_rights then
          l_tmc_err.extend;
          l_tmc_err(l_tmc_err.count) := l_Tmc_Collect(i);
          l_tmc_err(l_tmc_err.count).message := 'ТМЦ (' || l_Tmc_Collect(i).imsi || ', ' || l_Tmc_Collect(i)
                                               .iccid ||
                                                '): у организации отсутствуют права на ревизию.';
          l_Tmc_Collect.Delete(i);
          rollback to sp_revision_loop;
        when ex_err_back then
          l_tmc_err.extend;
          l_tmc_err(l_tmc_err.count) := l_Tmc_Collect(i);
          l_tmc_err(l_tmc_err.count).message := 'ТМЦ (' || l_Tmc_Collect(i).imsi || ', ' || l_Tmc_Collect(i)
                                               .iccid ||
                                                '): произошла ошибка при возврате ТМЦ: ' ||
                                                l_err_msg;
          l_Tmc_Collect.Delete(i);
          rollback to sp_revision_loop;
        when others then
          l_tmc_err.extend;
          l_tmc_err(l_tmc_err.count) := l_Tmc_Collect(i);
          l_tmc_err(l_tmc_err.count).message := sqlerrm || ' ' ||
                                                dbms_utility.format_error_backtrace;
          l_Tmc_Collect.Delete(i);
          rollback to sp_revision_loop;
      end;
    end loop;
    -- Удаляем ТМЦ
    del_operation := pi_operation;
    if l_Tmc_Collect.count > 0 then
      del_operation.org_Id  := nvl(l_org_pid, pi_operation.org_id);
      del_operation.opunits := l_Tmc_Collect;
      del_operation.opDate  := sysdate;
      -- удалить параметр pi_tmc_type после удаления параметра на вебе
      Delete_OTT_STB(del_operation,
                     -- pi_tmc_type,
                     pi_worker_id,
                     l_err,
                     l_back_msg,
                     po_err_num,
                     po_err_msg);
    end if;
    -- Формируем итоговый результат
    open res for
      Select dr.kl_name || ' ' || dr.kl_socr region,
             nvl(iptv.serial_number, t.imsi) imsi,
             nvl(iptv.mac_address, t.iccid) iccid,
             smod.stb_model,
             /*osmi.*/
             tt.tmc_tmp_cost * 100 retail_price,
             org.org_name,
             decode(dv.dv_name,
                    null,
                    decode(t.tmc_id,
                           null,
                           'Не зарегистрировано в системе',
                           'Удалено'),
                    dv.dv_name) status,
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
                    null) state,
             t.message
        from Table(l_Tmc_Collect) t
        left join t_tmc_iptv iptv
        -- stepanov-km 21.02.2017 добавлена связь по tmc_id
          on (t.tmc_id = iptv.tmc_id)
          or (t.tmc_id is null
         and t.imsi = iptv.serial_number
         and t.iccid = iptv.mac_address
         and iptv.priznak = pi_operation.tmcType) -- pi_tmc_type
        left join t_tmc tt
          on tt.tmc_id = iptv.tmc_id
        left join t_org_tmc_status ots
          on ots.tmc_id = tt.tmc_id
        left Join t_Organizations org
          on org.Org_Id = ots.org_id
        left join t_ott_stb_model_info osmi
          on osmi.id = iptv.stb_model_id
         and iptv.priznak = pi_operation.tmcType -- pi_tmc_type
        left join t_stb_model smod
          on smod.id = osmi.model_stb_id
        Left Join t_Organizations org
          on org.Org_Id = tt.org_id
        left join t_dic_values dv
          on dv.dv_id = ots.status
        left join t_dic_region dr
          on osmi.region_id = dr.reg_id
      union all
      Select dr.kl_name || ' ' || dr.kl_socr region,
             nvl(t.imsi, iptv.serial_number) imsi,
             nvl(t.iccid, iptv.mac_address) iccid,
             smod.stb_model,
             osmi.retail_price,
             org.org_name,
             decode(dv.dv_name,
                    null,
                    decode(t.tmc_id,
                           null,
                           'Не зарегистрирована в системе',
                           'Не найдена на складе организации'),
                    dv.dv_name) status,
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
                    null) state,
             t.message
        from Table(l_Tmc_err) t
        left join t_tmc_iptv iptv
        -- stepanov-km 21.02.2017 добавлена связь по tmc_id
          on (t.tmc_id = iptv.tmc_id)
          or (t.tmc_id is null
         and t.imsi = iptv.serial_number
         and t.iccid = iptv.mac_address
         and iptv.priznak = pi_operation.tmcType) -- pi_tmc_type
        left join t_tmc tt
          on tt.tmc_id = iptv.tmc_id
        left join t_org_tmc_status ots
          on ots.tmc_id = tt.tmc_id
        left Join t_Organizations org
          on org.Org_Id = ots.org_id
        left join t_ott_stb_model_info osmi
          on osmi.id = iptv.stb_model_id
         and iptv.priznak = pi_operation.tmcType -- pi_tmc_type
        left join t_stb_model smod
          on smod.id = osmi.model_stb_id
        Left Join t_Organizations org
          on org.Org_Id = tt.org_id
        left join t_dic_values dv
          on dv.dv_id = ots.status
        left join t_dic_region dr
          on osmi.region_id = dr.reg_id;
    return res;
  end revise_ott_stb;

  ---------------------------------------------------------------------------------------
  -- Сохранение территории доставки оборудования
  ---------------------------------------------------------------------------------------
  procedure saveDogTerDelivery(pi_worker_id in number,
                               pi_terr      in org_territory_CH_tab,
                               pi_dog_id    in number,
                               po_err_num   out number,
                               po_err_msg   out varchar2) is
    PU_NAME constant varchar2(100) := '.saveDogTerDelivery';
    l_cnt        number;
    l_check      number;
    l_ter_id_old num_tab;
    l_count_max  number;
    --l_reg_id number;
    function getStrParam return varchar2 is
      l_str varchar2(2000);
    begin
      begin
        select substr(listagg(' terr_id=' || terr_id || ';' || 'region_id=' ||
                              region_id || ';' || 'kl_code=' || kl_code || ';' ||
                              'is_child_include=' || is_child_include || ';' ||
                              'is_access=' || is_access || ';' || 'email=' ||
                              email || ';' || 'T_ADDR_OBJ_ID=' ||
                              T_ADDR_OBJ_ID || ';' || 'phone=' || phone || ';')
                      within group(order by terr_id),
                      1,
                      2000)
          into l_str
          from table(pi_terr);
      exception
        when others then
          null;
      end;
      return ' pi_dog_id=' || pi_dog_id || ';' || l_str;
    end getStrParam;
  begin
    savepoint sp_saveDogTerDelivery;

    logging_pkg.debug(getStrParam, c_package || PU_NAME);
    select count(*)
      into l_check
      from t_dogovor d
      join (select PAYMENT_SUM from table(pi_terr)) tab
        on nvl(tab.PAYMENT_SUM, 0) < 1
     where d.dog_id = pi_dog_id
       and d.payment_type = 0;
    if l_check > 0 then
      po_err_num := -1;
      po_err_msg := 'Не указана сумма доставки по территориям!';
      return;
    end if;
    update t_territory_delivery_equip c
       set c.is_actual = 0
     where c.dog_id = pi_dog_id
       and not exists
     (select null from table(pi_terr) where terr_id = c.id);

    insert into t_territory_delivery_equip
      (dog_id,
       reg_id,
       mrf_id,
       kladr_id,
       is_child_include,
       is_access,
       email,
       phone,
       KL_LVL,
       is_actual,
       T_ADDR_OBJ_ID,
       PAYMENT_SUM,
       duration_guideline)
      select distinct pi_dog_id,
                      r.reg_id,
                      nvl(m.id, t.mrf_id),
                      nvl(t.kl_code, ar.kl_code),
                      t.is_child_include,
                      t.is_access,
                      t.email,
                      t.phone,
                      case
                        when t.kl_code is null then
                         ar.kl_lvladdrobj
                        else
                         cast(t.KL_LVL as varchar2(20))
                      end,
                      1,
                      nvl(t.T_ADDR_OBJ_ID, ar.id),
                      t.PAYMENT_SUM,
                      t.duration_guideline
        from table(pi_terr) t
        left join t_dic_region r
          on r.kl_region = t.region_id
        left join t_dic_mrf m
          on m.id = t.mrf_id
        left join T_ADDRESS_OBJECT ao
          on ao.kl_region = r.kl_region
         and ao.parent_id is null
         and NVL(ao.IS_DELETED, 0) = 0
        left join T_ADDRESS_REF ar
          on ao.id = ar.id
         and NVL(ar.IS_DELETED, 0) = 0
         and ar.SOURCE_TYPE in
             (SELECT dr.SOURCE_TYPE
                FROM T_DIC_REGION dr
               WHERE dr.REG_ID = t.region_id)
       where t.terr_id is null;

    update t_territory_delivery_equip c
       set (c.email,
            c.phone,
            c.payment_sum,
            c.duration_guideline,
            c.reg_id,
            c.mrf_id) =
           (select t.email,
                   t.phone,
                   t.payment_sum,
                   t.duration_guideline,
                   r.reg_id,
                   t.mrf_id
              from table(pi_terr) t
              left join t_dic_region r
                on r.kl_region = t.region_id
             where t.terr_id = c.id)
     where c.id in (select terr_id from table(pi_terr));

    insert into t_territory_delivery_equip_hst
      select SEQ_T_TERR_DELIV_EQUIP_HST.NEXTVAL, pi_worker_id, sysdate, c.*
        from t_territory_delivery_equip c
       where c.dog_id = pi_dog_id
         and c.is_actual = 1;

    select count(*)
      into l_cnt
      from T_TERR_DELIVERY_EQUIP_COUNT c
     where c.dog_id = pi_dog_id;

    if l_cnt = 0 then
      insert into T_TERR_DELIVERY_EQUIP_COUNT
        (dog_id, count_ter, TERR_ID)
        select pi_dog_id, 0, t.id
          from t_territory_delivery_equip t
         where t.dog_id = pi_dog_id;
    else
      select t.terr_id bulk collect
        into l_ter_id_old
        from T_TERR_DELIVERY_EQUIP_COUNT t
       where t.dog_id = pi_dog_id;
      insert into T_TERR_DELIVERY_EQUIP_COUNT
        (dog_id, count_ter, TERR_ID)
        select pi_dog_id, 0, t.id
          from t_territory_delivery_equip t
         where t.dog_id = pi_dog_id
           and t.id not in (select * from table(l_ter_id_old));
      --
    end if;
    for ora_up in (select co.dog_id, co.terr_id, der.t_addr_obj_id
                     from T_TERR_DELIVERY_EQUIP_COUNT co
                     join t_territory_delivery_equip der
                       on der.id = co.terr_id
                    where co.dog_id = pi_dog_id
                      and co.count_ter = 0) loop
      select min(tc.count_ter)
        into l_count_max
        from t_territory_delivery_equip de
        join T_TERR_DELIVERY_EQUIP_COUNT tc
          on tc.dog_id = de.dog_id
         and tc.terr_id = de.id
         and de.is_actual = 1
       where de.dog_id <> ora_up.dog_id
         and de.t_addr_obj_id = ora_up.t_addr_obj_id;

      update T_TERR_DELIVERY_EQUIP_COUNT t
         set t.count_ter = nvl(l_count_max, 0)
       where t.dog_id = ora_up.dog_id
         and t.terr_id = ora_up.terr_id;
    end loop;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg,
                        c_package || PU_NAME);
      return;
  end saveDogTerDelivery;

  -----------------------------------------------------------
  --Получение списка территорий по коду города или наименованию
  -----------------------------------------------------------
  function get_list_territory_by_city(pi_kl_region        in varchar2,
                                      pi_city_addr_obj_id in number,
                                      pi_model_tab        in ARRAY_NUM_3, -- модель - количество -- тип
                                      pi_date_beg         in date,
                                      pi_date_end         in date,
                                      pi_worker_id        in number,
                                      po_err_num          out number,
                                      po_err_msg          out varchar2)
    return deliv_org_tab is
    res deliv_org_tab;
    c_pr_name constant varchar2(65) := c_package ||
                                       '.get_list_territory_by_city';
    l_region     number;
    l_cities_ids num_tab := num_tab();
    --l_check_model number := 0;
    l_array_date2 array_date2;
    ex_fetch exception;
    l_date date;
    pragma exception_init(ex_fetch, -06504);
    l_tmc_count number;
    l_org_id    num_tab := num_tab();
    l_cnt       number;
    /****************************************/
    function getStrParam return varchar2 is
    begin
      return substr('pi_model_tab= ' ||
                    get_str_by_array_num_3(pi_model_tab) ||
                    ' pi_kl_region=' || pi_kl_region || ';' ||
                    'pi_city_addr_obj_id=' || pi_city_addr_obj_id || ';' ||
                    'pi_worker_id=' || pi_worker_id,
                    1,
                    2000);
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    begin
      select r.reg_id
        into l_region
        from t_dic_region r
       where r.kl_region = pi_kl_region;
    exception
      when no_data_found then
        po_err_num := 3;
        po_err_msg := 'Некорректный идентификатор региона';
        return null;
    end;
  
    if pi_city_addr_obj_id is not null then
      select count(*)
        into l_cnt
        from t_address_object ao
       where ao.id = pi_city_addr_obj_id
         and ao.is_deleted = 0
         and ao.kl_region = pi_kl_region;
    
      if l_cnt = 0 then
        po_err_num := 103;
        po_err_msg := 'Указанный код адресного объекта не найден в справочнике';
        return null;
      end if;
    end if;
  
    l_tmc_count := pi_model_tab.count;
  
    -- Ищем только те организации, на складе которых есть нужные модели в указанном количестве
    select distinct org_id bulk collect
      into l_org_id
      from (select org_id, sum(cnt)
              from (select org.org_id, count(*) cnt
                      from t_organizations org
                      join mv_org_tree tree
                        on org.org_id = tree.org_id
                       and tree.root_reltype in (999, 1004)
                       and org.is_enabled = 1
                      join t_dogovor td
                        on td.org_rel_id = tree.root_rel_id
                      join t_ott_stb_model_org osmo
                        on osmo.dog_id = td.dog_id
                       and osmo.is_enabled = 1
                       and nvl(osmo.date_end, sysdate) >= sysdate
                       and osmo.type_delivery = 2
                      join t_ott_stb_model_info si
                        on si.id = osmo.id_ott_stb
                      join t_stb_model sm
                        on sm.id = si.model_stb_id
                      join table(pi_model_tab) model
                        on model.number_1 = osmo.id_ott_stb
                       and model.number_3 <> 8
                     where td.dog_class_id in (10, 11, 12)
                       and exists (select 1
                              from t_dogovor_prm dp
                              join t_perm_to_perm pp
                                on pp.perm_dog = dp.dp_prm_id
                             where td.dog_id = dp.dp_dog_id
                               and dp.dp_is_enabled = 1
                               and pp.direction = 5017)
                     group by org.org_id
                    -- Симки
                    -- По постановке нам все равно, есть ли на складе организации данное количество симок
                    union all
                    select org.org_id, count(*) cnt
                      from t_organizations org
                      join mv_org_tree tree
                        on org.org_id = tree.org_id
                       and tree.root_reltype in (999, 1004)
                       and org.is_enabled = 1
                      join t_dogovor td
                        on td.org_rel_id = tree.root_rel_id
                    -- Лезу в тарифы, чтобы достать только для симок
                      join t_dic_mvno_region dmr
                        on dmr.reg_id = org.region_id
                      join t_tarif_by_at_id at
                        on at.at_region_id = dmr.id
                       and at.ver_date_end is null
                      join table(pi_model_tab) model
                        on model.number_1 = at.at_id
                       and model.number_3 = 8
                     where td.dog_class_id in (10, 11, 12)
                       and exists (select 1
                              from t_dogovor_prm dp
                              join t_perm_to_perm pp
                                on pp.perm_dog = dp.dp_prm_id
                             where td.dog_id = dp.dp_dog_id
                               and dp.dp_is_enabled = 1
                               and pp.direction = 5000)
                     group by org.org_id)
             group by org_id
            having sum(cnt) = l_tmc_count);
  
    if pi_city_addr_obj_id is not null then
      l_cities_ids := num_tab(pi_city_addr_obj_id);
    end if;
    if trunc(pi_date_beg) <> trunc(sysdate) then
      l_date := trunc(pi_date_beg);
    end if;
  
    --вернуть ид территории, наименование города, ид организации-исполнитель на этой территории
    with terr_tree as
     (select ao.id,
             CONNECT_BY_ROOT(ao.a_name) a_name,
             CONNECT_BY_ROOT(ao.a_full_name) a_full_name,
             CONNECT_BY_ROOT(ao.id) root_id
        from T_ADDRESS_OBJECT ao
       where ao.kl_region = pi_kl_region
      connect by prior ao.parent_id = ao.id
             and prior ao.kl_region = ao.kl_region
       start with ao.id in (select column_value from table(l_cities_ids) t)),
    current_tree as
     (select ao.id,
             ao.a_name      a_name,
             ao.a_full_name a_full_name,
             ao.id          root_id
        from T_ADDRESS_OBJECT ao
       where ao.kl_region = pi_kl_region
         and ao.id in (select column_value from table(l_cities_ids) t))
    select deliv_org_type(org_name           => org_name,
                          org_id             => org_id,
                          TERRITORY_ID       => id,
                          TERRITORY_PHONE    => phone,
                          TERRITORY_EMAIL    => email,
                          DURATION_GUIDELINE => duration_guideline,
                          PAYMENT_TYPE       => payment_type,
                          PAYMENT_SUM        => payment_sum,
                          PLAN_DATE          => null) bulk collect
      into res
      from (select distinct o.org_name,
                            o.org_id,
                            h.id,
                            h.phone,
                            h.email,
                            trunc(sysdate) + h.duration_guideline duration_guideline,
                            (case
                              when tt.payment = 0 then
                               td.payment_type
                              else
                               2
                            end) payment_type,
                            (case
                              when tt.payment = 0 and td.payment_type != 2 then
                               h.payment_sum
                              else
                               0
                            end) payment_sum,
                            null
              from (select distinct bb.id terr_id,
                                    bb.a_name,
                                    bb.a_full_name,
                                    bb.root_id,
                                    bb.payment
                      from (select t.id,
                                   aa.a_name,
                                   aa.a_full_name,
                                   aa.root_id,
                                   d.dog_id,
                                   (case
                                     when smo.payment_is_oper = 1 and
                                          nvl(smo.period_campaign_beg, sysdate) <=
                                          pi_date_end and
                                          nvl(smo.period_campaign_end, sysdate) >=
                                          least(pi_date_beg, sysdate) then
                                      1
                                     else
                                      0
                                   end) payment
                              from t_territory_delivery_equip t
                              join t_dogovor d
                                on d.dog_id = t.dog_id
                               and d.is_enabled = 1
                              join terr_tree aa
                                on aa.id = t.T_ADDR_OBJ_ID
                              join t_ott_stb_model_org smo
                                on smo.dog_id = d.dog_id
                               and smo.is_enabled = 1
                            -- Отбираем только с наличием на складе
                              join table(pi_model_tab) model
                                on model.number_1 = smo.id_ott_stb
                               and model.number_3 <> 8
                            
                             where t.reg_id = l_region
                               and t.is_actual = 1
                               and t.is_access = 1 --то что доступна "только в"
                               and t.is_child_include = 1 -- включая подчиненные
                            union
                            select t.id,
                                   aa.a_name,
                                   aa.a_full_name,
                                   aa.root_id,
                                   d.dog_id,
                                   (case
                                     when smo.payment_is_oper = 1 and
                                          nvl(smo.period_campaign_beg, sysdate) <=
                                          pi_date_end and
                                          nvl(smo.period_campaign_end, sysdate) >=
                                          least(pi_date_beg, sysdate) then
                                      1
                                     else
                                      0
                                   end) payment
                              from t_territory_delivery_equip t
                              join current_tree aa
                                on aa.id = t.T_ADDR_OBJ_ID
                              join t_dogovor d
                                on d.dog_id = t.dog_id
                               and d.is_enabled = 1
                              join t_ott_stb_model_org smo
                                on smo.dog_id = d.dog_id
                               and smo.is_enabled = 1
                            -- Отбираем только с наличием на складе
                              join table(pi_model_tab) model
                                on model.number_1 = smo.id_ott_stb
                               and model.number_3 <> 8
                             where t.reg_id = l_region
                               and t.is_actual = 1
                               and t.is_access = 1 --то что доступна "только в"
                               and t.is_child_include = 0 -- не включая подчиненные
                            union
                            select distinct t_reg.id,
                                            null a_name,
                                            null a_full_name,
                                            null root_id,
                                            d.dog_id,
                                            (case
                                              when smo.payment_is_oper = 1 and
                                                   nvl(smo.period_campaign_beg,
                                                       sysdate) <= pi_date_end and
                                                   nvl(smo.period_campaign_end,
                                                       sysdate) >=
                                                   least(pi_date_beg, sysdate) then
                                               1
                                              else
                                               0
                                            end) payment
                              from t_territory_delivery_equip t_reg
                              join t_dogovor d
                                on d.dog_id = t_reg.dog_id
                               and d.is_enabled = 1
                              left join terr_tree aa
                                on t_reg.reg_id = l_region
                               and t_reg.t_addr_obj_id = aa.id
                              join t_ott_stb_model_org smo
                                on smo.dog_id = d.dog_id
                               and smo.is_enabled = 1
                            -- Отбираем только с наличием на складе
                              join table(pi_model_tab) model
                                on model.number_1 = smo.id_ott_stb
                               and model.number_3 <> 8
                             where t_reg.reg_id = l_region
                               and aa.id is null
                               and t_reg.is_actual = 1
                               and t_reg.is_access = 0 --то что доступна "везде кроме" 
                               and t_reg.is_child_include = 1 --то что исключено вместе с детьми        
                            union
                            select distinct t_reg.id,
                                            null a_name,
                                            null a_full_name,
                                            null root_id,
                                            d.dog_id,
                                            (case
                                              when smo.payment_is_oper = 1 and
                                                   nvl(smo.period_campaign_beg,
                                                       sysdate) <= pi_date_end and
                                                   nvl(smo.period_campaign_end,
                                                       sysdate) >=
                                                   least(pi_date_beg, sysdate) then
                                               1
                                              else
                                               0
                                            end) payment
                              from t_territory_delivery_equip t_reg
                              join t_dogovor d
                                on d.dog_id = t_reg.dog_id
                               and d.is_enabled = 1
                              left join current_tree aa
                                on t_reg.reg_id = l_region
                               and t_reg.t_addr_obj_id = aa.id
                              join t_ott_stb_model_org smo
                                on smo.dog_id = d.dog_id
                               and smo.is_enabled = 1
                            -- Отбираем только с наличием на складе
                              join table(pi_model_tab) model
                                on model.number_1 = smo.id_ott_stb
                               and model.number_3 <> 8
                             where t_reg.reg_id = l_region
                               and aa.root_id is null
                               and t_reg.is_actual = 1
                               and t_reg.is_access = 0 --то что доступна "везде кроме" 
                               and t_reg.is_child_include = 0
                            
                            -- Симки 
                            union
                            select t.id,
                                   aa.a_name,
                                   aa.a_full_name,
                                   aa.root_id,
                                   d.dog_id,
                                   0 payment
                              from t_territory_delivery_equip t
                              join t_dogovor d
                                on d.dog_id = t.dog_id
                               and d.is_enabled = 1
                              join terr_tree aa
                                on aa.id = t.T_ADDR_OBJ_ID
                              join table(pi_model_tab) model
                                on model.number_3 = 8
                              join t_tarif_by_at_id tat
                                on model.number_1 = tat.at_id
                               and tat.ver_date_end is null
                             where t.reg_id = l_region
                               and t.is_actual = 1
                               and t.is_access = 1 --то что доступна "только в"
                               and t.is_child_include = 1 -- включая подчиненные
                            union
                            select t.id,
                                   aa.a_name,
                                   aa.a_full_name,
                                   aa.root_id,
                                   d.dog_id,
                                   0 payment
                              from t_territory_delivery_equip t
                              join current_tree aa
                                on aa.id = t.T_ADDR_OBJ_ID
                              join t_dogovor d
                                on d.dog_id = t.dog_id
                               and d.is_enabled = 1
                              join table(pi_model_tab) model
                                on model.number_3 = 8
                              join t_tarif_by_at_id tat
                                on model.number_1 = tat.at_id
                               and tat.ver_date_end is null
                             where t.reg_id = l_region
                               and t.is_actual = 1
                               and t.is_access = 1 --то что доступна "только в"
                               and t.is_child_include = 0 -- не включая подчиненные
                            union
                            select distinct t_reg.id,
                                            null     a_name,
                                            null     a_full_name,
                                            null     root_id,
                                            d.dog_id,
                                            0        payment
                              from t_territory_delivery_equip t_reg
                              join t_dogovor d
                                on d.dog_id = t_reg.dog_id
                               and d.is_enabled = 1
                              left join terr_tree aa
                                on t_reg.reg_id = l_region
                               and t_reg.t_addr_obj_id = aa.id
                              join table(pi_model_tab) model
                                on model.number_3 = 8
                              join t_tarif_by_at_id tat
                                on model.number_1 = tat.at_id
                               and tat.ver_date_end is null
                             where t_reg.reg_id = l_region
                               and aa.id is null
                               and t_reg.is_actual = 1
                               and t_reg.is_access = 0 --то что доступна "везде кроме" 
                               and t_reg.is_child_include = 1 --то что исключено вместе с детьми        
                            union
                            select distinct t_reg.id,
                                            null     a_name,
                                            null     a_full_name,
                                            null     root_id,
                                            d.dog_id,
                                            0        payment
                              from t_territory_delivery_equip t_reg
                              join t_dogovor d
                                on d.dog_id = t_reg.dog_id
                               and d.is_enabled = 1
                              left join current_tree aa
                                on t_reg.reg_id = l_region
                               and t_reg.t_addr_obj_id = aa.id
                              join table(pi_model_tab) model
                                on model.number_3 = 8
                              join t_tarif_by_at_id tat
                                on model.number_1 = tat.at_id
                               and tat.ver_date_end is null
                             where t_reg.reg_id = l_region
                               and aa.root_id is null
                               and t_reg.is_actual = 1
                               and t_reg.is_access = 0 --то что доступна "везде кроме" 
                               and t_reg.is_child_include = 0) bb) tt
              join t_territory_delivery_equip h
                on tt.terr_id = h.id
              join t_dogovor td
                on td.dog_id = h.dog_id
               and td.dog_class_id <> 1
              join mv_org_tree tree
                on tree.root_rel_id = td.org_rel_id
               and (tree.dog_id is null or tree.dog_id = td.dog_id)
              join t_organizations o
                on o.org_id = tree.org_id
             where o.org_id in (select * from table(l_org_id)))
     order by DURATION_GUIDELINE, PAYMENT_SUM;
    for i in 1 .. res.count loop
      if l_date is null then
        select max(d.temporary_indent)
          into l_date
          from t_territory_delivery_equip ter
          join t_dogovor d
            on d.dog_id = ter.dog_id
           and ter.id = res(i).TERRITORY_ID;
        if l_date is null then
          l_date := trunc(pi_date_beg);
        else
          if to_char(l_date, 'hh24') < to_char(sysdate, 'hh24') or
             (to_char(l_date, 'hh24') = to_char(sysdate, 'hh24') and
              to_char(l_date, 'mi') < to_char(sysdate, 'mi')) then
            l_date := trunc(pi_date_beg) + 1;
          else
            l_date := trunc(pi_date_beg);
          end if;
        end if;
      end if;
      select rec_date2(date_1 => to_date(to_char(tab_date.lvl_date,
                                                 'dd.mm.yyyy') || ' ' ||
                                         h24.ampm1,
                                         'dd.mm.yyyy hh24:mi:ss'),
                       date_2 => to_date(to_char(tab_date.lvl_date,
                                                 'dd.mm.yyyy') || ' ' ||
                                         h24.ampm2,
                                         'dd.mm.yyyy hh24:mi:ss')) bulk collect
        into l_array_date2
        from (Select l_date + level - 1 lvl_Date,
                     to_char(l_date + level - 1, 'd') day_week
                from dual
              connect by level <= EXTRACT(DAY FROM(pi_date_end - l_date) DAY TO
                                          SECOND)) tab_date
        join (select '09:00' ampm1, '12:00' ampm2, 1 ord
                from dual
              union
              select '12:00' ampm1, '15:00' ampm2, 2 ord
                from dual
              union
              select '15:00' ampm1, '18:00' ampm2, 3 ord
                from dual
              union
              select '18:00' ampm1, '21:00' ampm2, 4 ord
                from dual) h24
          on 1 = 1
        join t_territory_delivery_equip ter
          on ter.id = res(i).TERRITORY_ID
        join t_dogovor d
          on d.dog_id = ter.dog_id
         and d.dog_class_id <> 1
        join t_org_relations rel
          on rel.id = d.org_rel_id
        join t_org_ssc_timetable st
          on st.org_id = rel.org_id
         and st.day_number = tab_date.day_week
         and st.is_enabled = 1
       where ((substr(h24.ampm1, 1, 2) >= substr(st.work_start, 5, 2) and
             substr(h24.ampm1, 1, 2) < substr(st.work_end, 5, 2)) or
             (substr(h24.ampm2, 1, 2) > substr(st.work_start, 5, 2) and
             substr(h24.ampm2, 1, 2) < substr(st.work_end, 5, 2)))
         and (tab_date.lvl_date <> trunc(sysdate) or
             to_char(sysdate, 'hh24') < substr(h24.ampm2, 1, 2))
         and trunc(sysdate) + nvl(ter.duration_guideline, 0) < =
             tab_date.lvl_Date
       order by tab_date.lvl_Date, ord;
      res(i).PLAN_DATE := l_array_date2;
    end loop;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg, c_pr_name);
      return null;
  end;
  ------------------------------------------------------------
  --Получение списка территорий по организации
  --------------------------------------------------------------
  function get_territory_by_dog(pi_dog_id    in number,
                                pi_worker_id in number,
                                po_err_num   out number,
                                po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
    PU_NAME constant varchar2(100) := '.get_territory_by_org';
  begin
    open res for
      select c.dog_id,
             r.kl_region          reg_id,
             c.T_ADDR_OBJ_ID,
             c.email,
             c.phone,
             ao.a_name,
             ao.a_socr,
             c.T_ADDR_OBJ_ID      kl_code,
             c.id,
             c.is_child_include,
             c.is_access,
             c.kl_lvl,
             c.PAYMENT_SUM,
             c.duration_guideline,
             c.mrf_id
        from t_territory_delivery_equip c
        left join t_dic_region r
          on c.reg_id = r.reg_id
        left join T_ADDRESS_OBJECT ao
          on /* ao.kl_region = r.kl_region
                                                                                                                                                                                                                                                                                                                                                                           and*/
       ao.id = c.T_ADDR_OBJ_ID
       where c.dog_id = pi_dog_id
         and c.is_actual = 1
       order by c.id;

    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg,
                        c_package || PU_NAME);
      return null;
  end;

  ---------------------------------------------------------------------------------------
  -- Проверка, является ли пользователь курьером
  ---------------------------------------------------------------------------------------
  function isUserCourier(pi_worker_id in number,
                         po_err_num   out number,
                         po_err_msg   out varchar2) return number is
    l_count_courier number;
  begin
    select count(r.role_id)
      into l_count_courier
      from t_roles r
      join t_user_org uo
        on r.role_id = uo.role_id
      join t_system_parameters p
        on p.value = r.role_parent_id
     where p.sys_key = 'COURIER'
       and uo.usr_id = pi_worker_id
       and rownum = 1;
    return l_count_courier;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return 0;
  end isUserCourier;

  ---------------------------------------------------------------------------------------
  -- Сохранение расписания курьера
  ---------------------------------------------------------------------------------------
  procedure saveCourierSchedule(pi_user_id          in t_users.usr_id%type,
                                pi_courier_schedule in org_timetable_tab,
                                po_err_num          out number,
                                po_err_msg          out varchar2) is
    c_work_start constant interval day to second := interval '0 00:00:00' day to
                                                    second;
    c_work_end   constant interval day to second := interval '0 23:59:00' day to
                                                    second;
    PU_NAME      constant varchar2(100) := '.saveCourierSchedule';
  begin
    -- История
    INSERT INTO T_COURIER_SCHEDULE_HST
      (ACTION,
       CHANGE_DATE,
       USER_ID,
       DAY_NUMBER,
       WORK_START,
       WORK_END,
       BREAK_START,
       BREAK_END,
       IS_ENABLED)
      SELECT 'UPDATE',
             SYSDATE,
             tt.user_id,
             tt.DAY_NUMBER,
             tt.WORK_START,
             tt.WORK_END,
             tt.BREAK_START,
             tt.BREAK_END,
             tt.IS_ENABLED
        FROM T_COURIER_SCHEDULE tt
       WHERE tt.user_id = PI_USER_ID;
    -- Обновляем информацию в таблице T_COURIER_SCHEDULE
    MERGE INTO T_COURIER_SCHEDULE tt
    USING (SELECT p.DAY_NUMBER,
                  p.WORK_START,
                  p.WORK_END,
                  p.FULLTIME,
                  p.BREAK_START,
                  p.BREAK_END,
                  p.WITHOUT_BREAK,
                  p.IS_ENABLED
             FROM TABLE(PI_COURIER_SCHEDULE) p) ptt
    ON (tt.DAY_NUMBER = ptt.DAY_NUMBER AND tt.user_id = pi_user_id)
    WHEN MATCHED THEN
      UPDATE
         SET tt.IS_ENABLED  = ptt.IS_ENABLED,
             tt.WORK_START  = CASE
                                WHEN ptt.FULLTIME = 1 THEN
                                 c_work_start
                                ELSE
                                 ptt.WORK_START
                              END,
             tt.WORK_END    = CASE
                                WHEN ptt.FULLTIME = 1 THEN
                                 c_work_end
                                ELSE
                                 ptt.WORK_END
                              END,
             tt.BREAK_START = CASE
                                WHEN ptt.WITHOUT_BREAK = 1 THEN
                                 NULL
                                ELSE
                                 ptt.BREAK_START
                              END,
             tt.BREAK_END   = CASE
                                WHEN ptt.WITHOUT_BREAK = 1 THEN
                                 NULL
                                ELSE
                                 ptt.BREAK_END
                              END WHEN NOT MATCHED THEN INSERT(tt.User_Id, tt.IS_ENABLED, tt.DAY_NUMBER, tt.WORK_START, tt.WORK_END, tt.BREAK_START, tt.BREAK_END) VALUES(PI_USER_ID, ptt.IS_ENABLED, ptt.DAY_NUMBER,CASE
               WHEN ptt.FULLTIME = 1 THEN
                c_work_start
               ELSE
                ptt.WORK_START
             END,CASE
               WHEN ptt.FULLTIME = 1 THEN
                c_work_end
               ELSE
                ptt.WORK_END
             END,CASE
               WHEN ptt.WITHOUT_BREAK = 1 THEN
                NULL
               ELSE
                ptt.BREAK_START
             END,CASE
               WHEN ptt.WITHOUT_BREAK = 1 THEN
                NULL
               ELSE
                ptt.BREAK_END
             END);
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg,
                        c_package || PU_NAME);
  end saveCourierSchedule;

  ---------------------------------------------------------------------------------------
  -- Получение расписания курьера
  ---------------------------------------------------------------------------------------
  function getCourierSchedule(pi_user_id in t_users.usr_id%type,
                              po_err_num out number,
                              po_err_msg out varchar2) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select t.user_id,
             t.IS_ENABLED,
             t.DAY_NUMBER,
             CASE
               WHEN t.WORK_START = c_work_start AND t.WORK_END = c_work_end THEN
                NULL
               ELSE
                t.WORK_START
             END as WORK_START,
             CASE
               WHEN t.WORK_START = c_work_start AND t.WORK_END = c_work_end THEN
                NULL
               ELSE
                t.WORK_END
             END as WORK_END,
             CASE
               WHEN t.WORK_START = c_work_start AND t.WORK_END = c_work_end THEN
                1
               ELSE
                0
             END AS fulltime,
             t.BREAK_START,
             t.BREAK_END,
             CASE
               WHEN t.BREAK_START IS NULL AND t.BREAK_END IS NULL THEN
                1
               ELSE
                0
             END AS without_break
        from T_COURIER_SCHEDULE t
       where t.user_id = pi_user_id
         AND t.IS_ENABLED = 1;
    return res;
  exception
    when others then
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      po_err_num := sqlcode;
      return null;
  end getCourierSchedule;

  ----------------------------------------------------------------------------------------
  -- Резервирование ТМЦ
  ----------------------------------------------------------------------------------------
  function reserv_ott_stb(pi_reserv_id in num_tab,
                          pi_org_id    in number,
                          pi_date_beg  in timestamp,
                          pi_date_end  in timestamp,
                          pi_model_tab in ARRAY_NUM_2, -- модель - количество -- тип
                          pi_worker_id in number,
                          po_err_num   out pls_integer,
                          po_err_msg   out varchar2) return num_tab is
    c_pr_name constant varchar2(80) := c_package || 'reserv_ott_stb';
    l_cnt number;
    res   num_tab := num_tab();
    l_id  number;
    i     number := 1;
    l_err varchar2(3000);
    ex_continue exception;
    l_tmc_id         num_tab;
    l_unreserved_tmc number := 0;
    l_cnt_reserv     number;
  
    -----------------------------------------------------
    function getStrParam return varchar2 is
    begin
      return substr('pi_org_id=' || pi_org_id || ';' || 'pi_date_beg=' ||
                    pi_date_beg || ';' || 'pi_date_end=' || pi_date_end || ';' ||
                    'pi_model_tab=' ||
                    get_str_by_array_num_2(pi_model_tab) || ';' ||
                    'pi_worker_id=' || pi_worker_id || ';' ||
                    'pi_reserv_id=' || get_str_by_num_tab(pi_reserv_id),
                    1,
                    2000);
    end getStrParam;
    -----------------------------------------------------
  begin
  
    logging_pkg.debug(getStrParam, c_pr_name);
    savepoint sp_begin;
  
    if pi_reserv_id is not null and pi_reserv_id.count <> 0 then
      -- Отменяем бронь
      update t_tmc_ex_works_reserv t
         set t.date_end = sysdate
       where t.id in (select * from table(pi_reserv_id))
         /*and (t.model_id, t.cnt) in (select * from table(pi_model_tab))*/;
      res := pi_reserv_id;
    else
      /*select o.unreserved_tmc
       into l_unreserved_tmc
       from t_organizations o
      where o.org_id = pi_org_id;*/
      -- Резервируем оборудование
      for m in (select distinct number_1, number_2, sm.stb_model, sm.priznak
                  from table(pi_model_tab)
                  join t_ott_stb_model_info osmi
                    on osmi.id = number_1
                  join t_stb_model sm
                    on sm.id = osmi.model_stb_id) loop
        begin
          -- Проверка, что на складе есть такое количество оборудования
          select nvl(sum(t.cnt), 0)
            into l_cnt_reserv -- Количество зарезервированного
            from t_tmc_ex_works_reserv t
           where t.org_id = pi_org_id
             and t.model_id = m.number_1
             and nvl(t.date_end, sysdate) >= sysdate;
          -- Количество небронируемого
          select nvl(sum(t.cou), 0)
            into l_unreserved_tmc
            from t_org_tmc_unreserv t
           where t.org_id = pi_org_id
             and t.tmc_type = m.priznak;
          -- На складе
          select count(*)
            into l_cnt
            from t_tmc_iptv ti
            join t_org_tmc_status st
              on st.tmc_id = ti.tmc_id
            join t_tmc t
              on ti.tmc_id = t.tmc_id
            join t_ott_stb_model_org so
              on so.id_ott_stb = ti.stb_model_id
            join t_ott_stb_model_info i
              on i.id = so.id_ott_stb
            join t_stb_model dic
              on dic.id = i.model_stb_id
           where ti.state_id = 1
             and ti.priznak not in (7005)
             and st.org_id = pi_org_id
             and ti.stb_model_id = m.number_1
             and (st.dog_id is null or so.dog_id = st.dog_id)  
             and nvl(so.date_end, sysdate) >= sysdate
             and so.type_delivery = 2                 
                -- Назначение Самовывоз
             and (is_org_usi(st.org_id) = 1 or t.tmc_perm = 5019);
          l_cnt := l_cnt - l_unreserved_tmc - l_cnt_reserv;
          if l_cnt < m.number_2 then
            raise ex_continue;
          end if;
        
          -- Если есть, то блокируем ТМЦ
          select ti.tmc_id bulk collect
            into l_tmc_id
            from t_tmc_iptv ti
            join t_org_tmc_status st
              on st.tmc_id = ti.tmc_id
            join t_tmc t
              on ti.tmc_id = t.tmc_id
           where ti.state_id = 1
             and ti.priznak not in (7005)
             and st.org_id = pi_org_id
             and ti.stb_model_id = m.number_1
             and rownum <= m.number_2 + l_unreserved_tmc
                -- Назначение Самовывоз
             and (is_org_usi(st.org_id) = 1 or t.tmc_perm = 5019)
             for update skip locked;
          if l_tmc_id.count < m.number_2 then
            raise ex_continue;
          end if;
        
          insert into t_tmc_ex_works_reserv
            (ORG_ID, DATE_BEG, DATE_END, MODEL_ID, CNT)
          values
            (PI_ORG_ID, PI_DATE_BEG, PI_DATE_END, m.number_1, m.number_2)
          returning id into l_id;
          res.extend;
          res(i) := l_id;
          i := i + 1;
        
        exception
          when ex_continue then
            if l_err is null then
              l_err := l_err || 'Оборудование "' || m.stb_model || '"';
            else
              l_err := l_err || ', "' || m.stb_model || '"';
            end if;
        end;
      end loop;
    end if;
  
    if l_err is not null then
      po_err_num := 1;
      po_err_msg := l_err ||
                    ' отсутствует на складе выбранной организации.';
      rollback to sp_begin;
      return null;
    end if;
    --
    --res:= num_tab(1/*,2,3*/);
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg || ' ' || getStrParam, c_pr_name);
      rollback to sp_begin;
      return null;
  end;

  ----------------------------------------------------------------------------------------
  -- Продление брони
  ----------------------------------------------------------------------------------------
  procedure extend_reserv(pi_reserv_id in num_tab,
                          pi_worker_id in number,
                          po_err_num   out pls_integer,
                          po_err_msg   out varchar2) is
    c_pr_name constant varchar2(80) := c_package || 'extend_reserv';
    --l_cnt number;
    --res   num_tab := num_tab();
    --l_id  number;
    --i     number := 1;
    --l_err varchar2(3000);
    ex_continue exception;
    function getStrParam return varchar2 is
    begin
      return substr('pi_worker_id=' || pi_worker_id || ';' ||
                    'pi_reserv_id=' || get_str_by_num_tab(pi_reserv_id),
                    1,
                    2000);
    end getStrParam;
    -----------------------------------------------------
  begin
  
    logging_pkg.debug(getStrParam, c_pr_name);
    update t_tmc_ex_works_reserv t
       set t.date_end = null
     where t.id in (select * from table(pi_reserv_id));
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg, c_pr_name);
  end extend_reserv;

  ----------------------------------------------------------------------------------------
  -- Сообщение о количестве доступных ТМЦ для продажи по организации
  ----------------------------------------------------------------------------------------
  function get_tmc_sell_msg_by_org(pi_org_id    in number,
                                   pi_tmc_type  in number,
                                   pi_worker_id in number,
                                   po_err_num   out number,
                                   po_err_msg   out varchar2)
    return sys_refcursor is
    c_pr_name constant varchar2(65) := c_package ||
                                       '.get_tmc_sell_msg_by_org';
    res sys_refcursor;
    --l_reg_tab num_tab := num_tab();
    /****************************************/
    function getStrParam return varchar2 is
    begin
      return 'pi_org_id=' || pi_org_id || ';' || 'pi_worker_id=' || pi_worker_id;
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);

    open res for
      select o.org_id,
             o.org_name,
             t.stb_model_id,
             sm.stb_model,
             osmi.is_second_hand,
             count(ots.tmc_id) - nvl(sum(t.cnt), 0) cnt
        from t_organizations o
        join t_org_tmc_status ots
          on ots.org_id = o.org_id
         and ots.status = 11
        join t_tmc_iptv t
          on t.tmc_id = ots.tmc_id
         and t.priznak not in (7005)
        join t_tmc tt
          on tt.tmc_id = t.tmc_id
        join t_ott_stb_model_info osmi
          on osmi.id = t.stb_model_id
        join t_stb_model sm
          on sm.id = osmi.model_stb_id
        left join t_tmc_ex_works_reserv t
          on o.org_id = t.org_id
         and nvl(t.date_end, sysdate) >= sysdate
         and t.stb_model_id = osmi.id
         and is_org_usi(t.org_id) = 1 -- На агентах вычитать не надо, согласовано с Олей М.
       where o.org_id = pi_org_id
         and (is_org_usi(ots.org_id) = 1 or tt.tmc_perm = 5014)
         and t.priznak = pi_tmc_type
       group by o.org_id, o.org_name, t.stb_model_id, sm.stb_model, osmi.is_second_hand;

    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(getStrParam || ' ' || po_err_msg, c_pr_name);
      return null;
  end get_tmc_sell_msg_by_org;

end tmc_ott_stb;
/
