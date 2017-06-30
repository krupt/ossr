CREATE OR REPLACE PACKAGE BODY TMC_AB is

  c_package constant varchar2(30) := 'TMC_AB.';

  ex_funds_not_enough exception;

  ----------------------------------------------------------------------

  function Get_Real_Abon_Status(pi_abonent         in abonent_type,
                                pi_tmc_id          in T_TMC.TMC_ID%type,
                                pi_worker_id       in T_USERS.USR_ID%type,
                                pi_savetype        in number,
                                pi_is_edit         in number,
                                pi_req_delivery_id in t_request_delivery.id%type,
                                po_err_code        out number,
                                po_error           out varchar2)
    return number is
    l_vis_auto  number;
    l_status    number := 100;
    l_err_num   number;
    l_err_msg   varchar2(2000);
    l_check_acc number;
    l_count     number;
    l_cc_id     number;
  begin
    -- проверим разрешено ли автовизирование
    if (is_org_usi(pi_abonent.org_id) > 0) then
      -- если организация является оператором, то договор у нее отсутствует,
      -- и визирование всегда автоматическое
      l_vis_auto := 1;
    else
      begin
        select dog.is_vis_auto
          into l_vis_auto
          from t_dogovor dog
         where dog.dog_id in
               (select td.dog_id
                  from mv_org_tree tree
                  join t_dogovor td
                    on td.org_rel_id = tree.root_rel_id
                  join t_dogovor_prm dp
                    on td.dog_id = dp.dp_dog_id
                   and dp.dp_is_enabled = 1
                 where tree.org_id = pi_abonent.org_id
                   and td.is_enabled = 1
                   and dp.dp_prm_id = 2000
                   and ((pi_req_delivery_id is null and
                       td.dog_class_id <> 11) or (pi_req_delivery_id is not null and
                       td.dog_class_id = 11)));
      exception
        when no_data_found then
          logging_pkg.Raise_App_Err(-1000,
                                    'Договор не найден.');
      end;
    end if;
  
    -- проверяем совпадение расчетной суммы подключения с введенной
    -- пользователем, кроме случая, когда производится визирование
    -- раннее созданного подключения
    if (pi_is_edit = 0 or (pi_is_edit = 1 and pi_savetype = 1)) then
      if (pi_abonent.cost > /*!=*/
         pi_abonent.paid and pi_abonent.equipment_required = 0) then
        po_error := 'Расчетная стоимость подключения не совпадает с внесенной суммой.;';
      end if;
    end if;
  
    -- проверяем, указан ли продавец
    if (pi_abonent.user_id is null) then
      po_error := po_error || 'Не указан продавец.;';
    end if;
  
    if pi_abonent.reg_id_ps is null and pi_abonent.abonent_id is not null then
      select a.cc_id
        into l_cc_id
        from t_abonent a
       where a.ab_id = pi_abonent.abonent_id;
      select count(*)
        into l_count
        from t_org_calc_center occ
        join t_CALC_CENTER cc
          on cc.cc_is_ps = 1
         and cc.cc_id = occ.cc_id
       where occ.org_id = pi_abonent.org_id
         and cc.cc_id = l_cc_id;
    else
      -- проверяем соответствие расчетного центра организации выбранному значению
      select count(*)
        into l_count
        from t_org_calc_center occ
        join t_CALC_CENTER cc
          on cc.cc_is_ps = 1
         and cc.cc_id = occ.cc_id
        join t_dic_region dr
          on cc.cc_region_id = dr.reg_id
        join t_dic_mvno_region m
          on m.reg_id = dr.reg_id
       where occ.org_id = pi_abonent.org_id
         and m.id = pi_abonent.reg_id_ps --dr.kl_region = pi_abonent.KL_REGION_ID
      ;
    end if;
    if (l_count = 0) then
      po_error := po_error ||
                  'Выбранный расчетный центр не соответствует организации.';
      logging_pkg.debug('pi_abonent.org_id=' || pi_abonent.org_id ||
                        ' pi_abonent.reg_id_ps' || pi_abonent.reg_id_ps,
                        'Get_Real_Abon_Status');
    end if;
  
    if (po_error is not null) then
      po_err_code := -104;
      return l_status;
    end if;
  
    -- При регистрации подключения автоматически визируем его, если нет ошибок и разрешено автовизирование
    -- для организации. При редактировании визируем, если нет ошибок и поступила команда "Визировать"
    if (pi_savetype = 2 and pi_abonent.err_code = 0 and
       (l_vis_auto = 1 or pi_is_edit = 1)) then
      l_status := 102; -- статус "готов" (визирован)
    end if;
  
    if (l_status = 102) then
      -- проверяем достаточно ли денег на счете
      /*      todo вместо pi_abonent.org_id нужно передавать dog_id
      06/07/2009 e.komissarov временно
      l_check_acc := Acc_operations.Check_Acc_For_Tmc_Sell(pi_tmc_id,
                                                                 pi_abonent.paid,
                                                                 pi_abonent.org_id,
                                                                 pi_worker_id,
                                                                 l_err_num,
                                                                 l_err_msg);
      */
      if (l_err_num != 0) then
        logging_pkg.Raise_App_Err(l_err_num, l_err_msg);
        if (l_check_acc != 1) then
          l_status    := 100;
          po_err_code := -102;
          po_error    := 'Недостаточно денег на счете для создания подключения.';
        end if;
      end if;
    end if;
    return l_status;
  end Get_Real_Abon_Status;

  --------------------------------------------------------------------------------------

  function Get_Tmc_By_Imsi(pi_imsi in number, pi_type in number)
    return T_TMC.TMC_ID%type is
    res      T_TMC.TMC_ID%type;
    l_isimsi integer := 0;
  begin
    if (pi_type = 51) then
      -- sim карта
      l_isimsi := Util.Is_Card_Num_Imsi(pi_imsi);
      -- e.komissarov 27/09/2010 убраны лишние условия и таблицы
      /*      select t.tmc_id
              into res
              from t_tmc t, t_tmc_sim s
             where (l_isimsi = 1 and s.sim_imsi = pi_imsi or
                   l_isimsi = 0 and s.sim_iccid = pi_imsi)
               and s.tmc_id = t.tmc_id
               and t.tmc_type = 8;
      */
      select s.tmc_id
        into res
        from t_tmc_sim s
       where (l_isimsi = 1 and s.sim_imsi = to_char(pi_imsi) or
             l_isimsi = 0 and s.sim_iccid = to_char(pi_imsi));

    elsif (pi_type = 53) then
      -- ruim карта
      select t.tmc_id
        into res
        from t_tmc t, t_tmc_ruim r
       where r.ruim_imsi = pi_imsi
         and r.tmc_id = t.tmc_id
         and t.tmc_type = 9;
    end if;
    return res;
  exception
    when no_data_found then
      logging_pkg.Raise_App_Err(-100,
                                'Ошибка: Поиск ТМЦ по IMSI не дал результатов.');
  end Get_Tmc_By_Imsi;

  --------------------------------------------------------------------------------------

  procedure Change_Tar(pi_tmc_id     t_tmc.tmc_id%type,
                       pi_org_id     t_organizations.org_id%type,
                       pi_new_tar_id t_tmc_sim.tar_id%type,
                       pi_worker_id  t_users.usr_id%type) is
    l_op_id      t_tmc_operations.op_id%type;
    l_old_tar_id t_tmc_sim.tar_id%type;
    l_sim_id     t_tmc_sim.tmc_id%type;
    l_imsi       t_tmc_sim.sim_imsi%type;
    l_callsign   t_tmc_sim.sim_callsign%type;
    l_sim_type   number;
    -- 57964
    l_sim_perm number;
  begin
    -- получим старый ТП, позывной, imsi и идентификатор карты
    begin
      select s.tmc_id,
             s.sim_imsi,
             s.sim_callsign,
             s.tar_id,
             s.sim_type,
             -- 57964
             t.tmc_perm
        into l_sim_id,
             l_imsi,
             l_callsign,
             l_old_tar_id,
             l_sim_type,
             -- 57964
             l_sim_perm
        from t_tmc_sim s,
             -- 57964
             t_tmc t
       where s.tmc_id =
             (select t.tmc_id from t_tmc t where t.tmc_id = pi_tmc_id)
         and t.tmc_id = s.tmc_id;
    exception
      when no_data_found then
        logging_pkg.Raise_App_Err(-1000,
                                  'Ошибка при смене ТП: сим-карта не найдена.');
    end;

    insert into t_tmc_operations
      (op_type, org_id, user_id, op_date, op_dog_id)
    values
      (26,
       pi_org_id,
       pi_worker_id,
       systimestamp,
       ACC_OPERATIONS.GetDogByTmcId(pi_tmc_id, pi_org_id))
    returning op_id into l_op_id;
    -- Добавляем юнит операции
    insert into t_tmc_operation_units
      (op_id,
       tmc_id,
       tar_id_0,
       tar_id_1,
       callsign_0,
       callsign_1,
       owner_id_0,
       owner_id_1,
       error_id,
       st_sklad_0,
       st_sklad_1,
       imsi_num,
       sim_type,
       -- 57964
       sim_perm)
    values
      (l_op_id,
       pi_tmc_id,
       l_old_tar_id,
       pi_new_tar_id,
       l_callsign,
       l_callsign,
       pi_org_id,
       pi_org_id,
       0,
       11,
       11,
       l_imsi,
       l_sim_type,
       -- 57964
       l_sim_perm);

    -- меняем ТП у сим-карты
    update t_tmc_sim s
       set s.tar_id = pi_new_tar_id
     where s.tmc_id = l_sim_id;

  end Change_Tar;
  --------------------------------------------------------------------------------------
procedure Change_Tariff(pi_tmc_id     t_tmc.tmc_id%type,
                          pi_org_id     t_organizations.org_id%type,
                          pi_new_tar_id t_tmc_sim.tar_id%type,
                          pi_worker_id  t_users.usr_id%type) is
    l_op_id      t_tmc_operations.op_id%type;
    l_old_tar_id t_tmc_sim.tar_id%type;
    l_sim_id     t_tmc_sim.tmc_id%type;
    l_imsi       t_tmc_sim.sim_imsi%type;
    l_callsign   t_tmc_sim.sim_callsign%type;
    l_sim_type   number;
    -- 57964
    l_sim_perm number;
    l_cost     number;
  begin
    -- получим старый ТП, позывной, imsi и идентификатор карты
    begin
      select s.tmc_id,
             s.sim_imsi,
             s.sim_callsign,
             s.tar_id,
             s.sim_type,
             -- 57964
             t.tmc_perm,
             t.tmc_tmp_cost
        into l_sim_id,
             l_imsi,
             l_callsign,
             l_old_tar_id,
             l_sim_type,
             -- 57964
             l_sim_perm,
             l_cost
        from t_tmc_sim s,
             -- 57964
             t_tmc t
       where s.tmc_id =
             (select t.tmc_id from t_tmc t where t.tmc_id = pi_tmc_id)
            -- 57964
         and t.tmc_id = s.tmc_id;
    exception
      when no_data_found then
        logging_pkg.Raise_App_Err(-1000,
                                  'Ошибка при смене ТП: сим-карта не найдена.');
    end;

    insert into t_tmc_operations
      (op_type, org_id, user_id, op_date, op_dog_id)
    values
      (1902,
       pi_org_id,
       pi_worker_id,
       systimestamp,
       ACC_OPERATIONS.GetDogByTmcId(pi_tmc_id, pi_org_id))
    returning op_id into l_op_id;
    -- Добавляем юнит операции
    insert into t_tmc_operation_units
      (op_id,
       tmc_id,
       tar_id_0,
       tar_id_1,
       callsign_0,
       callsign_1,
       owner_id_0,
       owner_id_1,
       error_id,
       st_sklad_0,
       st_sklad_1,
       imsi_num,
       sim_type,
       -- 57964
       sim_perm,
       tmc_op_cost)
    values
      (l_op_id,
       pi_tmc_id,
       l_old_tar_id,
       pi_new_tar_id,
       l_callsign,
       l_callsign,
       pi_org_id,
       pi_org_id,
       0,
       11,
       11,
       l_imsi,
       l_sim_type,
       -- 57964
       l_sim_perm,
       l_cost);

    -- меняем ТП у сим-карты
    update t_tmc_sim s
       set s.tar_id = pi_new_tar_id
     where s.tmc_id = l_sim_id;

  end Change_Tariff;

  --------------------------------------------------------------------------------------
  --------------------------------------------------------------------------------------

  function Add_Tmc_Sell_Operation(pi_org_id          in T_ORGANIZATIONS.ORG_ID%type,
                                  pi_tmc_id          in T_TMC.TMC_ID%type,
                                  pi_callsign        in number,
                                  pi_imsi            in varchar2,
                                  pi_tar_id          in T_ABSTRACT_TAR.AT_ID%type,
                                  pi_abonent_type    in T_TARIFF2.TYPE_VDVD_ID%type,
                                  pi_related         in integer,
                                  pi_is_alien_ab     in number,
                                  pi_req_delivery_id in t_request_delivery.id%type,
                                  pi_worker_id       in T_USERS.USR_ID%type,
                                  po_err_num         out pls_integer,
                                  po_err_msg         out t_Err_Msg)
    return number is
    l_op_id            T_TMC_OPERATIONS.OP_ID%type;
    l_callsign_city    T_CALLSIGN.CITY_CALLSIGN%type;
    l_color            T_CALLSIGN.COLOR%type;
    l_is_tech          T_TARIFF2.IS_TECH%type;
    ll_cs_id           t_callsign.tmc_id%type;
    l_callsign_city_id t_callsign_city.tmc_id%type;
    l_dog_id           number;
    l_old_tar          number;
    l_op_tar_id        number;
    l_unit_tar_id      number;
    l_sim_type         number;
  begin
  
    -- удалим ТМЦ со склада
    delete from t_org_tmc_status ots
     where ots.tmc_id = pi_tmc_id
       and ots.org_id = pi_org_id
       and ots.status = 11;
    if (sql%notfound) then
      po_err_num := 4;
      po_err_msg := 'ТМЦ отсутствует на складе подключающей организации либо имеет статус, запрещающий продажу.';
    end if;
  
    -- удалим инф-ию о владельце карты  
    if (pi_abonent_type = 51) then
      update t_tmc t set t.org_id = null where t.tmc_id = pi_tmc_id;
    elsif (pi_abonent_type = 53) then
      update t_tmc t set t.org_id = null where t.tmc_id = pi_tmc_id;
    end if;
  
    if (sql%notfound) then
      po_err_num := 5;
      po_err_msg := 'Нет данных о привязке ТМЦ к подключающей организации.';
    end if;
  
    -- e.komissarov 07/07/2009 только для связанных карт
    if (pi_abonent_type = 51 and pi_related = 1) then
      -- Проверим наличие у сим-карты Единого ТП
      begin
        select t2.is_tech
          into l_is_tech
          from t_tmc t, t_tmc_sim s, t_tariff2 t2
         where t.tmc_id = pi_tmc_id
           and t.tmc_id = s.tmc_id
              -- e.komissarov 07/07/2009 только для SIM
           and t.tmc_type = tmc.c_tmc_sim_id
           and t2.id = get_last_tar_id_by_at_id(s.tar_id);
        if (l_is_tech = 1) then
          -- Выполняем смену с Единого ТП на выбранный ТП
          Change_Tariff(pi_tmc_id, pi_org_id, pi_tar_id, pi_worker_id);
        end if;
      exception
        when no_data_found then
          po_err_num := 6;
          po_err_msg := 'Ошибка при проверке наличия Единого ТП: Тарифный план не найден.';
      end;
    Else
      -- при связывании создаем операцию смены ТП.
      if pi_related = 0 then
        Change_Tariff(pi_tmc_id, pi_org_id, pi_tar_id, pi_worker_id);
      End If;
    end if;
  
    begin
      select td.dog_id
        into l_dog_id
        from mv_org_tree tree
        join t_dogovor td
          on td.org_rel_id = tree.root_rel_id
        join t_dogovor_prm dp
          on td.dog_id = dp.dp_dog_id
         and dp.dp_is_enabled = 1
       where tree.org_id = pi_org_id
         and td.is_enabled = 1
         and dp.dp_prm_id = 2000
         and ((pi_req_delivery_id is null and td.dog_class_id <> 11) or
             (pi_req_delivery_id is not null and td.dog_class_id = 11));
    exception
      when no_data_found then
        l_dog_id := null;
    end;      
  
    -- Корректировка ТП на этапе продажи
    -- Смотрим текущий ТП карты
    if pi_related = 1 /*and pi_abonent.client_type = 'P'*/
     then
      select ts.tar_id, ts.sim_type
        into l_old_tar, l_sim_type
        from t_tmc_sim ts
       where ts.tmc_id = pi_tmc_id;
      if l_old_tar <> pi_tar_id then
        -- Проверяем права - в check
        -- Добавляем операцию корректировки ТП
        -- Заявка на смену ТП
        insert into t_tmc_operations
          (op_type, org_id, user_id, op_date, op_dog_id)
        values
          (19, pi_org_id, pi_worker_id, systimestamp, l_dog_id)
        returning op_id into l_op_tar_id;
        -- Добавляем юнит операции
        insert into t_tmc_operation_units
          (op_id,
           tmc_id,
           tar_id_0,
           tar_id_1,
           callsign_0,
           callsign_1,
           owner_id_0,
           owner_id_1,
           error_id,
           st_sklad_0,
           st_sklad_1,
           imsi_num,
           sim_type)
        values
          (l_op_tar_id,
           pi_tmc_id,
           l_old_tar,
           l_old_tar,
           pi_callsign,
           pi_callsign,
           pi_org_id,
           pi_org_id,
           0,
           11,
           11,
           pi_imsi,
           l_sim_type)
        returning unit_id into l_unit_tar_id;
      
        -- Вставляем заявку на смену ТП
        insert into t_sim_change_tar
          (sim_id,
           tar_id,
           asr_sync_status,
           change_date,
           user_id,
           op_id,
           unit_id)
        values
          (pi_tmc_id,
           pi_tar_id,
           6503, -- Новый статус для корректировки при продаже
           sysdate,
           pi_worker_id,
           l_op_tar_id,
           l_unit_tar_id);
      end if;
    end if;
    
    -- свяжем карту
    if (pi_related = 0) then
      --if pi_is_alien_ab = 0 then
      begin
        select cc.callsign_city, c.color, c.tmc_id, c.callsign_city_id --.city_callsign
          into l_callsign_city, l_color, ll_cs_id, l_callsign_city_id
          from t_callsign c
          left join t_callsign_city cc
            on cc.tmc_id = c.callsign_city_id
         where c.federal_callsign = pi_callsign
           and c.is_related = 0;
      exception
        when no_data_found then
          po_err_num := 7;
          po_err_msg := 'Ошибка при связывании карты. Не найден свободный абонентский номер.';
          logging_pkg.Raise_App_Err(po_err_num, po_err_msg); --120005
      end;
      --end if;
      update t_tmc_sim s
         set s.sim_callsign      = pi_callsign,
             s.sim_callsign_city = l_callsign_city,
             s.sim_color         = l_color,
             s.tar_id            = pi_tar_id,
             s.is_related        = 2,
             s.callsign_id       = ll_cs_id
       where s.tmc_id =
             (select t.tmc_id from t_tmc t where t.tmc_id = pi_tmc_id)
         and s.is_related = 0;
      if (sql%notfound) then
        po_err_num := 8;
        po_err_msg := 'Ошибка при связывании карты. Карта не найдена.';
      end if;
    
      update t_callsign c
         set c.is_related = 2
       where c.federal_callsign = pi_callsign
         and c.is_related = 0;
      /*-- №27073 06.12.2010
      -- ставим карте текущую стоимость  ТП
      l_cost := get_tar_pack_cost(pi_tmc_id, null, sysdate);
      update t_tmc t
         set t.tmc_tmp_cost = l_cost
       where t.tmc_id = pi_tmc_id;*/
    end if;
  
    -- Продажа
    -- добавим операцию
    insert into t_tmc_operations
      (op_type, org_id, user_id, op_date, op_dog_id)
    values
      (22, pi_org_id, pi_worker_id, systimestamp, l_dog_id)
    returning op_id into l_op_id;
  
    -- добавим юнит
    insert into t_tmc_operation_units
      (op_id,
       tmc_id,
       tar_id_0,
       tar_id_1,
       callsign_0,
       callsign_1,
       owner_id_0,
       error_id,
       st_sklad_0,
       st_sklad_1,
       imsi_num,
       sim_type,
       -- 57964
       sim_perm,
       TMC_OP_COST)
    values
      (l_op_id,
       pi_tmc_id,
       (Select Max(ts.tar_id)
          from t_tmc tt
          join t_tmc_sim ts
            on ts.tmc_id = tt.tmc_id
         where tt.tmc_id = pi_tmc_id),
       case when l_old_tar <> pi_tar_id then l_old_tar else pi_tar_id end, -- Корректировка ТП на этапе подключепния
       case when pi_related = 1 then pi_callsign else null end,
       pi_callsign,
       pi_org_id,
       0,
       11,
       12,
       pi_imsi,
       (Select Max(ts.sim_type)
          from t_tmc_sim ts
         where ts.tmc_id = pi_tmc_id),
       -- 57964
       (Select tt.tmc_perm from t_tmc tt where tt.tmc_id = pi_tmc_id),
       ACC_OPERATIONS.Get_Tmc_Cost_By_Tar(pi_tmc_id,
                                          case when l_old_tar <> pi_tar_id then l_old_tar else pi_tar_id end,
                                          8,
                                          systimestamp));
  
    --перенесла связывание симкарты перед продажей
  
    -- alla 01/02/11
    -- удалим ТМЦ федерального номера со склада
    delete from t_org_tmc_status ots
     where ots.tmc_id = ll_cs_id
       and ots.status = 11;
  
    -- удалим ТМЦ городского номера со склада
    if (l_callsign_city_id is not null) then
      delete from t_org_tmc_status ots
       where ots.tmc_id = l_callsign_city_id
         and ots.status = 11;
    end if;
  
    return l_op_id;
  
  end Add_Tmc_Sell_Operation;

  --------------------------------------------------------------------------------------

  procedure Cancel_Tmc_Sell_Operation(pi_op_id     in T_TMC_OPERATIONS.OP_ID%type,
                                      pi_tmc_id    in T_TMC.TMC_ID%type,
                                      pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                                      pi_related   in integer,
                                      pi_worker_id in T_USERS.USR_ID%type) is
    l_tmc_id       T_TMC.TMC_ID%type;
    l_tmc_type     T_TMC.TMC_TYPE%type;
    l_unit_id      T_TMC_OPERATION_UNITS.UNIT_ID%type;
    l_is_tech      T_TARIFF2.IS_TECH%type;
    l_sim_tar_id   T_TMC_SIM.TAR_ID%type;
    l_old_tar_id   T_TMC_SIM.TAR_ID%type;
    l_new_tar_id   T_TMC_SIM.TAR_ID%type;
    l_old_callsign number;
    l_new_callsign number;
    l_dog_id       t_org_tmc_status.dog_id%type;
    l_org_id       number;
    l_owner_id_0   number;
    l_owner_id_1   number;
    l_op_id        number;
    l_imsi_num     t_tmc_operation_units.imsi_num%type;
    l_tmc_op_cost  number;
    l_op_date      date;
    l_op_type      number;
    l_op1902_id    number;
    l_sim_type     number;
    -- 57964
    l_sim_perm   number;
    l_request_id number;
  begin

    begin
      -- Проходим по юнитам
      select --ou.unit_id,
      distinct ou.tmc_id,
               o.org_id,
               ou.tar_id_0,
               ou.tar_id_1,
               ou.callsign_0,
               ou.callsign_1,
               ou.owner_id_0,
               ou.owner_id_1,
               ou.imsi_num,
               ou.tmc_op_cost,
               o.op_date,
               ou.sim_type,
               -- 57964
               ou.sim_perm,
               tor.request_id
        into --l_unit_id,
             l_tmc_id,
             l_org_id,
             l_old_tar_id,
             l_new_tar_id,
             l_old_callsign,
             l_new_callsign,
             l_owner_id_0,
             l_owner_id_1,
             l_imsi_num,
             l_tmc_op_cost,
             l_op_date,
             l_sim_type,
             -- 57964
             l_sim_perm,
             l_request_id
        from t_tmc_operation_units ou
        join t_tmc_operations o
          on ou.op_id = o.op_id
        left join T_TMC_OPERATION_REQUEST tor
          on tor.oper_id = o.op_id
       where ou.op_id = pi_op_id
         and ou.tmc_id = pi_tmc_id;
    exception
      when no_data_found then
        logging_pkg.Raise_App_Err(-1000,
                                  'Невозможно отменить операцию. Юнит не найден.');
      when others then
        logging_pkg.Raise_App_Err(-1000,
                                  'Невозможно отменить операцию. Ошибка в истории операций.');
    end;
    -- Сохраняем всю историю
    /*    -- Удаляем юнит
    delete from t_tmc_operation_units ou where ou.unit_id = l_unit_id;

    -- Удаляем операцию
    delete from t_tmc_operations o where o.op_id = pi_op_id;
    if (sql%notfound) then
      logging_pkg.Raise_App_Err(-1000,
                         'Невозможно отменить операцию. Операция не найдена.');
    end if;*/

    -- e.komissarov 28/10/2009 необходимо указать договор, по которому карта пришла на склад
    select op_dog_id
      into l_dog_id
      from (select top.op_dog_id
              from t_tmc_operations top, t_tmc_operation_units tun
             where top.op_id = tun.op_id
               and tun.tmc_id = pi_tmc_id
             order by top.op_date desc, top.op_id desc, tun.unit_id desc)
     where rownum <= 1;

    -- Вставляем операцию отмены продажи
    insert into t_tmc_operations
      (op_type, org_id, user_id, op_date, op_dog_id)
    values
      (24, l_org_id, pi_worker_id, systimestamp, l_dog_id)
    returning op_id into l_op_id;

    -- Вставляем юнит
    insert into t_tmc_operation_units
      (op_id,
       tmc_id,
       tar_id_0,
       tar_id_1,
       callsign_0,
       callsign_1,
       owner_id_0,
       owner_id_1,
       error_id,
       st_sklad_0,
       st_sklad_1,
       imsi_num,
       tmc_op_cost,
       sim_type,
       -- 57964
       sim_perm)
    values
      (l_op_id,
       l_tmc_id,
       l_new_tar_id,
       l_new_tar_id,
       l_new_callsign,
       l_new_callsign,
       null,
       l_owner_id_0,
       0,
       12,
       11,
       l_imsi_num,
       l_tmc_op_cost,
       l_sim_type,
       -- 57964
       l_sim_perm);

    if l_request_id is not null then
      insert into T_TMC_OPERATION_REQUEST
        (oper_id, request_id, IS_EX_WORKS)
      values
        (l_op_id, l_request_id, 0);
    end if;

    -- Добавляем на склад организации
    begin
      insert into t_org_tmc_status
        (tmc_id, org_id, status, income_date, dog_id)
      values
        (pi_tmc_id, -- 40073

         l_owner_id_0 /*pi_org_id*/,
         11,
         sysdate,
         l_dog_id);
    exception
      when dup_val_on_index then
        logging_pkg.Raise_App_Err(-1000,
                                  'Невозможно отменить операцию. Карта уже имеется на складе организации.');
    end;

    -- находим тип ТМЦ
    begin
      select t.tmc_type
        into l_tmc_type
        from t_tmc t
       where t.tmc_id = l_tmc_id;
    exception
      when no_data_found then
        logging_pkg.Raise_App_Err(-1000,
                                  'Невозможно отменить операцию. ТМЦ не найдена.');
    end;

    -- добавляем привязку sim(ruim) карты к организации
    if (l_tmc_type = 8) then
      /*update t_tmc_sim s
        set s.real_owner_id = pi_org_id
      where s.tmc_id =
            (select t.tmc_id from t_tmc t where t.tmc_id = pi_tmc_id)
        and s.real_owner_id is null;*/
      update t_tmc t
      -- 40073
         set t.org_id = l_owner_id_0
       where t.org_id is null
         and t.tmc_id = pi_tmc_id;
    elsif (l_tmc_type = 9) then
      /*update t_tmc_ruim r
        set r.real_owner_id = pi_org_id
      where r.tmc_id =
            (select t.tmc_id from t_tmc t where t.tmc_id = pi_tmc_id)
        and r.real_owner_id is null;*/
      update t_tmc t
      -- 40073
         set t.org_id = l_owner_id_0
       where t.org_id is null
         and t.tmc_id = pi_tmc_id;
    end if;
    if (sql%notfound) then
      logging_pkg.Raise_App_Err(-1000,
                                'Невозможно отменить операцию. У карты уже имеется привязка к организации.');
    end if;

    -- если перед продажей была смена тарифа
    if (pi_related = 1 and l_tmc_type = 8) then
      -------- возвращаем тариф ----------------------
      -- находим последнюю операцию перед продажей
      select max(o.op_type) keep(dense_rank last order by o.op_date, o.op_type),
             max(o.op_id) keep(dense_rank last order by o.op_date, o.op_type)
        into l_op_type, l_op1902_id
        from t_tmc_operation_units u
        join t_tmc_operations o
          on o.op_id = u.op_id
       where u.tmc_id = l_tmc_id
         and o.op_type <> 22
         and o.op_date <= (select o22.op_date
                             from t_tmc_operations o22
                            where o22.op_id = pi_op_id);
      -- если перед продажей была смена тарифа
      if l_op_type = 1902 then

        select u.tar_id_0, u.tar_id_1
          into l_old_tar_id, l_new_tar_id
          from t_tmc_operation_units u
         where u.tmc_id = l_tmc_id
           and u.op_id = l_op1902_id;

        -- проверим, что замена ТП проводилась именно на текущий ТП
        select s.tar_id
          into l_sim_tar_id
          from t_tmc_sim s, t_tmc t
         where t.tmc_id = l_tmc_id
           and t.tmc_id = s.tmc_id;

        if (nvl(l_sim_tar_id, 0) = nvl(l_new_tar_id, 0)) then

          -- восстанавливаем старый ТП
          insert into t_tmc_operations
            (op_type, org_id, user_id, op_date, op_dog_id)
          values
            (26, l_org_id, pi_worker_id, systimestamp, l_dog_id)
          returning op_id into l_op_id;
          -- Добавляем юнит операции
          insert into t_tmc_operation_units
            (op_id,
             tmc_id,
             tar_id_0,
             tar_id_1,
             callsign_0,
             callsign_1,
             owner_id_0,
             owner_id_1,
             error_id,
             st_sklad_0,
             st_sklad_1,
             imsi_num,
             sim_type,
             -- 57964
             sim_perm)
          values
            (l_op_id,
             l_tmc_id,
             l_new_tar_id, -- возвращаем старый тариф
             l_old_tar_id,
             l_new_callsign,
             l_old_callsign,
             l_owner_id_0,
             l_owner_id_0,
             0,
             11,
             11,
             l_imsi_num,
             l_sim_type,
             -- 57964
             l_sim_perm);
          -- меняем ТП у сим-карты (35215)
          update t_tmc_sim s
             set s.tar_id = l_old_tar_id
           where s.tmc_id = l_tmc_id;
          -- операции по счетам
          acc_operations.Change_acc_tar(l_tmc_id, l_op_id, pi_worker_id);
        end if;
      end if;
      -----------------------------------------------------
    end if;

    if (pi_related = 2 and l_tmc_type = 8) then
      -- освободим номер
      update t_callsign c
         set c.is_related = 0
       where c.federal_callsign =
             (select s.sim_callsign
                from t_tmc_sim s, t_tmc t
               where t.tmc_id = pi_tmc_id
                 and t.tmc_id = s.tmc_id
                 and s.is_related = 2)
         and c.is_related = 2;
      if (sql%notfound) then
        logging_pkg.Raise_App_Err(-1000,
                                  'Невозможно отменить операцию. Связанный номер не найден.');
      end if;
      -- Добавляем на склад организации
      begin
        insert into t_org_tmc_status
          (tmc_id, org_id, status, income_date, dog_id)
        values
          ((select s.callsign_id
             from t_tmc_sim s, t_tmc t
            where t.tmc_id = pi_tmc_id
              and t.tmc_id = s.tmc_id
              and s.is_related = 2),
           l_owner_id_0,
           11,
           sysdate,
           l_dog_id);
      exception
        when others then
          null;
      end;

      -- удаляем привязку карты
      update t_tmc_sim s
         set s.sim_callsign      = null,
             s.tar_id            = null,
             s.sim_callsign_city = null,
             s.sim_color         = tmc_sim.get_color_default(),
             s.is_related        = 0,
             -- e.komissarov 07/07/2009
             s.callsign_id = null
       where s.tmc_id =
             (select t.tmc_id from t_tmc t where t.tmc_id = pi_tmc_id)
         and s.is_related = 2;
      if (sql%notfound) then
        logging_pkg.Raise_App_Err(-1000,
                                  'Невозможно отменить операцию. Связанная карта не найдена.');
      end if;

      -- восстанавливаем базовую стоимость ТМЦ
      update t_tmc t
         set t.tmc_tmp_cost = tmc.c_tmc_sim_cost
       where t.tmc_id = pi_tmc_id;

    end if;

    -- Если отменяется продажа по Единому ТП
    if (l_tmc_type = 8 and pi_related = 0) then
      -- Найдем последнюю операцию смены ТП для данной ТМЦ
      select max(ou.tar_id_0) keep(dense_rank last order by op.op_date) old_tar_id,
             max(ou.tar_id_1) keep(dense_rank last order by op.op_date) new_tar_id
        into l_old_tar_id, l_new_tar_id
        from t_tmc_operations op, t_tmc_operation_units ou
       where ou.tmc_id = pi_tmc_id
         and ou.op_id = op.op_id
         and op.op_type in ( /*19, 26,*/ 1902);

      -- e.komissarov 07/07/2009 если была операция смены ТП
      if l_old_tar_id is not null then
        -- Проверим, является ли данный ТП Единым
        select t.is_tech
          into l_is_tech
          from t_tariff2 t
         where t.id = get_last_tar_id_by_at_id(l_old_tar_id);

        if (l_is_tech = 1) then
          -- проверим, что замена ТП проводилась именно на текущий ТП
          select s.tar_id
            into l_sim_tar_id
            from t_tmc_sim s, t_tmc t
           where t.tmc_id = pi_tmc_id
             and t.tmc_id = s.tmc_id;

          if (l_sim_tar_id = l_new_tar_id) then
            -- Все проверки пройдены,
            -- восстанавливаем старый ТП
            Change_Tar(pi_tmc_id, pi_org_id, l_old_tar_id, pi_worker_id);
          end if;
        end if;
      end if;
    end if;

  end Cancel_Tmc_Sell_Operation;

  --------------------------------------------------------------------------------------

  function Get_Card_Related_Status(pi_tmc_id T_TMC.TMC_ID%type)
    return t_tmc_sim.is_related%type is
    res t_tmc_sim.is_related%type;
  begin
    select s.is_related
      into res
      from t_tmc_sim s
     where s.tmc_id = pi_tmc_id;
    -- e.komissarov 28/09/2010
    return res;
  end Get_Card_Related_Status;

  --------------------------------------------------------------------------------------

  function Get_Abonent_Reg_Date(pi_ab_is_bad in number,
                                pi_ab_id     in T_ABON_BAD.ID%type)
    return date is
    res date := sysdate;
  begin
    -- получим дату регистрации, если подключение редактируется
    if (pi_ab_is_bad = 1) then
      select ab.reg_date
        into res
        from t_abon_bad ab
       where ab.id = pi_ab_id;
    elsif (pi_ab_id is not null and pi_ab_id <> -1) then
      select ab.ab_reg_date
        into res
        from t_abonent ab
       where ab.ab_id = pi_ab_id;
    end if;
    return res;
  exception
    when no_data_found then
      logging_pkg.Raise_App_Err(-1000,
                                'Ошибка при регистрации подключения. Абонент не найден.');
      return res;
  end Get_Abonent_Reg_Date;

  ------------------------------------------------------------------------------------------

  function Add_Abonent_Bad(pi_abonent     in abonent_type,
                           pi_seller_id   in T_USERS.USR_ID%type,
                           pi_worker_id   in T_USERS.USR_ID%type,
                           pi_errors      in varchar2,
                           pi_is_common   in number,
                           pi_is_alien_ab in number,
                           pi_dog_id      in number)
    return T_ABON_BAD.ID%type is
    l_ab_id    T_ABON_BAD.ID%type;
    l_reg_date date;
    l_cc_id    number;
    l_err_num  number;
    l_errors   varchar2(3000);
    l_org_reg_id number;
  begin

    l_reg_date := Get_Abonent_Reg_Date(pi_abonent.is_bad,
                                       pi_abonent.abonent_id);
    select o.region_id
      into l_org_reg_id
      from t_organizations o
     where o.org_id = pi_abonent.org_id;

    l_cc_id    := get_ccid_by_klregionid(pi_abonent.reg_id_ps, l_org_reg_id);

    insert into t_abon_bad
      (dog_date,
       id_org,
       id_cc,
       imsi,
       callsign,
       city_num,
       sim_color,
       id_tar,
       tar_name,
       id_client,
       reg_date,
       mod_date,
       phone_make,
       phone_model,
       phone_imei,
       paid,
       cost,
       err_msg,
       user_id,
       worker_id,
       is_related,
       equipment_required,
       equipment_model_id,
       equipment_cost,
       client_account,
       is_common,
       -- 51312 Бюджетирование
       limit_type,
       limit_warning,
       limit_block,
       limit_date_start,
       limit_date_end,
       seller_active_id,
       root_org_id,
       ATTACHED_DOC,
       NUMBER_SHEETS,
       TRANSFER_PHONE,
       TRANSFER_DATE,
       OPERATION_DONOR,
       AGREE_PAY_OFF_DEBT,
       AGREE_WORK_INFO,
       AGREE_CANCEL_DOG,
       TYPE_REQ,
       STATE_SBDPN,
       operator_recipient,
       npid,
       TYPE_PROCESS_CBDPN,
       dog_id,
       -- 69988
       channel_id,
       CODE_WORD,
       OUT_ACCOUNT,
       CREDIT_LIMIT,
       ALARM_LIMIT,
       reg_id_ps,
       kl_region_id,
       usl_number,
       FIX_ADDRESS_ID,
       FIX_NAME,
       ab_comment,
       req_delivery_id,
       request_id,
       consent_msg,
       method_connect)
    values
      (pi_abonent.dog_date,
       pi_abonent.org_id,
       --pi_abonent.cc_id,
       l_cc_id,
       pi_abonent.imsi,
       pi_abonent.callsign,
       pi_abonent.citynum,
       pi_abonent.simcolor,
       pi_abonent.tar_id,
       pi_abonent.tar_name,
       pi_abonent.client_id,
       l_reg_date,
       sysdate,
       pi_abonent.phone_make,
       pi_abonent.phone_model,
       pi_abonent.phone_imei,
       pi_abonent.paid,
       pi_abonent.cost,
       pi_abonent.err_msg || pi_errors,
       pi_seller_id,
       pi_worker_id,
       pi_abonent.related,
       pi_abonent.equipment_required,
       pi_abonent.equipment_model_id,
       pi_abonent.equipment_cost,
       pi_abonent.client_account,
       pi_is_common,
       -- 51312 Бюджетирование
       pi_abonent.limit_type,
       pi_abonent.limit_warning,
       pi_abonent.limit_block,
       pi_abonent.limit_date_start,
       pi_abonent.limit_date_end,
       --51299 - активные продавцы
       pi_abonent.seller_active_id,
       Orgs.Get_Root_Org_Or_Self(pi_abonent.org_id),
       pi_abonent.ATTACHED_DOC,
       pi_abonent.NUMBER_SHEETS,
       pi_abonent.TRANSFER_PHONE,
       pi_abonent.wish_date,
       pi_abonent.OPERATION_DONOR,
       pi_abonent.AGREE_PAY_OFF_DEBT,
       pi_abonent.AGREE_WORK_INFO,
       pi_abonent.AGREE_CANCEL_DOG,
       nvl(pi_is_alien_ab, 0),
       decode(pi_is_alien_ab, 1, 0, null),
       pi_abonent.operator_recipient,
       pi_abonent.npid,
       pi_abonent.TYPE_PROCESS_CBDPN,
       pi_dog_id,
       -- 69988
       pi_abonent.channel_id,
       pi_abonent.CODE_WORD,
       pi_abonent.OUT_ACCOUNT,
       pi_abonent.CREDIT_LIMIT,
       pi_abonent.ALARM_LIMIT,
       pi_abonent.reg_id_ps,
       pi_abonent.KL_REGION_ID,
       pi_abonent.usl_number,
       pi_abonent.fix_address_id,
       pi_abonent.FIX_NAME,
       pi_abonent.ab_comment,
       pi_abonent.req_delivery_id,
       pi_abonent.request_id,
       pi_abonent.consent_msg,
       pi_abonent.method_connect)
    returning id into l_ab_id;
    -- 51312 Бюджетирование
    if pi_abonent.limit_type is not null then
      insert into t_abon_bad_budget abb
        (BAD_ID,
         CLIENT_ID,
         LIMIT_TYPE,
         LIMIT_WARNING,
         LIMIT_BLOCK,
         LIMIT_DATE_START,
         LIMIT_DATE_END,
         IS_PERSONAL_ACCOUNT)
      values
        (l_ab_id,
         pi_abonent.limit_client_id,
         pi_abonent.limit_type,
         pi_abonent.limit_warning,
         pi_abonent.limit_block,
         pi_abonent.limit_date_start,
         pi_abonent.limit_date_end,
         pi_abonent.IS_PERSONAL_ACCOUNT);
    end if;
    return l_ab_id;
  end Add_Abonent_Bad;

  --------------------------------------------------------------------------------------------

  procedure Lock_Tmc(pi_tmc_id in T_TMC.TMC_ID%type) is
    dummy number;
  begin
    select tmc_id
      into dummy
      from t_tmc
     where tmc_id = pi_tmc_id
       for update nowait;
  Exception
    when NO_DATA_FOUND then
      null;
    When Others then
      raise;
  end Lock_Tmc;

  ------------------------------------------------------------------------------------------
  function Save_Abonent3(pi_abonent   in abonent_type,
                         pi_services  in service_tab,
                         pi_savetype  in number,
                         pi_worker_id in T_USERS.USR_ID%type,
                         pi_cost      in number,
                         pi_is_common in number,
                         -- 35948 (35953) Брендированное оборудование (телефоны)
                         pi_tmc_phone   in tmc_phone_tab,
                         pi_is_alien_ab in number, --признак,что абонент не усишный
                         po_is_ok       out number, -- 0- t_abon_bad, 1- t_abonent без ошибок, 2- t_abonent c ошибками
                         po_err_num     out pls_integer,
                         po_err_msg     out t_Err_Msg)
    return T_ABONENT.AB_ID%type is
    l_is_card_ok   integer := 1;
    l_reg_date     date;
    l_ab_type      number;
    l_errors       varchar2(4000);
    l_err_num      number;
    l_err_msg      varchar2(2000);
    l_status       number;
    l_old_status   number;
    l_new_op_id    number;
    l_old_op_id    number;
    l_new_tmc_id   T_TMC.TMC_ID%type;
    l_old_tmc_id   T_TMC.TMC_ID%type;
    l_ab_id        T_ABONENT.AB_ID%type := pi_abonent.abonent_id;
    l_old_ab_id    T_ABONENT.AB_ID%type;
    l_is_edit      integer := 0;
    l_status_error varchar2(2000);
    l_status_code  number := 0;
    l_related      number;
    l_new_related  number;
  
    l_new_tar_id number;
    l_old_tar_id number;
    --
    l_check_city number;
    l_check_rel  number;
    l_city_id    number;
    l_fed_num    number;
  
    l_equipment_tmc_id     number := null; -- ид usb-модема
    l_old_equipment_tmc_id number := null; -- ид usb-модема старого
  
    l_str  varchar2(2000);
    l_tmp  number;
    l_espp number;
    ex_resource_busy exception;
    PRAGMA EXCEPTION_INIT(ex_resource_busy, -54);
  
    -- 35948 (35953) Брендированное оборудование (телефоны)
    l_old_equipment_tmc_id_phone number;
    l_equipment_tmc_id_phone     number;
    -- 38602
    l_model_id number;
    l_ser_num  varchar2(25);
  
    -- 40261 Брендированное оборудование
    l_type t_clients.client_type%type;
  
    l_budget_count number;
    -- 57180
    l_equipment_required number;
    l_eq_model           number;
    dummy                number;
    ex_no_eq exception;
    l_serial_number  varchar2(25);
    l_old_eq_id      number;
    l_eq_tmc_id      number;
    l_org_id         number;
    l_dog_id         number;
    l_op_id          number;
    l_cnt_before     number;
    l_cost           number;
    l_ab_reg_date    date;
    l_equipment_cost number;
    l_equipment_id   number; --нужен для передачи в АСР
    ex_no_unique exception;
    --  Задача № 74453 - Регистрация GSM/UMTS подключения
    l_user_is_rtk   boolean;
    l_user_is_rtm   boolean;
    l_org_structure number;
    l_cc_id         number;
    l_org_reg_id    number;
    ex_no_req exception;
    l_req_count     number;
    l_deliv_dog_cnt number;
    ex_deliv_dog exception;    
    l_old_tar     number;
    l_op_tar_id   number;
    l_unit_tar_id number;
  begin
    if pi_abonent is not null then
      l_str := 'ab_id: ' || pi_abonent.abonent_id || ', dog_date: ' ||
               pi_abonent.dog_date || ', org_id: ' || pi_abonent.org_id ||
               ', imsi: ' || pi_abonent.imsi || ', cs: ' ||
               pi_abonent.callsign || ' related: ' || pi_abonent.related ||
               ', tar_id: ' || pi_abonent.tar_id || ', is_bad: ' ||
               pi_abonent.is_bad || ', w_id: ' || pi_worker_id ||
               ', limit_type: ' || pi_abonent.limit_type ||
               ', pi_is_alien_ab: ' || pi_is_alien_ab ||
               ', limit_warning: ' || pi_abonent.limit_warning ||
               ', limit_block: ' || pi_abonent.limit_block ||
               ', limit_date_start: ' || pi_abonent.limit_date_start ||
               ', limit_date_end: ' || pi_abonent.limit_date_end ||
               ', out_account: ' || pi_abonent.out_account ||
               ', CREDIT_LIMIT: ' || pi_abonent.CREDIT_LIMIT ||
               ', ALARM_LIMIT: ' || pi_abonent.ALARM_LIMIT ||
               ', reg_id_ps: ' || pi_abonent.reg_id_ps ||
               ', KL_REGION_ID: ' || pi_abonent.KL_REGION_ID ||
               ', IS_PERSONAL_ACCOUNT: ' || pi_abonent.IS_PERSONAL_ACCOUNT ||
               ', pi_abonent.request_id: ' || pi_abonent.request_id ||
               ', pi_abonent.REQ_DELIVERY_ID: ' ||
               pi_abonent.REQ_DELIVERY_ID;
      logging_pkg.info(pi_message  => l_str,
                       pi_location => c_package || 'Save_Abonent3');
    else
      logging_pkg.error(pi_message  => 'pi_abonent is null',
                        pi_location => c_package || 'Save_Abonent3');
    end if;
  
    /*
        select nvl(d.is_pay_espp,1)
          into l_with_ab_pay
        from mv_org_tree r
           left join t_dogovor d
               on d.org_rel_id=r.root_rel_id
               and d.is_enabled=1
               where r.org_id=pi_abonent.org_id;
    
    */
  
    savepoint save_abonent;
  
    if (not Security_pkg.Check_Rights_str('EISSD.CONNECTIONS.GSM.REGISTER',
                                          pi_abonent.org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      -- прав на создание подключения нет
      po_err_msg := 'У пользователя в выбранной организации нет прав на продажу GSM';
      return - 1;
    end if;
  
    -- Проверка права пользователя на подключение MVNO в закрытом регионе
    if is_reg_mvno(pi_abonent.reg_id_ps) = 0 and
       (not security_pkg.Check_User_Right_str('EISSD.CONNECTIONS.GSM.REGISTER_CLOSE_REG',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      -- прав на создание подключения нет
      po_err_msg := 'У пользователя нет прав на продажу GSM в данном регионе';
      return - 1;
    end if;
  
    security_pkg.get_user_structures(pi_worker_id => pi_worker_id,
                                     po_is_rtk    => l_user_is_rtk,
                                     po_is_rtm    => l_user_is_rtm,
                                     po_err_num   => po_err_num,
                                     po_err_msg   => po_err_msg);
  
    if l_user_is_rtk and not l_user_is_rtm then
      if orgs.is_org_have_prm(pi_org_id         => pi_abonent.org_id,
                              pi_prm_tab        => num_tab(2000),
                              pi_is_org_enabled => 0,
                              pi_is_dog_enabled => 0,
                              pi_structure      => 0,
                              po_err_num        => po_err_num,
                              po_err_msg        => po_err_msg) = 0 then
        -- прав на создание подключения нет
        po_err_num := 1;
        po_err_msg := 'Регистрация GSM/UMTS подключений от лица агентов, у которых нет договора на продажу GSM/UMTS с подразделением РТК, запрещена';
        return - 1;
      end if;
    end if;
  
    if (pi_abonent.abonent_id is not null and pi_abonent.abonent_id != -1) then
      l_is_edit := 1;
    end if;
  
    po_is_ok := 0;
    --if pi_is_alien_ab <> 1 then
    l_is_card_ok := check_abonent(pi_abonent,
                                  pi_worker_id,
                                  -- 35948 (35953) Брендированное оборудование (телефоны)
                                  pi_tmc_phone,
                                  pi_is_alien_ab,
                                  l_errors,
                                  l_err_num,
                                  l_err_msg);
    --end if;
    if (l_err_num != 0) then
      --    if (l_is_card_ok<>1) then
      logging_pkg.info(pi_message  => 'after check. res: ' ||
                                      nvl(l_err_msg, 'null') || '. ' ||
                                      l_str,
                       pi_location => c_package || 'Save_Abonent3');
      logging_pkg.Raise_App_Err(l_err_num, l_err_msg);
    end if;
  
    if pi_abonent.reg_id_ps is not null then
      select o.region_id
        into l_org_reg_id
        from t_organizations o
       where o.org_id = pi_abonent.org_id;
      l_cc_id := get_ccid_by_klregionid(pi_abonent.reg_id_ps, l_org_reg_id);
    end if;
  
    -- Patrick 22485: Не обращать внимания на признак связанности карты (is_related)
    -- который приходит с веб, а получать его внутри функции.
    l_new_related := pi_abonent.related;
  
    /*    select s.is_related
          into l_new_related
          from t_tmc_sim s
         where s.sim_imsi = pi_abonent.imsi
            -- e.komissarov 20100819
            or s.sim_iccid = pi_abonent.imsi;
    */
    if (l_is_edit = 1) then
      -- пытаемся получить монопольный доступ к абоненту
      if (pi_abonent.is_bad = 1) then
        select id
          into l_old_ab_id
          from t_abon_bad
         where id = pi_abonent.abonent_id
           for update nowait;
      else
        select ab_id
          into l_old_ab_id
          from t_abonent
         where ab_id = pi_abonent.abonent_id
           for update nowait;
      end if;
    end if;
  
    if (l_is_card_ok = 1) then
      -- параметры sim(ruim)-карты корректны
      -- найдем тип абонента по идентификатору ТП
      begin
        select t.type_vdvd_id
          into l_ab_type
          from t_tariff2 t
         where t.id = get_last_tar_id_by_at_id(pi_abonent.tar_id);
      exception
        when no_data_found then
          logging_pkg.Raise_App_Err(-1000,
                                    'Ошибка при регистрации подключения. Тарифный план не найден.');
      end;
    
      -- создание нового подключения или преобразование ошибочного подключения в нормальное
      if (pi_abonent.abonent_id = -1 or pi_abonent.is_bad = 1) then
      
        -- получим идентификатор ТМЦ
        l_new_tmc_id := Get_Tmc_By_Imsi(pi_abonent.imsi, l_ab_type);
      
        -- Для обычного подключения - не из доставки:
        /*SIM-карты и абонентские номера, которые перемещены на организацию с классом договора
        «Доставка оборудования», не должны быть доступны для выбора на странице создания нового
        подключения*/
        if pi_abonent.REQ_DELIVERY_ID is null then
          -- Смотрим класс договора, по которому ТМЦ пришла на склад
          select count(*)
            into l_deliv_dog_cnt
            from t_org_tmc_status ots
            join t_dogovor td
              on td.dog_id = ots.dog_id
           where td.dog_class_id = 11
             and ots.tmc_id = l_new_tmc_id;
          if l_deliv_dog_cnt > 0 then
            raise ex_deliv_dog;
          end if;
        end if;
        -- Проверяем не удалено ли уже ТМЦ данного подключения из заявки
        if pi_abonent.req_delivery_id is not null then
          select count(*)
            into l_req_count
            from t_req_deliv_equip t
           where t.req_id = pi_abonent.req_delivery_id
             and t.tmc_id = l_new_tmc_id;
          if l_req_count = 0 then
            raise ex_no_req;
          end if;
        end if;
      
        if (l_new_tmc_id is null) then
          logging_pkg.Raise_App_Err(-1000,
                                    'Ошибка при регистрации подключения. ТМЦ не найдена.');
        end if;
        /*l_dog_id := ACC_OPERATIONS.getDogByTmcId(l_new_tmc_id,
        pi_abonent.org_id);*/ -- или Orgs.Get_Root_Org_Or_Self(pi_abonent.org_id)
        begin
          select td.dog_id
            into l_dog_id
            from mv_org_tree tree
            join t_dogovor td
              on td.org_rel_id = tree.root_rel_id
            join t_dogovor_prm dp
              on td.dog_id = dp.dp_dog_id
             and dp.dp_is_enabled = 1
           where tree.org_id = pi_abonent.org_id
             and td.is_enabled = 1
             and dp.dp_prm_id = 2000
             and ((pi_abonent.REQ_DELIVERY_ID is null and
                 td.dog_class_id <> 11) or (pi_abonent.REQ_DELIVERY_ID is not null and
                 td.dog_class_id = 11));
        exception
          when no_data_found then
            l_dog_id := null;
        end;
        -- блокируем ТМЦ
        Lock_Tmc(l_new_tmc_id);
              
        /*-- Корректировка ТП на этапе продажи
        -- Смотрим текущий ТП карты
        if pi_abonent.related = 1 and pi_abonent.client_type = 'P' then
          select ts.tar_id
            into l_old_tar
            from t_tmc_sim ts
           where ts.tmc_id = l_new_tmc_id;
          if l_old_tar <> pi_abonent.tar_id then
            -- Проверяем права - в check
            -- Добавляем операцию корректировки ТП
            -- Надо ли проверки как при смене ТП ???
            insert into t_tmc_operations
              (op_type, org_id, user_id, op_date, op_dog_id)
            values
              (26, pi_abonent.org_id, pi_worker_id, systimestamp, l_dog_id)
            returning op_id into l_op_tar_id;
            -- Добавляем юнит операции
            insert into t_tmc_operation_units
              (op_id,
               tmc_id,
               tar_id_0,
               tar_id_1,
               callsign_0,
               callsign_1,
               owner_id_0,
               owner_id_1,
               error_id,
               st_sklad_0,
               st_sklad_1,
               imsi_num)
            values
              (l_op_tar_id,
               l_new_tmc_id,
               l_old_tar,
               pi_abonent.tar_id,
               pi_abonent.callsign,
               pi_abonent.callsign,
               pi_abonent.org_id,
               pi_abonent.org_id,
               0,
               11,
               11,
               pi_abonent.imsi) returning unit_id into l_unit_tar_id;
          
            -- меняем ТП у сим-карты - после ответа АСР ??? или сейчас?
            update t_tmc_sim s
               set s.tar_id = pi_abonent.tar_id
             where s.tmc_id = l_new_tmc_id;
          
            -- Вставляем заявку на смену ТП
            insert into t_sim_change_tar
              (sim_id,
               tar_id,
               asr_sync_status,
               change_date,
               user_id,
               op_id,
               unit_id)
            values
              (l_new_tmc_id,
               pi_abonent.tar_id,
               6503, -- Новый статус для корректировки при продаже
               sysdate,
               pi_worker_id,
               l_op_tar_id,
               -- 45265
               l_unit_tar_id);
          end if;
        end if;*/
      
        -- добавляем операцию продажи
        l_new_op_id := Add_Tmc_Sell_Operation(pi_abonent.org_id,
                                              l_new_tmc_id,
                                              pi_abonent.callsign,
                                              pi_abonent.imsi,
                                              pi_abonent.tar_id,
                                              l_ab_type,
                                              l_new_related,
                                              pi_is_alien_ab,
                                              pi_abonent.REQ_DELIVERY_ID,
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg);
        if pi_abonent.REQ_DELIVERY_ID is not null then
          insert into T_TMC_OPERATION_REQUEST
            (oper_id, request_id, IS_EX_WORKS)
          values
            (l_new_op_id, pi_abonent.REQ_DELIVERY_ID, 0);
        end if;
      
        -- определим статус подключения
        if nvl(pi_is_alien_ab, 0) = 0 then
          l_status := Get_Real_Abon_Status(pi_abonent,
                                           l_new_tmc_id,
                                           pi_worker_id,
                                           pi_savetype,
                                           l_is_edit,
                                           pi_abonent.REQ_DELIVERY_ID,
                                           l_status_code,
                                           l_status_error);
        else
          l_status := pi_abonent.status;
        end if;
      
        l_reg_date := Get_Abonent_Reg_Date(pi_abonent.is_bad,
                                           pi_abonent.abonent_id);
      
        -- Если продажа с модемом
        -- 35948 (35953) сделан case в связи с появлением телефонов
        case
          when pi_abonent.equipment_required = 4 then
            --получаем ИД ТМЦ модема
            if pi_abonent.equipment_tmc_id is null then
              l_equipment_tmc_id := usb_modems.get_first_modem_by_model(pi_abonent.equipment_model_id,
                                                                        pi_abonent.org_id,
                                                                        pi_worker_id);
            else
              l_equipment_tmc_id := pi_abonent.equipment_tmc_id;
            end if;
            begin
              select d.equipment_id
                into l_equipment_id
                from t_tmc_modem_usb u
                join t_dic_equipment d
                  on d.model_id = u.usb_model
                join t_organizations org
                  on org.org_id = pi_abonent.org_id
                join t_dic_region r
                  on r.reg_id = org.region_id
                 and d.equipment_type = 4
                 and r.mrf_id = d.mrf_if
               where u.tmc_id = l_equipment_tmc_id;
            exception
              when others then
                null;
            end;
            --Блокируем ТМЦ модема
            Lock_Tmc(l_equipment_tmc_id);
          
            --Добавляюем операцию продажи на уже существующую по СИМ-карте
            usb_modems.sell_usb_modem(pi_abonent.org_id,
                                      pi_worker_id,
                                      l_equipment_tmc_id,
                                      l_new_op_id,
                                      pi_abonent.usb_ser,
                                      po_err_num,
                                      po_err_msg);
          
          when pi_abonent.equipment_required = 7003 then
            if pi_tmc_phone(1).tmc_id is null then
              -- Получаем ИД ТМЦ телефона
              l_equipment_tmc_id_phone := tmc_phones.get_first_phone_by_model(pi_tmc_phone(1)
                                                                              .model_id,
                                                                              pi_abonent.org_id,
                                                                              pi_worker_id);
            else
              l_equipment_tmc_id_phone := pi_tmc_phone(1).tmc_id;
            end if;
            begin
              select d.equipment_id
                into l_equipment_id
                from t_tmc_phone p
                join t_dic_equipment d
                  on d.model_id = p.model_id
                 and d.equipment_type = 7003
                join t_organizations org
                  on org.org_id = pi_abonent.org_id
                join t_dic_region r
                  on r.reg_id = org.region_id
                 and r.mrf_id = d.mrf_if
               where p.tmc_id = l_equipment_tmc_id_phone;
            exception
              when others then
                null;
            end;
            --Блокируем ТМЦ телефона
            Lock_Tmc(l_equipment_tmc_id_phone);
          
            --Добавляем операцию продажи на уже существующую по СИМ-карте
            tmc_phones.sell_phone(pi_abonent.org_id,
                                  pi_worker_id,
                                  l_equipment_tmc_id_phone,
                                  l_new_op_id,
                                  pi_tmc_phone,
                                  po_err_num,
                                  po_err_msg);
          else
            null;
        end case;
        --24.09.2010 зад.22932
        begin
          select o.IS_PAY_ESPP
            into l_espp
            from t_organizations o
           where o.org_id = pi_abonent.org_id;
        exception
          when others then
            null;
        end;
        -- добавляем запись в T_ABONENT
        insert into t_abonent
          (client_id,
           ab_reg_date,
           ab_mod_date,
           phone_make,
           phone_model,
           phone_imei,
           ab_status,
           ab_paid,
           ab_cost,
           ab_tmc_id,
           err_code,
           err_msg,
           org_id,
           cc_id,
           ab_dog_date,
           id_op,
           root_org_id,
           user_id,
           ab_note,
           worker_id,
           equipment_required,
           equipment_tmc_id,
           equipment_cost,
           with_ab_pay,
           is_common,
           CLIENT_ACCOUNT,
           -- 51312 Бюджетирование
           LIMIT_TYPE,
           LIMIT_WARNING,
           LIMIT_BLOCK,
           LIMIT_DATE_START,
           LIMIT_DATE_END,
           SELLER_ACTIVE_ID,
           ATTACHED_DOC,
           NUMBER_SHEETS,
           equipment_id,
           TYPE_REQUEST,
           TRANSFER_PHONE,
           TRANSFER_DATE,
           OPERATION_DONOR,
           AGREE_PAY_OFF_DEBT,
           AGREE_WORK_INFO,
           AGREE_CANCEL_DOG,
           STATE_SBDPN,
           operator_recipient,
           dog_id,
           -- 69988
           channel_id,
           CODE_WORD,
           OUT_ACCOUNT,
           CREDIT_LIMIT,
           ALARM_LIMIT,
           reg_id_ps,
           kl_region_id,
           usl_number,
           FIX_ADDRESS_ID,
           FIX_NAME,
           ab_comment,
           req_delivery_id,
           consent_msg,
           method_connect)
        values
          (pi_abonent.client_id,
           l_reg_date,
           sysdate,
           pi_abonent.phone_make,
           pi_abonent.phone_model,
           pi_abonent.phone_imei,
           l_status,
           /* case when nvl(pi_is_alien_ab, 0) = 0 then l_status else
           c_ab_prepare end,*/
           pi_abonent.paid,
           pi_abonent.cost,
           l_new_tmc_id,
           decode(pi_abonent.err_code,
                  0,
                  l_status_code,
                  pi_abonent.err_code),
           pi_abonent.err_msg || l_status_error,
           pi_abonent.org_id,
           --pi_abonent.cc_id,
           l_cc_id,
           pi_abonent.dog_date,
           l_new_op_id,
           Orgs.Get_Root_Org_Or_Self(pi_abonent.org_id),
           pi_abonent.user_id,
           pi_abonent.note,
           pi_worker_id,
           pi_abonent.equipment_required,
           -- 35948 (35953) Брендированное оборудование (телефоны)
           nvl(l_equipment_tmc_id, l_equipment_tmc_id_phone),
           pi_abonent.equipment_cost,
           l_espp,
           nvl(pi_is_common, 0),
           pi_abonent.CLIENT_ACCOUNT,
           -- 51312 Бюджетирование
           pi_abonent.LIMIT_TYPE,
           pi_abonent.LIMIT_WARNING,
           pi_abonent.LIMIT_BLOCK,
           pi_abonent.LIMIT_DATE_START,
           pi_abonent.LIMIT_DATE_END,
           pi_abonent.seller_active_id,
           pi_abonent.ATTACHED_DOC,
           pi_abonent.NUMBER_SHEETS,
           l_equipment_id,
           nvl(pi_is_alien_ab, 0),
           pi_abonent.TRANSFER_PHONE,
           pi_abonent.wish_date,
           pi_abonent.OPERATION_DONOR,
           pi_abonent.AGREE_PAY_OFF_DEBT,
           pi_abonent.AGREE_WORK_INFO,
           pi_abonent.AGREE_CANCEL_DOG,
           decode(pi_is_alien_ab,
                  1,
                  decode(l_status, 97, 5, 96, 6, 0),
                  null),
           pi_abonent.operator_recipient,
           decode(is_org_usi(pi_abonent.org_id), 1, null, l_dog_id),
           -- 69988
           pi_abonent.channel_id,
           pi_abonent.CODE_WORD,
           pi_abonent.OUT_ACCOUNT,
           pi_abonent.CREDIT_LIMIT,
           pi_abonent.ALARM_LIMIT,
           pi_abonent.reg_id_ps,
           pi_abonent.KL_REGION_ID,
           pi_abonent.usl_number,
           pi_abonent.fix_address_id,
           pi_abonent.FIX_NAME,
           pi_abonent.AB_COMMENT,
           pi_abonent.req_delivery_id,
           pi_abonent.consent_msg,
           pi_abonent.method_connect)
        returning AB_ID into l_ab_id;
      
        if pi_abonent.request_id is not null then
          insert into T_ABONENT_TO_REQUEST
            (ab_id, request_id, is_send)
          values
            (l_ab_id,
             pi_abonent.request_id,
             case
               when pi_abonent.OUT_ACCOUNT is not null then
                3
               else
                0
             end);
        end if;
        --
        /*if pi_is_alien_ab = 1 then
          insert into t_abonent_outside
            (ab_id,
             wish_date,
             agree_work_info,
             agree_cancel_dog,
             agree_pay_off_debt,
             status,
             TRANSFER_PHONE,
             OPERATION_DONOR,
             NPID,
             TYPE_PROCESS_CBDPN)
          values
            (l_ab_id,
             pi_abonent.wish_date,
             pi_abonent.agree_work_info,
             pi_abonent.agree_cancel_dog,
             pi_abonent.agree_pay_off_debt,
             0,
             pi_abonent.TRANSFER_PHONE,
             pi_abonent.OPERATION_DONOR,
             pi_abonent.NPID,
             pi_abonent.TYPE_PROCESS_CBDPN);
          insert into t_abonent_out_status
            (ab_id, status,WORKER_ID)
          values
            (l_ab_id, 0,pi_worker_id);
        end if;*/
        -- 51312 Бюджетирование
        if pi_abonent.limit_type is not null then
          insert into t_abonent_budget
            (AB_ID,
             CLIENT_ID,
             LIMIT_TYPE,
             LIMIT_WARNING,
             LIMIT_BLOCK,
             LIMIT_DATE_START,
             LIMIT_DATE_END,
             IS_PERSONAL_ACCOUNT)
          values
            (l_ab_id,
             pi_abonent.limit_client_id,
             pi_abonent.limit_type,
             pi_abonent.limit_warning,
             pi_abonent.limit_block,
             pi_abonent.limit_date_start,
             pi_abonent.limit_date_end,
             pi_abonent.IS_PERSONAL_ACCOUNT);
        end if;
      
        logging_pkg.info('ab_id=' || l_ab_id || ', equipment_required=' ||
                         pi_abonent.equipment_required ||
                         ', equipment_tmc_id=' || l_equipment_tmc_id ||
                         ', l_equipment_tmc_id_phone=' ||
                         l_equipment_tmc_id_phone || ', equipment_cost=' ||
                         pi_abonent.equipment_cost || ', org_id=' ||
                         pi_abonent.org_id,
                         c_package || 'Save_Abonent3');
      
        -- сохраняем связь с подключенными услугами
        for i in 1 .. pi_services.count loop
          if (pi_services(i) is not null and pi_services(i)
             .serv_id is not null) then
            insert into t_serv_abonent
              (ab_id, serv_id)
            values
              (l_ab_id, pi_services(i).serv_id);
          end if;
        end loop;
      
        if (pi_abonent.is_bad = 1) then
          -- удаляем ошибочное подключение из T_ABON_BAD
          update t_abon_bad ab
             set ab.is_deleted = 1, ab.mod_date = sysdate
           where ab.id = pi_abonent.abonent_id;
        end if;
      
        /*        logging_pkg.debug('new. err_code=' || pi_abonent.err_code ||
        ', status_error=' || l_status_error,
        c_package || 'Save_Abonent3');*/
      
        --  Определяем после блока if-else (вставка/редактирование записи)
        /*        if ((pi_abonent.err_code = 0 or pi_abonent.err_code is null) and
           l_status_error is null) then
          po_err_msg := '';
        
          po_is_ok := 1;
        end if;*/
        -- 35948 (35953) Вставляем связь абонента, телефона и услуги
        if pi_abonent.equipment_required = 7003 then
          insert into t_abonent_phone
            (ab_id, tmc_id, bonus_id)
            Select l_ab_id, l_equipment_tmc_id_phone, t.bonus_id
              from table(pi_tmc_phone) t;
        end if;
      else
        -- редактирование подключения
        -- получим новый идентификатор ТМЦ
        l_new_tmc_id := Get_Tmc_By_Imsi(pi_abonent.imsi, l_ab_type);
        -- Для обычного подключения - не из доставки:
        /*SIM-карты и абонентские номера, которые перемещены на организацию с классом договора
        «Доставка оборудования», не должны быть доступны для выбора на странице создания нового
        подключения*/
        if pi_abonent.REQ_DELIVERY_ID is null then
          -- Смотрим класс договора, по которому ТМЦ пришла на склад
          select count(*)
            into l_deliv_dog_cnt
            from t_org_tmc_status ots
            join t_dogovor td
              on td.dog_id = ots.dog_id
           where td.dog_class_id = 11
             and ots.tmc_id = l_new_tmc_id;
          if l_deliv_dog_cnt > 0 then
            raise ex_deliv_dog;
          end if;
        end if;
        begin
          select td.dog_id
            into l_dog_id
            from mv_org_tree tree
            join t_dogovor td
              on td.org_rel_id = tree.root_rel_id
            join t_dogovor_prm dp
              on td.dog_id = dp.dp_dog_id
             and dp.dp_is_enabled = 1
           where tree.org_id = pi_abonent.org_id
             and td.is_enabled = 1
             and dp.dp_prm_id = 2000
             and ((pi_abonent.REQ_DELIVERY_ID is null and
                 td.dog_class_id <> 11) or (pi_abonent.REQ_DELIVERY_ID is not null and
                 td.dog_class_id = 11));
        exception
          when no_data_found then
            l_dog_id := null;
        end;
      
        -- 35948 (35953) сделан case в связи с появлением телефонов
        case pi_abonent.equipment_required
          when 4 then
            --получаем ИД ТМЦ модема
            if pi_abonent.equipment_tmc_id is null then
              l_equipment_tmc_id := usb_modems.get_first_modem_by_model(pi_abonent.equipment_model_id,
                                                                        pi_abonent.org_id,
                                                                        pi_worker_id);
            else
              l_equipment_tmc_id := pi_abonent.equipment_tmc_id;
            end if;
            begin
              select d.equipment_id
                into l_equipment_id
                from t_tmc_modem_usb u
                join t_dic_equipment d
                  on d.model_id = u.usb_model
                 and d.equipment_type = 4
                join t_organizations org
                  on org.org_id = pi_abonent.org_id
                join t_dic_region r
                  on r.reg_id = org.region_id
                 and r.mrf_id = d.mrf_if
               where u.tmc_id = l_equipment_tmc_id;
            exception
              when others then
                null;
            end;
          when 7003 then
            if pi_tmc_phone(1).tmc_id is null then
              -- Получаем ИД ТМЦ телефона
              l_equipment_tmc_id_phone := tmc_phones.get_first_phone_by_model(pi_tmc_phone(1)
                                                                              .model_id,
                                                                              pi_abonent.org_id,
                                                                              pi_worker_id);
            else
              l_equipment_tmc_id_phone := pi_tmc_phone(1).tmc_id;
            end if;
            begin
              select d.equipment_id
                into l_equipment_id
                from t_tmc_phone p
                join t_dic_equipment d
                  on d.model_id = p.model_id
                 and d.equipment_type = 7003
                join t_organizations org
                  on org.org_id = pi_abonent.org_id
                join t_dic_region r
                  on r.reg_id = org.region_id
                 and r.mrf_id = d.mrf_if
               where p.tmc_id = l_equipment_tmc_id_phone;
            exception
              when others then
                null;
            end;
          else
            null;
        end case;
        -- блокируем новую ТМЦ
        Lock_Tmc(l_new_tmc_id);
        if (nvl(l_equipment_tmc_id, 0) <> 0) then
          Lock_tmc(l_equipment_tmc_id);
        end if;
        if (nvl(l_equipment_tmc_id_phone, 0) <> 0) then
          Lock_tmc(l_equipment_tmc_id_phone);
        end if;
      
        -- получим старый идентификатор ТМЦ и идентификатор операции продажи
        select a.ab_tmc_id,
               a.id_op,
               a.equipment_tmc_id,
               -- 35948 (35953)
               ap.tmc_id
          into l_old_tmc_id,
               l_old_op_id,
               l_old_equipment_tmc_id,
               -- 35948 (35953)
               l_old_equipment_tmc_id_phone
          from t_abonent a
        -- 35948 (35953)
          left join t_abonent_phone ap
            on a.ab_id = ap.ab_id
         where a.ab_id = pi_abonent.abonent_id;
      
        -- блокируем старую ТМЦ
        Lock_Tmc(l_old_tmc_id);
        if (NVL(l_old_equipment_tmc_id, 0) <> 0) then
          Lock_Tmc(l_old_equipment_tmc_id);
        end if;
        if (NVL(l_old_equipment_tmc_id_phone, 0) <> 0) then
          Lock_Tmc(l_old_equipment_tmc_id_phone);
        end if;
      
        -- если при редактировании подключения выбрана другая sim(ruim) карта
        -- или изменили признак связанности,
        -- то отменяем продажу предыдущей карты и добавляем продажу новой
        begin
          l_related := Get_Card_Related_Status(l_old_tmc_id);
        exception
          when others then
            logging_pkg.Raise_App_Err(-1000,
                                      'Не удалось определить статус связанности карты.');
        end;
      
        --Если карта не связана, получаем старый тарифный план
        l_new_tar_id := pi_abonent.tar_id;
        if (l_new_related in (0, 1)) then
          select ts.tar_id
            into l_old_tar_id
            from t_tmc_sim ts
           where ts.tmc_id = l_old_tmc_id;
        else
          l_old_tar_id := l_new_tar_id;
        end if;
      
        if (not (l_related = l_new_related or
            l_related = 2 and l_new_related = 0) or
           (l_new_tmc_id != l_old_tmc_id) or
           (l_new_tar_id != l_old_tar_id)) then
          -- отменяем старую операцию продажи
          Cancel_Tmc_Sell_Operation(l_old_op_id,
                                    l_old_tmc_id,
                                    pi_abonent.org_id,
                                    l_related,
                                    pi_worker_id);
        
          -- добавляем новую операцию продажи
          l_new_op_id := Add_Tmc_Sell_Operation(pi_abonent.org_id,
                                                l_new_tmc_id,
                                                pi_abonent.callsign,
                                                pi_abonent.imsi,
                                                pi_abonent.tar_id,
                                                l_ab_type,
                                                l_new_related,
                                                pi_is_alien_ab,
                                                pi_abonent.REQ_DELIVERY_ID,
                                                pi_worker_id,
                                                po_err_num,
                                                po_err_msg);
          if pi_abonent.REQ_DELIVERY_ID is not null then
            insert into T_TMC_OPERATION_REQUEST
              (oper_id, request_id, IS_EX_WORKS)
            values
              (l_new_op_id, pi_abonent.REQ_DELIVERY_ID, 0);
          end if;
        else
          l_new_op_id := l_old_op_id;
        end if;
      
        -- 40077 Сравнение нужно всегда
        --Если модель модема изменилась, то отменяем продажу предыдущего модема,
        --и добавляем продажу нового.
        -- 35948 (35953) сделан case в связи с появлением телефонов
        case pi_abonent.equipment_required
          when 4 then
            begin
              select tum.usb_model, tum.usb_ser
                into l_model_id, l_ser_num
                from t_tmc t, t_tmc_modem_usb tum, t_abonent a
               where t.tmc_id = tum.tmc_id
                 and t.tmc_id = a.equipment_tmc_id
                 and a.ab_id = pi_abonent.abonent_id;
            exception
              when no_data_found then
                l_model_id := 0;
            end;
            -- Если модель оборудования изменилась, то отменяем продажу предыдущего,
            -- и добавляем продажу нового.
            if pi_abonent.equipment_model_id <> l_model_id then
              --получаем ИД ТМЦ модема
              l_equipment_tmc_id := usb_modems.get_first_modem_by_model(pi_abonent.equipment_model_id,
                                                                        pi_abonent.org_id,
                                                                        pi_worker_id);
              -- блокируем новую ТМЦ
              if (nvl(l_equipment_tmc_id, 0) <> 0) then
                Lock_tmc(l_equipment_tmc_id);
              end if;
              if l_model_id <> 0 then
                -- блокируем старую ТМЦ
                if (NVL(l_old_equipment_tmc_id, 0) <> 0) then
                  Lock_Tmc(l_old_equipment_tmc_id);
                end if;
                -- Отменяем старую операцию продажи
                Cancel_Tmc_Sell_Operation(l_old_op_id,
                                          l_old_equipment_tmc_id,
                                          pi_abonent.org_id,
                                          l_related,
                                          pi_worker_id);
              end if;
              -- Вставляем новую операцию продажи
              usb_modems.sell_usb_modem(pi_abonent.org_id,
                                        pi_worker_id,
                                        l_equipment_tmc_id,
                                        l_new_op_id,
                                        pi_abonent.usb_ser,
                                        po_err_num,
                                        po_err_msg);
            else
              l_equipment_tmc_id := l_old_equipment_tmc_id;
              -- Если изменился серийный номер, то меняем его
              if nvl(l_ser_num, 0) <> nvl(pi_abonent.usb_ser, 0) then
                update t_tmc_modem_usb tmu
                   set tmu.usb_ser = pi_abonent.usb_ser
                 where tmu.tmc_id = l_equipment_tmc_id;
              end if;
            end if;
          when 7003 then
            begin
              select tp.model_id, tp.serial_number
                into l_model_id, l_ser_num
                from t_tmc t, t_tmc_phone tp, t_abonent a
               where t.tmc_id = tp.tmc_id
                 and t.tmc_id = a.equipment_tmc_id
                 and a.ab_id = pi_abonent.abonent_id;
            exception
              when no_data_found then
                l_model_id := 0;
            end;
            -- Если модель оборудования изменилась, то отменяем продажу предыдущего,
            -- и добавляем продажу нового.
            if pi_tmc_phone(1).model_id <> l_model_id then
              -- Получаем ИД ТМЦ телефона
              if pi_tmc_phone(1).tmc_id is null then
                -- Получаем ИД ТМЦ телефона
                l_equipment_tmc_id_phone := tmc_phones.get_first_phone_by_model(pi_tmc_phone(1)
                                                                                .model_id,
                                                                                pi_abonent.org_id,
                                                                                pi_worker_id);
              else
                l_equipment_tmc_id_phone := pi_tmc_phone(1).tmc_id;
              end if;
              -- блокируем новую ТМЦ
              if (nvl(l_equipment_tmc_id_phone, 0) <> 0) then
                Lock_tmc(l_equipment_tmc_id_phone);
              end if;
              if l_model_id <> 0 then
                -- блокируем старую ТМЦ
                if (NVL(l_old_equipment_tmc_id_phone, 0) <> 0) then
                  Lock_Tmc(l_old_equipment_tmc_id_phone);
                end if;
                -- Отменяем старую операцию продажи
                Cancel_Tmc_Sell_Operation(l_old_op_id,
                                          l_old_equipment_tmc_id_phone,
                                          pi_abonent.org_id,
                                          l_related,
                                          pi_worker_id);
              end if;
              -- Вставляем новую операцию продажи
              tmc_phones.sell_phone(pi_abonent.org_id,
                                    pi_worker_id,
                                    l_equipment_tmc_id_phone,
                                    l_new_op_id,
                                    pi_tmc_phone,
                                    po_err_num,
                                    po_err_msg);
            else
              l_equipment_tmc_id_phone := l_old_equipment_tmc_id_phone;
              -- Если изменился серийный номер, то меняем его
              if nvl(l_ser_num, 0) <> nvl(pi_tmc_phone(1).serial_number, 0) then
                update t_tmc_phone tp
                   set tp.serial_number = pi_tmc_phone(1).serial_number
                 where tp.tmc_id = l_equipment_tmc_id_phone;
              end if;
            end if;
          else
            null;
        end case;
      
        -- определим статус подключения
        if nvl(pi_is_alien_ab, 0) = 0 then
          l_status := Get_Real_Abon_Status(pi_abonent,
                                           l_new_tmc_id,
                                           pi_worker_id,
                                           pi_savetype,
                                           l_is_edit,
                                           pi_abonent.REQ_DELIVERY_ID,
                                           l_status_code,
                                           l_status_error);
        else
          l_status := pi_abonent.status;
        end if;
      
        -- определим старый статус абонента
        select ab_status,
               -- 57180
               a.equipment_required,
               a.equipment_tmc_id,
               a.id_op,
               a.org_id,
               a.ab_cost / 100,
               a.ab_reg_date,
               a.equipment_cost
          into l_old_status,
               -- 57180
               l_equipment_required,
               l_eq_tmc_id,
               l_op_id,
               l_org_id,
               l_cost,
               l_ab_reg_date,
               l_equipment_cost
          from t_abonent a
         where ab_id = pi_abonent.abonent_id;
      
        if (l_old_status in (102, 104, 112, 105)) then
          po_err_num := 9;
          po_err_msg := 'Запрещено редактировать подключения со статусами "Готов", "Одобрен АСР", "Одобрено вручную" или "Ожидает ответа от АСР".';
          logging_pkg.error(po_err_msg, c_package || 'Save_Abonent3');
          return -1;
        end if;
        --24.09.2010 зад.22932
        begin
          select o.IS_PAY_ESPP
            into l_espp
            from t_organizations o
           where o.org_id = pi_abonent.org_id;
        exception
          when others then
            null;
        end;
      
        -- 57180
        -- Допродажа доп. оборудования после визирования
        if l_status = 102 and l_old_status = 103 and
           l_equipment_required in (4, 7003) and
           pi_abonent.equipment_required <> -1 then
          select count(*)
            into l_cnt_before
            from t_acc_transfer tr
           where tr.tmc_op_id = l_op_id;
          if l_equipment_required = 4 then
            -- По старому модему смотрим, какая была модель
            select tum.usb_model, tum.usb_ser
              into l_eq_model, l_serial_number
              from t_tmc_modem_usb tum
             where tum.tmc_id = l_eq_tmc_id;
            -- Получаем ИД ТМЦ модема
            l_eq_tmc_id := usb_modems.get_first_modem_by_model(l_eq_model,
                                                               l_org_id,
                                                               pi_worker_id);
          
            if (l_eq_tmc_id = 0) then
              raise ex_no_eq;
            end if;
          
            -- Блокируем ТМЦ модема
            select tmc_id
              into dummy
              from t_tmc
             where tmc_id = l_eq_tmc_id
               for update nowait;
          
            -- Добавляем операцию продажи на уже существующую по СИМ-карте
            usb_modems.sell_usb_modem(l_org_id,
                                      pi_worker_id,
                                      l_eq_tmc_id,
                                      l_op_id,
                                      l_serial_number,
                                      po_err_num,
                                      po_err_msg);
          
            -- Проставляем новый tmc_id в абонента
            update t_abonent a
               set a.equipment_tmc_id = l_eq_tmc_id,
                   a.equipment_cost   = l_equipment_cost
             where a.ab_id = pi_abonent.abonent_id;
          elsif l_equipment_required = 7003 then
            -- По старому телефону смотрим, какая была модель и серийник
            select tp.model_id, tp.serial_number, tp.tmc_id
              into l_eq_model, l_serial_number, l_old_eq_id
              from t_tmc_phone tp
             where tp.tmc_id = l_eq_tmc_id;
          
            -- Стираем серийник со старого модема
            update t_tmc_phone tp
               set tp.serial_number = null
             where tp.tmc_id = l_old_tmc_id;
          
            -- Получаем ИД ТМЦ модема
            l_eq_tmc_id := tmc_phones.get_first_phone_by_model(l_eq_model,
                                                               l_org_id,
                                                               pi_worker_id);
          
            if (l_eq_tmc_id = 0) then
              raise ex_no_eq;
            end if;
          
            -- Блокируем ТМЦ телефона
            select tmc_id
              into dummy
              from t_tmc
             where tmc_id = l_eq_tmc_id
               for update nowait;
          
            -- Добавляем операцию продажи на уже существующую по СИМ-карте
            tmc_phones.sell_phone(l_org_id,
                                  pi_worker_id,
                                  l_eq_tmc_id,
                                  l_op_id,
                                  tmc_phone_tab(tmc_phone_type(l_eq_tmc_id,
                                                               l_serial_number,
                                                               l_eq_model,
                                                               null)),
                                  po_err_num,
                                  po_err_msg);
            -- Проставляем новый tmc_id в абонента
            update t_abonent a
               set a.equipment_tmc_id = l_eq_tmc_id,
                   a.equipment_cost   = l_equipment_cost
             where a.ab_id = pi_abonent.abonent_id;
          end if;
        elsif l_status = 102 and l_old_status = 103 and
              l_equipment_required in (4, 7003) and
              pi_abonent.equipment_required = -1 then
          -- Получаем id оборудования
          select a.equipment_tmc_id
            into l_old_tmc_id
            from t_abonent a
           where a.ab_id = pi_abonent.abonent_id;
          if l_equipment_required = 4 then
            -- Стираем серийник со старого модема
            update t_tmc_modem_usb tmu
               set tmu.usb_ser = null
             where tmu.tmc_id = l_old_tmc_id;
            -- Проставляем новый tmc_id в абонента
            update t_abonent a
               set a.equipment_tmc_id   = null,
                   a.equipment_cost     = 0,
                   a.equipment_required = 0
             where a.ab_id = pi_abonent.abonent_id;
          elsif l_equipment_required = 7003 then
            -- Стираем серийник со старого телефона
            update t_tmc_phone tp
               set tp.serial_number = null
             where tp.tmc_id = l_old_tmc_id;
            -- Проставляем новый tmc_id в абонента
            update t_abonent a
               set a.equipment_tmc_id   = null,
                   a.equipment_cost     = 0,
                   a.equipment_required = 0
             where a.ab_id = pi_abonent.abonent_id;
          end if;
        end if;
      
        -- редактируем запись в T_ABONENT
        update t_abonent
           set client_id   = pi_abonent.client_id,
               ab_mod_date = sysdate,
               phone_make  = pi_abonent.phone_make,
               phone_model = pi_abonent.phone_model,
               phone_imei  = pi_abonent.phone_imei,
               ab_status   = l_status,
               ab_paid     = pi_abonent.paid,
               ab_cost     = pi_abonent.cost,
               ab_tmc_id   = l_new_tmc_id,
               --ab_sync            = 0,
               ab_dog_date        = pi_abonent.dog_date,
               err_code           = decode(pi_abonent.err_code,
                                           0,
                                           l_status_code,
                                           pi_abonent.err_code),
               err_msg            = pi_abonent.err_msg || l_status_error,
               org_id             = pi_abonent.org_id,
               cc_id              = nvl(l_cc_id, cc_id),
               id_op              = l_new_op_id,
               root_org_id        = Orgs.Get_Root_Org_Or_Self(pi_abonent.org_id),
               user_id            = pi_abonent.user_id,
               ab_note            = pi_abonent.note,
               worker_id          = pi_worker_id,
               equipment_required = pi_abonent.equipment_required,
               -- 35948 (35953) Брендированное оборудование (телефоны)
               equipment_tmc_id = nvl(l_equipment_tmc_id,
                                      l_equipment_tmc_id_phone),
               equipment_cost   = pi_abonent.equipment_cost,
               with_ab_pay      = l_espp,
               is_common        = pi_is_common,
               CLIENT_ACCOUNT   = pi_abonent.CLIENT_ACCOUNT,
               -- 51312 Бюджетирование
               LIMIT_TYPE       = pi_abonent.LIMIT_TYPE,
               LIMIT_WARNING    = pi_abonent.LIMIT_WARNING,
               LIMIT_BLOCK      = pi_abonent.LIMIT_BLOCK,
               LIMIT_DATE_START = pi_abonent.LIMIT_DATE_START,
               LIMIT_DATE_END   = pi_abonent.LIMIT_DATE_END,
               seller_active_id = pi_abonent.seller_active_id,
               equipment_id     = l_equipment_id,
               state_sbdpn      = decode(pi_is_alien_ab,
                                         1,
                                         decode(l_status,
                                                102,
                                                0,
                                                98,
                                                0,
                                                97,
                                                5,
                                                96,
                                                6,
                                                state_sbdpn),
                                         null),
               dog_id           = decode(is_org_usi(pi_abonent.org_id),
                                         1,
                                         null,
                                         l_dog_id),
               -- 69988
               channel_id      = pi_abonent.channel_id,
               CODE_WORD       = nvl(pi_abonent.CODE_WORD, CODE_WORD),
               OUT_ACCOUNT     = nvl(pi_abonent.OUT_ACCOUNT, OUT_ACCOUNT),
               CREDIT_LIMIT    = nvl(pi_abonent.CREDIT_LIMIT, CREDIT_LIMIT),
               ALARM_LIMIT     = nvl(pi_abonent.ALARM_LIMIT, ALARM_LIMIT),
               reg_id_ps       = nvl(pi_abonent.reg_id_ps, reg_id_ps),
               ab_comment      = pi_abonent.ab_comment,
               req_delivery_id = pi_abonent.req_delivery_id,
               consent_msg     = pi_abonent.consent_msg,
               method_connect  = pi_abonent.method_connect
         where ab_id = pi_abonent.abonent_id
        returning ab_id into l_ab_id;
      
        if pi_abonent.request_id is not null then
          update t_abonent_to_request
             set request_id = pi_abonent.request_id
           where ab_id = l_ab_id;
        end if;
      
        -- 51312 Бюджетирование
        if pi_abonent.LIMIT_TYPE is not null then
          select count(*)
            into l_budget_count
            from t_abonent_budget ab
           where ab.ab_id = pi_abonent.abonent_id;
          if l_budget_count = 0 then
            insert into t_abonent_budget
              (AB_ID,
               CLIENT_ID,
               LIMIT_TYPE,
               LIMIT_WARNING,
               LIMIT_BLOCK,
               LIMIT_DATE_START,
               LIMIT_DATE_END,
               IS_PERSONAL_ACCOUNT)
            values
              (pi_abonent.abonent_id,
               pi_abonent.limit_client_id,
               pi_abonent.limit_type,
               pi_abonent.limit_warning,
               pi_abonent.limit_block,
               pi_abonent.limit_date_start,
               pi_abonent.limit_date_end,
               pi_abonent.IS_PERSONAL_ACCOUNT);
          else
            update t_abonent_budget ab
               set ab.client_id        = pi_abonent.limit_client_id,
                   ab.limit_type       = pi_abonent.limit_type,
                   ab.limit_warning    = pi_abonent.limit_warning,
                   ab.limit_block      = pi_abonent.limit_block,
                   ab.limit_date_start = pi_abonent.limit_date_start,
                   ab.limit_date_end   = pi_abonent.limit_date_end,
                   IS_PERSONAL_ACCOUNT = nvl(pi_abonent.IS_PERSONAL_ACCOUNT,
                                             IS_PERSONAL_ACCOUNT)
             where ab.ab_id = pi_abonent.abonent_id;
          end if;
        end if;
      
        -- редактируем услуги
        delete from t_serv_abonent where ab_id = pi_abonent.abonent_id;
        for i in 1 .. pi_services.count loop
          if (pi_services(i).serv_id is not null) then
            insert into T_SERV_ABONENT
              (AB_ID, SERV_ID)
            values
              (pi_abonent.abonent_id, pi_services(i).serv_id);
          end if;
        end loop;
        -- 35948 (35953) Редактируем связь абонента, телефона и услуги
        if pi_abonent.equipment_required = 7003 then
          logging_pkg.info('l_equipment_tmc_id_phone:=' ||
                           l_equipment_tmc_id_phone || ',
                     pi_tmc_phone(1).bonus_id:=' || pi_tmc_phone(1)
                           .bonus_id || ',
                     pi_abonent.abonent_id:=' ||
                           pi_abonent.abonent_id,
                           'pi_abonent.equipment_required = 7003');
          update t_abonent_phone ap
             set ap.tmc_id   = l_equipment_tmc_id_phone,
                 ap.bonus_id = pi_tmc_phone(1).bonus_id
           where ap.ab_id = pi_abonent.abonent_id;
        end if;
      end if;
    
      /*      logging_pkg.debug('edit. err_code=' || pi_abonent.err_code ||
      ', status_error=' || l_status_error,
      c_package || 'Save_Abonent3');*/
    
      if (nvl(pi_abonent.err_code, 0) = 0 and l_status_error is null) then
        po_err_msg := '';
        po_is_ok   := 1;
      else
        po_err_msg := '';
        po_is_ok   := 2;
      end if;
      --задача 25988
      if pi_abonent.citynum is not null then
        select t.is_related, t.tmc_id, to_number(t.federal_callsign)
          into l_check_rel, l_check_city, l_fed_num
          from t_callsign_city tc
          left join t_callsign t
            on tc.tmc_id = t.callsign_city_id
           and t.tmc_id in
               (select s.tmc_id
                  from t_org_tmc_status s
                  join t_callsign tt
                    on s.tmc_id = tt.tmc_id
                 where tt.callsign_city_id = t.callsign_city_id)
        
         where tc.CALLSIGN_CITY = pi_abonent.citynum;
        if l_fed_num is null or l_fed_num <> pi_abonent.callsign then
          select t.callsign_city_id
            into l_city_id
            from t_callsign t
           where t.federal_callsign = pi_abonent.callsign;
          if l_city_id is not null then
            update t_callsign_city t
               set t.is_related = 0
             where t.tmc_id = l_city_id;
          end if;
          if l_fed_num is not null then
            update t_callsign t
               set t.callsign_city_id = null
             where t.tmc_id = l_check_city;
          end if;
          update t_callsign_city t
             set t.is_related = 1
           where t.callsign_city = pi_abonent.citynum;
          update t_callsign t
             set t.callsign_city_id =
                 (select tc.tmc_id
                    from t_callsign_city tc
                   where tc.callsign_city = pi_abonent.citynum)
           where t.federal_callsign = pi_abonent.callsign;
        end if;
      end if;
    else
      -- параметры sim(ruim)-карты некорректны
      -- если создается новое подключение
      if (l_is_edit = 0) then
        -- добавляем запись в T_ABON_BAD
        l_ab_id := Add_Abonent_Bad(pi_abonent,
                                   pi_abonent.user_id,
                                   pi_worker_id,
                                   l_errors,
                                   pi_is_common,
                                   pi_is_alien_ab,
                                   l_dog_id);
        if pi_abonent.equipment_required = 4 then
          update t_abon_bad ab
             set ab.equipment_ser = pi_abonent.usb_ser
           where ab.id = l_ab_id;
        end if;
        -- 35948 (35953) Добавляем в t_abon_bad модель, серию и услугу
        if pi_abonent.equipment_required = 7003 then
          update t_abon_bad ab
             set ab.phone_model_id      = pi_tmc_phone(1).model_id,
                 ab.phone_serial_number = pi_tmc_phone(1).serial_number,
                 ab.phone_bonus_id      = pi_tmc_phone(1).bonus_id
           where ab.id = l_ab_id;
        end if;
      elsif (pi_abonent.is_bad = 1) then
        -- редактирование ошибочного подключения (осталось ошибочным)
        -- редактируем запись в T_ABON_BAD
        update t_abon_bad
           set dog_date           = pi_abonent.dog_date,
               id_org             = pi_abonent.org_id,
               id_cc              = nvl(l_cc_id, id_cc),
               imsi               = pi_abonent.imsi,
               callsign           = pi_abonent.callsign,
               city_num           = pi_abonent.citynum,
               sim_color          = pi_abonent.simcolor,
               tar_name           = pi_abonent.tar_name,
               id_tar             = pi_abonent.tar_id,
               id_client          = pi_abonent.client_id,
               mod_date           = sysdate,
               phone_make         = pi_abonent.phone_make,
               phone_model        = pi_abonent.phone_model,
               phone_imei         = pi_abonent.phone_imei,
               paid               = pi_abonent.paid,
               cost               = pi_abonent.cost,
               err_msg            = pi_abonent.err_msg || l_errors,
               user_id            = pi_abonent.user_id,
               worker_id          = pi_worker_id,
               is_related         = l_new_related,
               equipment_required = pi_abonent.equipment_required,
               equipment_model_id = pi_abonent.equipment_model_id,
               equipment_cost     = pi_abonent.equipment_cost,
               is_common          = pi_is_common,
               CLIENT_ACCOUNT     = pi_abonent.client_account,
               -- 51312 Бюджетирование
               limit_type       = pi_abonent.limit_type,
               limit_warning    = pi_abonent.limit_warning,
               limit_block      = pi_abonent.limit_block,
               limit_date_start = pi_abonent.limit_date_start,
               limit_date_end   = pi_abonent.limit_date_end,
               SELLER_ACTIVE_ID = pi_abonent.seller_active_id,
               -- 69988
               channel_id      = pi_abonent.channel_id,
               reg_id_ps       = pi_abonent.reg_id_ps,
               ab_comment      = pi_abonent.ab_comment,
               req_delivery_id = pi_abonent.req_delivery_id,
               request_id      = pi_abonent.request_id,
               consent_msg     = pi_abonent.consent_msg,
               method_connect  = pi_abonent.method_connect
         where id = pi_abonent.abonent_id;
      
        -- 51312 Бюджетирование
        if pi_abonent.LIMIT_TYPE is not null then
          select count(*)
            into l_budget_count
            from t_abon_bad_budget ab
           where ab.bad_id = pi_abonent.abonent_id;
          if l_budget_count = 0 then
            insert into t_abon_bad_budget
              (bad_id,
               CLIENT_ID,
               LIMIT_TYPE,
               LIMIT_WARNING,
               LIMIT_BLOCK,
               LIMIT_DATE_START,
               LIMIT_DATE_END,
               IS_PERSONAL_ACCOUNT)
            values
              (pi_abonent.abonent_id,
               pi_abonent.limit_client_id,
               pi_abonent.limit_type,
               pi_abonent.limit_warning,
               pi_abonent.limit_block,
               pi_abonent.limit_date_start,
               pi_abonent.limit_date_end,
               pi_abonent.IS_PERSONAL_ACCOUNT);
          else
            update t_abon_bad_budget ab
               set ab.client_id           = pi_abonent.limit_client_id,
                   ab.limit_type          = pi_abonent.limit_type,
                   ab.limit_warning       = pi_abonent.limit_warning,
                   ab.limit_block         = pi_abonent.limit_block,
                   ab.limit_date_start    = pi_abonent.limit_date_start,
                   ab.limit_date_end      = pi_abonent.limit_date_end,
                   ab.IS_PERSONAL_ACCOUNT = pi_abonent.IS_PERSONAL_ACCOUNT
             where ab.bad_id = pi_abonent.abonent_id;
          end if;
        end if;
      
        -- 35948 (35953)
        if pi_abonent.equipment_required = 7003 then
          update t_abon_bad
             set phone_model_id      = pi_tmc_phone(1).model_id,
                 phone_serial_number = pi_tmc_phone(1).serial_number,
                 phone_bonus_id      = pi_tmc_phone(1).bonus_id
           where id = pi_abonent.abonent_id;
        end if;
        if pi_abonent.equipment_required = 4 then
          update t_abon_bad a
             set a.equipment_ser = pi_abonent.usb_ser
           where id = pi_abonent.abonent_id;
        end if;
      else
        -- стало ошибочным после редактирования
        -- помечаем подключение, как удаленное
        update t_abonent
           set ab_mod_date = sysdate, is_deleted = 1
         where ab_id = pi_abonent.abonent_id;
        -- получим старый идентификатор ТМЦ и идентификатор операции продажи
        select a.ab_tmc_id, a.id_op, a.equipment_tmc_id
          into l_old_tmc_id, l_old_op_id, l_old_equipment_tmc_id
          from t_abonent a
         where a.ab_id = pi_abonent.abonent_id;
      
        -- блокируем ТМЦ
        Lock_Tmc(l_old_tmc_id);
        if (NVL(l_old_equipment_tmc_id, 0) <> 0) then
          Lock_Tmc(l_old_equipment_tmc_id);
        end if;
      
        -- отменяем старую операцию продажи
        begin
          l_related := Get_Card_Related_Status(l_old_tmc_id);
        exception
          when others then
            logging_pkg.Raise_App_Err(-1000,
                                      'Не удалось определить статус связанности карты.');
        end;
        logging_pkg.info('cancel tmc operation. op_id=' || l_old_op_id ||
                         ', equipment_id=' || l_old_equipment_tmc_id ||
                         ', tmc_id=' || l_old_tmc_id,
                         c_package || 'Save_Abonent3');
        if (l_old_tmc_id is not null) then
          Cancel_Tmc_Sell_Operation(l_old_op_id,
                                    l_old_tmc_id,
                                    pi_abonent.org_id,
                                    l_related,
                                    pi_worker_id);
        end if;
        if (NVL(l_old_equipment_tmc_id, 0) <> 0) then
          logging_pkg.info('cancel sell. old_equipment=' ||
                           l_old_equipment_tmc_id || ', related=' ||
                           l_related,
                           c_package || 'Save_Abonent3');
        
          Cancel_Tmc_Sell_Operation(l_old_op_id,
                                    l_old_equipment_tmc_id,
                                    pi_abonent.org_id,
                                    l_related,
                                    pi_worker_id);
        end if;
        -- добавляем запись в T_ABON_BAD (+ причину ошибки)
        l_ab_id := Add_Abonent_Bad(pi_abonent,
                                   pi_abonent.user_id,
                                   pi_worker_id,
                                   l_errors,
                                   pi_is_common,
                                   pi_is_alien_ab,
                                   l_dog_id);
        -- 35948 (35953) Добавляем в t_abon_bad модель, серию и услугу
        if pi_abonent.equipment_required = 7003 then
          update t_abon_bad ab
             set ab.phone_model_id      = pi_tmc_phone(1).model_id,
                 ab.phone_serial_number = pi_tmc_phone(1).serial_number,
                 ab.phone_bonus_id      = pi_tmc_phone(1).bonus_id
           where ab.id = l_ab_id;
        end if;
        if pi_abonent.equipment_required = 4 then
          update t_abon_bad a
             set a.equipment_ser = pi_abonent.usb_ser
           where id = pi_abonent.abonent_id;
        end if;
      
      end if;
    
    end if;
  
    /*    logging_pkg.debug('exit. err_code=' || pi_abonent.err_code ||
    ', status_error=' || l_status_error,
    c_package || 'Save_Abonent3');*/
  
    if ((pi_abonent.err_code = 0 or pi_abonent.err_code is null) and
       l_status_error is null) then
      po_err_msg := '';
      --      po_is_ok   := 1;
    end if;
  
    return l_ab_id;
  
  exception
    -- 57180
    when ex_no_unique then
      po_err_num := 3;
      po_err_msg := 'Нарушена уникальность серийного номера модема.';
      rollback to save_abonent;
      return - 1;
    when ex_no_eq then
      --rollback;
      po_err_num := 2;
      po_err_msg := 'Доп. оборудование не найдено на складе организации.';
      rollback to save_abonent;
      return - 1;
    when ex_resource_busy then
      po_err_num := 1;
      po_err_msg := 'Возник конфликт одновременного доступа. Повторите операцию позднее.';
      rollback to save_abonent;
      return - 1;
    when ex_no_req then
      po_err_num := 4;
      po_err_msg := 'Из заявки на доставку ' || pi_abonent.REQ_DELIVERY_ID ||
                    ' была удалена SIM-карта: IMSI ' || pi_abonent.imsi ||
                    ', федеральный номер ' || pi_abonent.callsign ||
                    '. Сохранение подключения невозможно.';
      rollback to save_abonent;
      return - 1;
    when ex_deliv_dog then
      po_err_num := 5;
      po_err_msg := 'Данная SIM-карта предназначена для продажи в рамках доставки оборудования.';
      rollback to save_abonent;
      return - 1;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg || ' ' || l_str,
                        c_package || 'Save_Abonent3');
      rollback to save_abonent;
      return - 1;
    
  end Save_Abonent3;
  ------------------------------------------------------------------------------------------
  -- Перевызов 35948 (35953) Брендированное оборудование (телефоны)
  function Save_Abonent3(pi_abonent   in abonent_type,
                         pi_services  in service_tab,
                         pi_savetype  in number,
                         pi_worker_id in T_USERS.USR_ID%type,
                         pi_cost      in number,
                         pi_is_common in number,
                         pi_tmc_phone in tmc_phone_tab,
                         po_is_ok     out number, -- 0- t_abon_bad, 1- t_abonent без ошибок, 2- t_abonent c ошибками
                         po_err_num   out pls_integer,
                         po_err_msg   out t_Err_Msg)
    return T_ABONENT.AB_ID%type is
  begin

    return Save_Abonent3(pi_abonent,
                         pi_services,
                         pi_savetype,
                         pi_worker_id,
                         pi_cost,
                         pi_is_common,
                         pi_tmc_phone, -- тип телефоны
                         0,
                         po_is_ok,
                         po_err_num,
                         po_err_msg);

  end Save_Abonent3;

  ----------------------------------------------------------------------------
  function Get_Abonent_By_Id2(pi_ab_id     in T_ABONENT.AB_ID%type,
                              pi_worker_id in T_USERS.USR_ID%type,
                              po_err_num   out pls_integer,
                              po_err_msg   out t_Err_Msg,
                              pi_is_bad    in number := 0)
    return sys_refcursor is
    res      sys_refcursor;
    l_org_id T_ORGANIZATIONS.ORG_ID%type;
  begin

    logging_pkg.debug(pi_message  => 'pi_ab_id = ' || pi_ab_id ||
                                     ' pi_worker_id = ' || pi_worker_id ||
                                     ' pi_is_bad = ' || pi_is_bad,
                      pi_location => 'get_abonent_by_id2');

    select Max(ORG_ID)
      into l_org_id
      from (select AB.ORG_ID
              from T_ABONENT AB
             Where ab.ab_id = pi_ab_id
               and pi_is_bad = 0
            union
            select AB_B.ID_ORG ORG_ID
              from T_ABON_BAD AB_B
             where ab_b.id = pi_ab_id
               and pi_is_bad = 1);

    -- checking access for operation for specified user
    if (not Security_pkg.Check_Rights_str('EISSD.CONNECTIONS.GSM.VIEW',
                                          l_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      return null;
    end if;

    -- берем абонента
    open res for
      select A.AB_ID ABONENT_ID,
             A.CLIENT_ID,
             A.AB_TMC_ID TMC_ID,
             A.AB_REG_DATE REG_DATE,
             A.AB_MOD_DATE MOD_DATE,
             A.PHONE_MAKE,
             A.PHONE_MODEL,
             A.PHONE_IMEI,
             A.AB_STATUS STATUS,
             A.AB_PAID PAID,
             A.AB_COST COST,
             A.ERR_CODE,
             getAsrError(A.ERR_CODE, A.ERR_MSG) ERR_MSG,
             A.ORG_ID,
             A.CC_ID,
             A.ORG_ID WHICH_ORG_AB_ID,
             (select OOO.ORG_NAME
                from T_ORGANIZATIONS OOO
               where OOO.ORG_ID = A.ORG_ID) WHICH_ORG_AB_NAME,
             NVL2(A.TAR_ID,
                  (select CONCAT(DV1.DV_NAME,
                                 CONCAT(' ',
                                        CONCAT(DV3.DV_NAME,
                                               CONCAT('  ', DV2.DV_NAME))))
                     from T_TARIFF2    T,
                          T_DIC_VALUES DV1,
                          T_DIC_VALUES DV2,
                          T_DIC_VALUES DV3
                    where T.ID = get_tar_id_by_at_id(A.TAR_ID, A.ab_dog_date)
                      and T.AT_ID = A.TAR_ID
                      and T.TYPE_VDVD_ID = DV1.DV_ID
                      and T.TARIFF_TYPE = DV2.DV_ID
                      and T.PAY_TYPE = DV3.DV_ID),
                  'не известен') CONN_NAME,
             A.TAR_ID TAR_ID,
             a.tar_ver_date_end,
             a.AB_TAR_NAME TAR_NAME,
             NVL2(A.TAR_ID,
                  (select T.TYPE_VDVD_ID
                     from T_TARIFF2 T
                    where T.ID = get_tar_id_by_at_id(A.TAR_ID, ab_dog_date)),
                  null) TYPE_VDVD_ID,
             A.IMSI IMSI,
             A.CALLSIGN CS,
             A.CITY_NUM,
             A.SIM_COLOR,
             A.AB_DOG_DATE DOG_DATE,
             A.IS_BAD,
             A.ID_OP,
             A.ID_BAD AB_ID_BAD,
             A.AB_SERVS,
             (select trim(cc.cc_name)
                from T_CALC_CENTER cc
               where cc.cc_id = a.cc_id) CC_NAME,
             a.CHANGE_STATUS_DATE,
             '' DOG_NUMBER,
             A.ARCHIVE_MARK,
             A.USER_ID USER_ID,
             A.USER_ID WorkER,
             a.tmc_type,
             a.usb_full_cost,
             a.usb_model,
             A.IS_RELATED,
             a.package_id,
             a.client_remote_id,
             pp.person_lastname || ' ' || pp.person_firstname || ' ' ||
             pp.person_middlename seller,
             a.client_account,
             -- 28.10.2010 25247
             a.is_common,
             a.client_category,
             a.client_class,
             a.billing_group,
             a.limit_type,
             a.limit_warning,
             a.limit_block,
             a.limit_date_start,
             a.limit_date_end,
             a.ab_status_ex,
             a.budget_client,
             null               cg_id,
             -- 35948 (35953) Брендированное оборудование (телефоны)
             a.phone_serial_number,
             a.phone_name_model,
             a.phone_cost,
             a.phone_bonus_name,
             a.phone_bonus_id,
             a.phone_model_id,
             -- 42523 Модель модема
             a.usb_model_name,
             a.sa_id                 seller_active_id,
             a.seller_active_fio,
             a.is_resident,
             a.reg_deadline,
             sim_type_name,
             a.attached_doc,
             a.number_sheets,
             a.usb_ser,
             wish_date,
             agree_work_info,
             agree_cancel_dog,
             agree_pay_off_debt,
             state_sbdpn,
             transfer_phone,
             operation_donor,
             npid,
             type_process_cbdpn,
             state_sbdpn_name,
             type_portation,
             operator_recipient,
             LSMStransStatus,
             LSMSaddTransStatus,
             LSMStransStatus_name,
             LSMSaddTransStatus_name,
             description,
             mnp_oper_don_name,
             mnp_oper_don_code,
             mnp_oper_rec_name,
             mnp_oper_rec_code,
             SMSNOTICEFLAG,
             EMAILNOTICEFLAG,
             date_create_cbdpn,
             correlationid,
             COMMUN_DESTINATION,
             reject_reason, --причина отказа
             channel_id,
             CODE_WORD,
             OUT_ACCOUNT,
             CREDIT_LIMIT,
             ALARM_LIMIT,
             IS_PERSONAL_ACCOUNT,
             a.remote_id,
             a.reg_id_ps,
             a.usl_number,
             a.kl_region_id,
             a.fix_address_id,
             a.fix_name,
             a.ab_comment,
             a.req_delivery_id,
             a.request_id,
             a.consent_msg,
             a.agr_num,
             a.method_connect
        from (select AB1.AB_ID,
                     null ID_BAD,
                     AB1.CLIENT_ID,
                     AB1.AB_REG_DATE - 2 / 24 AB_REG_DATE,
                     AB1.AB_MOD_DATE - 2 / 24 AB_MOD_DATE,
                     AB1.PHONE_MAKE,
                     AB1.PHONE_MODEL,
                     AB1.PHONE_IMEI,
                     AB1.AB_STATUS,
                     AB1.AB_PAID,
                     AB1.AB_COST,
                     AB1.AB_TMC_ID,
                     --AB1.AB_SYNC,
                     AB1.ERR_CODE,
                     AB1.ERR_MSG,
                     AB1.ORG_ID,
                     AB1.REMOTE_ID,
                     AB1.CHANGE_STATUS_DATE - 2 / 24 CHANGE_STATUS_DATE,
                     AB1.CC_ID,
                     AB1.AB_DOG_DATE,
                     0 IS_BAD,
                     AB1.ID_OP,
                     SIM.TAR_ID,
                     tar.title AB_TAR_NAME,
                     null AB_SERVS,
                     AB1.ARCHIVE_MARK,
                     nvl(SIM.SIM_IMSI, sim.sim_iccid) IMSI,
                     SIM.SIM_CALLSIGN CALLSIGN,
                     to_number(SIM.SIM_CALLSIGN_CITY) CITY_NUM,
                     SIM.SIM_COLOR,
                     AB1.USER_ID,
                     tt.tmc_type,
                     tum.usb_full_cost,
                     tum.usb_model as usb_model,
                     NVL(SIM.IS_RELATED, 1) IS_RELATED,
                     ab1.package_id,
                     ab1.client_remote_id,
                     ab1.client_account,
                     -- 28.10.2010 25247
                     ab1.is_common,
                     j.jur_category      as client_category,
                     j.jur_class         as client_class,
                     j.jur_billing_group as billing_group,
                     tb.limit_type,
                     tb.limit_warning,
                     tb.limit_block,
                     tb.limit_date_start,
                     tb.limit_date_end,
                     tb.client_id        as budget_client,
                     ab1.ab_status_ex,
                     -- 35948 (35953) Брендированное оборудование (телефоны)
                     tp.serial_number as phone_serial_number,
                     mp.name_model    as phone_name_model,
                     -- 39401
                     pmc.cost_with_nds as phone_cost,
                     bp.bonus_name     as phone_bonus_name,
                     bp.bonus_id       as phone_bonus_id,
                     mp.model_id       as phone_model_id,
                     -- 42523 Модель модема
                     mmu.usb_model    as usb_model_name,
                     tar.ver_date_end tar_ver_date_end,
                     sa.sa_id,
                     -- 58777
                     -- 58777
                     decode(ab1.seller_active_id,
                            null,
                            null,
                            tsu.person_lastname || ' ' ||
                            tsu.person_firstname || ' ' ||
                            tsu.person_middlename || ' (' || su.su_emp_num || ')' || '/' ||
                            pers.person_lastname || ' ' ||
                            pers.person_firstname || ' ' ||
                            pers.person_middlename || ' (' || sa.sa_emp_num || ')') seller_active_fio,
                     c.is_resident,
                     c.reg_deadline,
                     sim.sim_type sim_type,
                     dic_st.name sim_type_name,
                     ab1.attached_doc,
                     ab1.number_sheets,
                     tum.usb_ser,
                     ab1.transfer_date wish_date,
                     ab1.agree_work_info,
                     ab1.agree_cancel_dog,
                     ab1.agree_pay_off_debt,
                     ab1.state_sbdpn,
                     ab1.transfer_phone,
                     ab1.operation_donor,
                     ab1.npid,
                     ab1.type_process_cbdpn,
                     mnp_st.name_state state_sbdpn_name,
                     ab1.type_request type_portation,
                     ab1.operator_recipient,
                     LSMStransStatus,
                     LSMSaddTransStatus,
                     nvl(lsms.full_name, LSMStransStatus) LSMStransStatus_name,
                     nvl(lsms_add.full_name, LSMSaddTransStatus) LSMSaddTransStatus_name,
                     description,
                     mnp_oper_don.name mnp_oper_don_name,
                     mnp_oper_don.operatorcode mnp_oper_don_code,
                     mnp_oper_rec.name mnp_oper_rec_name,
                     mnp_oper_rec.operatorcode mnp_oper_rec_code,
                     SMSNOTICEFLAG,
                     EMAILNOTICEFLAG,
                     ab1.date_create_cbdpn date_create_cbdpn,
                     ab1.correlationid,
                     ab1.COMMUN_DESTINATION,
                     ab1.reject_reason,
                     ab1.channel_id,
                     ab1.CODE_WORD,
                     ab1.OUT_ACCOUNT,
                     ab1.CREDIT_LIMIT,
                     ab1.ALARM_LIMIT,
                     tb.IS_PERSONAL_ACCOUNT,
                     ab1.reg_id_ps,
                     ab1.usl_number,
                     ab1.kl_region_id,
                     ab1.fix_address_id,
                     ab1.fix_name,
                     ab1.ab_comment,
                     ab1.req_delivery_id,
                     abr.request_id,
                     ab1.consent_msg,
                     ab1.agr_num,
                     ab1.method_connect
                from T_ABONENT AB1
                join t_tmc_sim sim
                  on ab1.ab_tmc_id = sim.tmc_id
                left join t_dic_sim_type dic_st
                  on dic_st.id = sim.sim_type
                left join t_tmc tt
                  on ab1.equipment_tmc_id = tt.tmc_id
                left join t_tmc_modem_usb tum
                  on tum.tmc_id = tt.tmc_id
                 and tt.tmc_type = 4
              -- 42523 Модель модема
                left join t_modem_model_usb mmu
                  on tum.usb_model = mmu.id
              -- 35948 (35953) Брендированное оборудование (телефоны)
                left join t_abonent_phone ap
                  on ap.ab_id = ab1.ab_id
                left join t_tmc_phone tp
                  on tp.tmc_id = ap.tmc_id
                left join t_model_phone mp
                  on tp.model_id = mp.model_id
                left join t_bonus_phone bp
                  on bp.bonus_id = ap.bonus_id
              -- 39401
                left join t_phone_model_cost pmc
                  on mp.model_id = pmc.model_id
                 and sysdate between pmc.ver_date_beg and
                     nvl(pmc.ver_date_end, sysdate)
                 and pmc.region_id in
                     (select distinct o.region_id
                        from t_organizations o
                       where o.org_id = AB1.ORG_ID)
                left join t_clients c
                  on c.client_id = ab1.client_id
                left join t_juristic j
                  on j.juristic_id = c.fullinfo_id
                left join t_abonent_budget tb
                  on tb.ab_id = ab1.ab_id
                left join t_tarif_by_at_id tar
                  on tar.at_id = sim.tar_id
                left join t_seller_active sa
                  on sa.sa_id = ab1.seller_active_id
                left join t_person pers
                  on sa.sa_person_id = pers.person_id
              -- 58777
                left join t_seller_active_rel sar
                  on sar.sa_id = sa.sa_id
                 and ab1.ab_reg_date >= sar.date_from
                 and ab1.ab_reg_date < nvl(sar.date_to, sysdate)
                left join t_supervisor su
                  on su.su_id = sar.su_id
                left join t_person tsu
                  on tsu.person_id = su.su_person_id
                left join t_dic_mnp_state mnp_st
                  on mnp_st.id_state = ab1.state_sbdpn
                left join t_dic_mnp_operator mnp_oper_don
                  on mnp_oper_don.id = ab1.operation_donor
                left join t_dic_mnp_operator mnp_oper_rec
                  on mnp_oper_rec.id = ab1.operator_recipient
                left join T_DIC_MNP_LSMS_STATUS lsms
                  on lsms.alt_name = ab1.lsmstransstatus
                left join T_DIC_MNP_LSMS_ADD_STATUS lsms_add
                  on lsms_add.alt_name = ab1.lsmsaddtransstatus
                left join t_abonent_to_request abr
                  on abr.ab_id=ab1.ab_id
               where ab1.is_deleted = 0
                 and Ab1.AB_ID = pi_ab_id
              union all
              select null AB_ID,
                     AB2.ID ID_BAD,
                     AB2.ID_CLIENT CLIENT_ID,
                     AB2.REG_DATE - 2 / 24 AB_REG_DATE,
                     AB2.MOD_DATE - 2 / 24 AB_MOD_DATE,
                     AB2.PHONE_MAKE,
                     AB2.PHONE_MODEL,
                     AB2.PHONE_IMEI,
                     101 AB_STATUS,
                     AB2.PAID AB_PAID,
                     AB2.COST AB_COST,
                     AB2.ID_TMC AB_TMC_ID,
                     --0 AB_SYNC,
                     NVL2(AB2.ERR_MSG, -100, null) ERR_CODE,
                     AB2.ERR_MSG,
                     AB2.ID_ORG ORG_ID,
                     null REMOTE_ID,
                     null CHANGE_STATUS_DATE,
                     AB2.ID_CC CC_ID,
                     AB2.DOG_DATE - 2 / 24 AB_DOG_DATE,
                     1 IS_BAD,
                     AB2.ID_OP,
                     AB2.ID_TAR TAR_ID,
                     AB2.TAR_NAME AB_TAR_NAME,
                     AB2.SERVICE_AREA AB_SERVS,
                     AB2.ARCHIVE_MARK,
                     AB2.IMSI,
                     AB2.CALLSIGN,
                     AB2.CITY_NUM,
                     AB2.SIM_COLOR,
                     AB2.USER_ID,
                     ab2.equipment_required as tmc_type,
                     ab2.equipment_cost / 100 as usb_full_cost,
                     ab2.equipment_model_id as usb_model,
                     -- Neverov 01.09.2010
                     AB2.IS_RELATED,
                     ab2.package_id,
                     null               client_remote_id,
                     ab2.client_account,
                     -- 28.10.2010 25247
                     ab2.is_common,
                     j.jur_category       as client_category,
                     j.jur_class          as client_class,
                     j.jur_billing_group  as billing_group,
                     tbb.limit_type,
                     tbb.limit_warning,
                     tbb.limit_block,
                     tbb.limit_date_start,
                     tbb.limit_date_end,
                     tbb.client_id        as budget_client,
                     null                 as ab_status_ex,
                     -- 35948 (35953) Брендированное оборудование (телефоны)
                     ab2.phone_serial_number as phone_serial_number,
                     mp.name_model           as phone_name_model,
                     -- 39401
                     pmc.cost_with_nds as phone_cost,
                     bp.bonus_name     as phone_bonus_name,
                     bp.bonus_id       as phone_bonus_id,
                     mp.model_id       as phone_model_id,
                     -- 42523 Модель модема
                     mmu.usb_model    as usb_model_name,
                     TAR.VER_DATE_END tar_ver_date_end,
                     sa.sa_id,
                     -- 58777
                     decode(ab2.seller_active_id,
                            null,
                            null,
                            tsu.person_lastname || ' ' ||
                            tsu.person_firstname || ' ' ||
                            tsu.person_middlename || ' (' || su.su_emp_num || ')' || '/' ||
                            pers.person_lastname || ' ' ||
                            pers.person_firstname || ' ' ||
                            pers.person_middlename || ' (' || sa.sa_emp_num || ')') seller_active_fio,
                     c.is_resident,
                     c.reg_deadline,
                     ts.sim_type sim_type,
                     dic_st.name sim_type_name,
                     ab2.attached_doc,
                     ab2.number_sheets,
                     ab2.equipment_ser usb_ser,
                     ab2.transfer_date wish_date,
                     ab2.agree_work_info,
                     ab2.agree_cancel_dog,
                     ab2.agree_pay_off_debt,
                     ab2.state_sbdpn,
                     ab2.transfer_phone,
                     ab2. operation_donor,
                     ab2. npid,
                     ab2. type_process_cbdpn,
                     mnp_st.name_state state_sbdpn_name,
                     ab2.type_req type_portation,
                     ab2.operator_recipient,
                     LSMStransStatus,
                     LSMSaddTransStatus,
                     nvl(lsms.full_name, LSMStransStatus) LSMStransStatus_name,
                     nvl(lsms_add.full_name, LSMSaddTransStatus) LSMSaddTransStatus_name,
                     description,
                     mnp_oper_don.name mnp_oper_don_name,
                     mnp_oper_don.operatorcode mnp_oper_don_code,
                     mnp_oper_rec.name mnp_oper_rec_name,
                     mnp_oper_rec.operatorcode mnp_oper_rec_code,
                     SMSNOTICEFLAG,
                     EMAILNOTICEFLAG,
                     ab2.date_create_cbdpn date_create_cbdpn,
                     ab2.correlationid,
                     ab2.COMMUN_DESTINATION,
                     ab2.reject_reason,
                     ab2.channel_id,
                     ab2.CODE_WORD,
                     ab2.OUT_ACCOUNT,
                     ab2.CREDIT_LIMIT,
                     ab2.ALARM_LIMIT,
                     tbb.IS_PERSONAL_ACCOUNT,
                     ab2.reg_id_ps,
                     ab2.usl_number,
                     ab2.kl_region_id,
                     ab2.fix_address_id,
                     ab2.fix_name,
                     ab2.ab_comment,
                     ab2.req_delivery_id,
                     ab2.request_id,
                     ab2.consent_msg,
                     null agr_num,
                     ab2.method_connect
                from T_ABON_BAD AB2
                left join t_clients c
                  on c.client_id = ab2.id_client
                left join t_juristic j
                  on j.juristic_id = c.fullinfo_id
                left join t_abon_bad_budget tbb
                  on tbb.bad_id = ab2.id
              -- 35948 (35953) Брендированное оборудование (телефоны)
                left join t_model_phone mp
                  on mp.model_id = ab2.phone_model_id
                left join t_bonus_phone bp
                  on bp.bonus_id = ab2.phone_bonus_id
              -- 39401
                left join t_phone_model_cost pmc
                  on mp.model_id = pmc.model_id
                 and sysdate between pmc.ver_date_beg and
                     nvl(pmc.ver_date_end, sysdate)
                 and pmc.region_id in
                     (select distinct o.region_id
                        from t_organizations o
                       where o.org_id = AB2.ID_ORG)
                left join t_modem_model_usb mmu
                  on ab2.equipment_model_id = mmu.id
                LEFT JOIN t_tarif_by_at_id tar
                  on tar.at_id = ab2.id_tar
                left join t_seller_active sa
                  on sa.sa_id = AB2.seller_active_id
                left join t_person pers
                  on sa.sa_person_id = pers.person_id
              -- 58777
                left join t_seller_active_rel sar
                  on sar.sa_id = sa.sa_id
                 and ab2.dog_date >= sar.date_from
                 and ab2.dog_date < nvl(sar.date_to, sysdate)
                left join t_supervisor su
                  on su.su_id = sar.su_id
                left join t_person tsu
                  on tsu.person_id = su.su_person_id
                left join t_tmc_sim ts
                  on nvl(ts.sim_imsi, ts.sim_iccid) = ab2.imsi
                left join t_dic_sim_type dic_st
                  on dic_st.id = ts.sim_type
                left join t_dic_mnp_state mnp_st
                  on mnp_st.id_state = ab2.state_sbdpn
                left join t_dic_mnp_operator mnp_oper_don
                  on mnp_oper_don.id = ab2.operation_donor
                left join t_dic_mnp_operator mnp_oper_rec
                  on mnp_oper_rec.id = ab2.operator_recipient
                left join T_DIC_MNP_LSMS_STATUS lsms
                  on lsms.alt_name = ab2.lsmstransstatus
                left join T_DIC_MNP_LSMS_ADD_STATUS lsms_add
                  on lsms_add.alt_name = ab2.lsmsaddtransstatus
               where AB2.IS_DELETED = 0
                 and AB2.ID = pi_ab_id) A,
             t_users pu,
             t_person pp
       where A.IS_BAD = pi_is_bad
         and A.ORG_ID is not null
         and not (A.ORG_ID = -1)
         and a.user_id = pu.usr_id(+)
         and pu.usr_person_id = pp.person_id(+);
    return res;

  exception
    when NO_DATA_FOUND then
      po_err_num := 1;
      po_err_msg := 'Абонент не найден (' || pi_ab_id || ')' ||
                    dbms_utility.format_error_backtrace;
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error('pi_ab_id=' || pi_ab_id || ' ' || po_err_msg,
                        c_package || 'get_abonent_by_id');
      return null;
  end Get_Abonent_By_Id2;
  ----------------------------------------------------------------------

  procedure Del_Abonent(pi_abonent_id in T_ABONENT.AB_ID%type,
                        pi_org_id     in T_ORGANIZATIONS.ORG_ID%type,
                        pi_worker_id  in T_USERS.USR_ID%type,
                        pi_is_bad     in number := 0,
                        po_err_num    out pls_integer,
                        po_err_msg    out t_Err_Msg) is
  begin
    -- Это чё за х-ня ???
    if (pi_is_bad = 0) then
      /*Del_Abonent_Normal(pi_abonent_id,
      pi_org_id,
      pi_worker_id,
      po_err_num,
      po_err_msg);*/
      null;

    else
      /*Del_Abonent_Bad(pi_abonent_id,
      pi_org_id,
      pi_worker_id,
      po_err_num,
      po_err_msg);*/
      null;
    end if;
  end Del_Abonent;

  ----------------------------------------------------------------------

  procedure Release_Tmc(pi_tmc_id    in T_ABONENT.AB_ID%type,
                        pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                        pi_worker_id in T_USERS.USR_ID%type,
                        po_err_num   out pls_integer,
                        po_err_msg   out t_Err_Msg) is
    cou     pls_integer := 0;
    l_op_id number;
  begin
    if (not Security_pkg.Check_Rights_str('EISSD.CONNECTIONS.GSM.REGISTER',
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then

      -- прав на создание подключения нет а значит и тмц освобождать нельзя
      return;
    end if;

    select count(*)
      into cou
      from t_org_tmc_status ots
     where ots.tmc_id = pi_tmc_id
       and ots.org_id = pi_org_id
       and status = constants_pkg.c_tmc_in_stock;

    if (cou = 0) then
      insert into t_org_tmc_status
        (tmc_id, org_id, status, income_date)
      values
        (pi_tmc_id, pi_org_id, constants_pkg.c_tmc_in_stock, sysdate);

      /*update t_tmc_sim
        set real_owner_id = pi_org_id
      where tmc_id =
            (select t.tmc_id from t_tmc t where t.tmc_id = pi_tmc_id);*/

      update t_tmc t set t.org_id = pi_org_id where t.tmc_id = pi_tmc_id;

      -- удаляем юнит и операцию
      begin
        select op.op_id
          into l_op_id
          from t_tmc_operation_units ou, t_tmc_operations op
         where ou.tmc_id = pi_tmc_id
           and ou.owner_id_1 = pi_org_id
           and op.op_id = ou.op_id
           and op.op_type = 22;

        delete from t_tmc_operation_units ou where ou.op_id = l_op_id;

        delete from t_tmc_operations o where o.op_id = l_op_id;

      exception
        when no_data_found then
          null;
      end;

    end if;

  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return;
  end Release_Tmc;

  ----------------------------------------------------------------------

  procedure Alter_Tmc(pi_tmc_id    in T_ABONENT.AB_ID%type,
                      pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                      pi_worker_id in T_USERS.USR_ID%type,
                      po_err_num   out pls_integer,
                      po_err_msg   out t_Err_Msg) is
  begin
    if (not Security_pkg.Check_Rights_str('EISSD.CONNECTIONS.GSM.REGISTER',
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then

      -- прав на создание подключения нет а значит и тмц занимать нельзя
      return;
    end if;

    delete from t_org_tmc_status
     where tmc_id = pi_tmc_id
       and org_id = pi_org_id;

    /*update t_tmc_sim
      set real_owner_id = null
    where tmc_id = (select t.tmc_id
                      from t_tmc t
                     where t.tmc_id = pi_tmc_id
                       and t.tmc_type = 8)
      and real_owner_id = pi_org_id;*/

    update t_tmc t
       set t.org_id = null
     where t.org_id = pi_org_id
       and t.tmc_id = pi_tmc_id;

  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return;
  end Alter_Tmc;

  -------------------------------- Cложить/Достать из архива --------------------------------
  procedure Mark_Archive(pi_abonent_id   in T_ABONENT.AB_ID%type,
                         pi_org_id       in T_ORGANIZATIONS.ORG_ID%type,
                         pi_worker_id    in T_USERS.USR_ID%type,
                         pi_archive_mark in number,
                         po_err_num      out pls_integer,
                         po_err_msg      out t_Err_Msg) is
  begin

    if (not Security_pkg.Check_Rights_str('EISSD.CONNECTIONS.GSM.SIGHT',
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      -- прав на визирование подключения нет, значит и архивировать нельзя
      return;
    end if;

    update T_ABONENT a
       set a.archive_mark = pi_archive_mark
     where a.ab_id = pi_abonent_id;

  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return;

  end Mark_Archive;
  -------------------- Возвращение данных по идентификатору сверки -----------------------

  function Get_Revise_By_Id(pi_revise_id in number,
                            pi_worker_id in number,
                            po_err_num   out pls_integer,
                            po_err_msg   out t_Err_Msg) return sys_refcursor is
    cur sys_refcursor;

  begin
    -- Проверка прав доступа
    if (not Security_pkg.Check_User_Right_str('EISSD.CONNECTIONS.SVERKY',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;

    open cur for
      select * from t_Revises r where r.rev_id = pi_revise_id;

    return cur;

  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;

  end Get_Revise_By_Id;
  -------------------- Таблица сверок -----------------------
  function Revise_T(pi_worker_id in number,
                    po_err_num   out number,
                    po_err_msg   out varchar2) return sys_refcursor is
    cur sys_refcursor;
  begin

    /*    -- Проверка прав доступа
    if (not Security_pkg.Check_User_Right2(5818,
                                       pi_worker_id,
                                       po_err_num,
                                       po_err_msg)) then
      return null;
    end if;*/

    open cur for
      select * from T_REVISES;

    return cur;

  end Revise_T;

  -------------------- Колличество совпавших подключений -----------------------

  /*function Identic_Subscribers return sys_refcursor is
      cur sys_refcursor;
    begin

      open cur for
        select * from T_IDENTIC T ORDER BY T.ID_DEALER;

      return cur;

    end Identic_Subscribers;
  */
  ------------------- Несовпавших подключения -----------------------

  /*function Mismatch_Subscribers return sys_refcursor is
      cur sys_refcursor;
    begin

      open cur for
        select * from T_MISMATCH T;

      return cur;

    end Mismatch_Subscribers;
  */
  -------------------- Возвращение совпадения по идентификатору сверки -----------------------
  function Get_Identic_By_Id(pi_identic_id in number,
                             pi_region_id  in number,
                             pi_worker_id  in number,
                             po_err_num    out pls_integer,
                             po_err_msg    out t_Err_Msg)
    return sys_refcursor is

    cur sys_refcursor;

  begin
    -- Проверка прав доступа
    if (not Security_pkg.Check_User_Right_str('EISSD.CONNECTIONS.SVERKY',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;

    open cur for
      select i.id_rev_id,
             i.id_tar_id,
             t.title,
             i.id_count,
             i.id_dealer,
             o.org_name

        from T_IDENTIC i, T_ORGANIZATIONS o, T_TARIFF2 t, T_ABSTRACT_TAR a
       where i.id_dealer = o.org_id
         and i.id_tar_id = a.at_remote_id
         and a.at_id = t.at_id
         and i.id_rev_id = pi_identic_id
         and ((o.region_id = pi_region_id and pi_region_id is not null) or
             pi_region_id is null)
       group by i.id_rev_id,
                i.id_tar_id,
                t.title,
                i.id_count,
                i.id_dealer,
                o.org_name;

    return cur;

  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;

  end Get_Identic_By_Id;

  ---------------------- Плата за подключения -----------------------
  function Activation_Fee(pi_ab_id    in T_ABONENT.AB_ID%type,
                          pi_pay_time in date,
                          pi_cost     in number,
                          po_msg      out varchar2) return number is
    l_accept_status number(3);
    l_num_adsl      number(11);
    l_pay_sub_id    T_PAY_SUBSCRIBES.PAY_SUB_ID%type;
    l_req_id        varchar2(128);
    l_pay_time      date := pi_pay_time;
    l_erkc_pay_id   number;
    l_accept_note   varchar2(2000);
  begin

    select ta.nsd
      into l_num_adsl --77590024627 номер широкополосного доступа ADSL
      from T_ABONENT a, T_TMC t, T_TMC_ADSL_CARD ta
     where a.ab_tmc_id = t.tmc_id
       and ta.tmc_id = t.tmc_id
       and a.ab_id = pi_ab_id;

    insert into T_PAY_SUBSCRIBES
      (PAY_DATE, COST, SUB_TYPE, FK_AB_ID)
    values
      (pi_pay_time, pi_cost, 2, pi_ab_id) -- тип подключения ADSL
    returning PAY_SUB_ID into l_pay_sub_id;

    l_req_id := to_char(l_pay_sub_id);

    if (pi_pay_time is null) then
      l_pay_time := sysdate;
    end if;

    l_erkc_pay_id := Erkc_Protocol_Out.Pay_Amount(l_req_id,
                                                  l_num_adsl,
                                                  l_pay_time,
                                                  pi_cost,

                                                  po_msg,
                                                  l_accept_status,
                                                  l_accept_note);

    update T_PAY_SUBSCRIBES p
       set p.remote_id   = l_erkc_pay_id,
           p.status      = l_accept_status,
           p.status_note = l_accept_note
     where p.pay_sub_id = l_pay_sub_id;

    commit;

    return l_accept_status;

  end Activation_Fee;

  --------------------------- вызывается джобой ---------------------------
  ------------- Статус платежа  - Платеж в «отложенной обработке» ---------
  procedure Check_Pay_Status is
    type pay_rec is record(
      pay_sub_id number,
      remote_id  number,
      pay_date   date);
    type pay_tab is table of pay_rec;
    pay_sub         pay_tab;
    l_accept_status number(3);
    l_accept_note   varchar2(2000);
    l_msg           varchar2(2000);
  begin
    savepoint sp_Check_Pay_Status;
    select p.pay_sub_id, p.remote_id, p.pay_date bulk collect
      into pay_sub
      from T_PAY_SUBSCRIBES p
     where p.status in (100, 102);

    if (sql%notfound) then
      rollback to sp_Check_Pay_Status;
      return;
    end if;

    for i in pay_sub.first .. pay_sub.last loop
      l_accept_status := Erkc_Protocol_Out.Get_Status_Pay(pay_sub(i)
                                                          .pay_sub_id,
                                                          pay_sub(i)
                                                          .remote_id,
                                                          pay_sub(i).pay_date,

                                                          l_accept_note,
                                                          l_msg);

      if (l_accept_status not in (100, 102)) then
        update T_PAY_SUBSCRIBES p
           set p.status = l_accept_status, p.reg_date = sysdate
         where p.pay_sub_id = pay_sub(i).pay_sub_id;
      end if;
    end loop;

    commit;

  end Check_Pay_Status;

  -------------------- Отчет о переходах состояний подключений ----------------------
  function Change_Status_Conn_Report(pi_date_from in date, -- [дата начала периода
                                     pi_date_to   in date) -- дата окончания периода}
   return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select ts.sim_callsign asc_ab_id,
             to_char(t.asc_ch_date, 'DD.MM.RRRR') asc_ch_date,
             to_char(t.asc_ch_date, 'HH24:MI:SS') asc_ch_time,
             (case t.asc_old_status
               when 0 then
                ''
               when 1 then
                'Удален'
               when 100 then
                'Ожидание (не готов)'
               when 101 then
                'Отклонено (ошибка)'
               when 102 then
                'Визирован (готов)'
               when 112 then
                'Ожидает ответа от АСР'
               when 103 then
                'Отклонен АСР (ошибка АСР)'
               when 104 then
                (case
                  when t.asc_old_status_ex = 105 then
                   'Установлен статус Одобрено АСР'
                  when t.asc_old_status_ex = 106 then
                   'Одобрен АСР. Ошибка доп. опций'
                  else
                   'Одобрен АСР'
                end)
               when 105 then
                'Установлен статус Одобрено АСР'
               when 106 then
                'Ожидает отправки в АСР (для юр. лиц)'
             end) asc_old_status,
             (case t.asc_new_status
               when 0 then
                ''
               when 1 then
                'Удален'
               when 100 then
                'Ожидание (не готов)'
               when 101 then
                'Отклонено (ошибка)'
               when 102 then
                'Визирован (готов)'
               when 112 then
                'Ожидает ответа от АСР'
               when 103 then
                'Отклонен АСР (ошибка АСР)'
               when 104 then
                (case
                  when t.asc_new_status_ex = 105 then
                   'Установлен статус Одобрено АСР'
                  when t.asc_new_status_ex = 106 then
                   'Одобрен АСР. Ошибка доп. опций'
                  else
                   'Одобрен АСР'
                end)
               when 105 then
                'Установлен статус Одобрено АСР'
               when 106 then
                'Ожидает отправки в АСР (для юр. лиц)'
             end) asc_new_status,
             1 kol,
             (case
               when t.asc_ch_date =
                    (select max(sc1.asc_ch_date)
                       from t_ab_status_change sc1
                      where sc1.asc_ab_id = t.asc_ab_id) then
                (case (select sc.asc_new_status
                     from t_ab_status_change sc
                    where sc.asc_ab_id = t.asc_ab_id
                      and sc.asc_ch_date =
                          (select max(tt.asc_ch_date)
                             from t_ab_status_change tt
                            where tt.asc_ab_id = t.asc_ab_id))
                  when 0 then
                   ''
                  when 1 then
                   'Удален'
                  when 100 then
                   'Ожидание (не готов)'
                  when 101 then
                   'Отклонено (ошибка)'
                  when 102 then
                   'Визирован (готов)'
                  when 112 then
                   'Ожидает ответа от АСР'
                  when 103 then
                   'Отклонен АСР (ошибка АСР)'
                  when 104 then
                   'Одобрен АСР'
                  when 105 then
                   'Установлен статус Одобрено АСР'
                  when 106 then
                   'Ожидает отправки в АСР (для юр. лиц)'
                end)
             end) last_status
        from t_ab_status_change t, t_abonent a, t_tmc tmc, t_tmc_sim ts
       where trunc(t.asc_ch_date) < pi_date_to
         and trunc(t.asc_ch_date) >= pi_date_from
         and t.asc_ab_id > 100000
         and t.asc_ab_id = a.ab_id
         and a.ab_tmc_id = tmc.tmc_id
         and tmc.tmc_type = 8
         and tmc.tmc_id = ts.tmc_id
       order by 1, 2, 3;

    return res;
  end Change_Status_Conn_Report;

  ----------------------------------------------------------------------------
  -- Проверка параметров подключения
  -- 0 - ошибка, 1 - все хорошо
  function Check_Abonent(pi_abonent_type in abonent_type,
                         pi_worker_id    in T_USERS.USR_ID%type,
                         -- 35948 (35953) Брендированное оборудование (телефоны)
                         pi_tmc_phone   in tmc_phone_tab,
                         pi_is_alien_ab in number, --признак,что абонент не усишный
                         po_errors      out varchar2,
                         po_err_num     out pls_integer,
                         po_err_msg     out t_Err_Msg) return number is
  
    type err_type is table of varchar2(1024);
    l_errs err_type := err_type('Не указана организация;', -- 1
                                'Не указана дата договора;', -- 2
                                'Тариф не был выбран из предложенного списка тарифов;', -- 3
                                'Не указан номер карты;', -- 4
                                'Не указан абонентский номер;', -- 5
                                'Не указан городской номер;', -- 6
                                'Не указан цвет;', -- 7
                                'Карта не найдена;', -- 8
                                'Номер карты не соответствует абонентскому номеру;', -- 9
                                'Городской номер не соответствует федеральному;', -- 10
                                'Цвет номера указан неверно;', -- 11
                                'Карта находится на складе другой организации;', -- 12
                                'Карта уже подключена, утеряна, бракована или в избытке;', -- 13
                                'Тарифный план карты не совпадает с выбранным;', -- 14
                                'Карта для введенного городского номера не найдена;', -- 15
                                'Подключение по выбранному тарифному плану закрыто;', -- 16
                                'Карта находится в процессе смены тарифного плана;', -- 17
                                'Абонентский номер не найден;', -- 18
                                'Абонентский номер уже занят (связан с другой SIM-картой);', -- 19
                                'SIM-карта уже занята (связана с другим абонентским номером);', -- 20
                                'Не указан расчетный центр для не связанной карты;', -- 21
                                'Выбранный телефонный номер не доступен для подключения в указанном РЦ;', -- 22
                                'Коммутатор телефонного номера не соответствует коммутатору SIM-карты;', -- 23
                                'Переход на данный тарифный план с Единого тарифного плана не предусмотрен;', -- 24
                                'Введенная стоимость не совпадает со стоимостью подключения;', -- 25
                                -- 29/03/2010 e.komissarov USB-модемы
                                'Для указанного тарифного плана не предусмотрена продажа оборудования;', -- 26
                                'Не указан тип оборудования;', -- 27 - если не передан параметр - тип оборудования
                                'Для указанного тарифного плана указан некорректный тип оборудования;', -- 28
                                'Оборудование указанного типа отсутствует на складе подключающей организации;', -- 29
                                'Введённая стоимость подключения с оборудованием данного типа не совпадает со стоимостью подключения;', -- 30
                                -- e.komissarov 04/05/2010
                                'Неверная длина абонентского номера;', -- 31
                                -- patrick 13.09.2010 № 22809
                                'Список тарифов при подключении Юр. лица ограничивается по признаку "Постпейд";', --32
                                'Не совпадают категории Юр. лица и тарифа;', -- 33
                                --16.11.2010 № 26301
                                'Регион подключающей организации и регион тарифного плана не совпадают;', -- 34
                                --08.07.2011 №32021(поправить чек_аб,чтоб он проверял назн. испол. (t_tmc.tmc_perm=5000))
                                'Не соответствует назначению использования ТМЦ;', -- 35
                                -- Задача № 35504
                                'Подключение на технологический тарифный план не предусмотрено;', -- 36
                                -- Брендированное оборудование
                                'Не выбрана модель телефона;', -- 37
                                -- 38023
                                'Не совпадают стоимость подключения и внесенная сумма', -- 38
                                'С данным тарифом должен быть выбран модем. Модемов на складе организации нет;', --39
                                'Выбранный расчетный центр не соответствует организации.', --40
                                'Нарушена уникальность серийного номера модема.', --41
                                'Коммутатор SIM-карты/позывного не соответствует региону тарифного плана.', --42
                                'У Вас недостаточно прав для корректировки тарифного плана', -- 43
                                'Для выбранного тарифного плана и типа номера нет стоимости подключения по тарифу', --44
                                'Смена тарифного плана возможна только с препейд на препейд и с постпейд на постпейд.' --45
                                );
  
    l_old_org_id         number;
    l_old_card_imsi      varchar2(21);
    l_old_card_iccid     varchar2(21);
    l_old_card_callsign  number;
    l_old_card_citynum   number;
    l_old_card_color     number;
    l_old_card_tar_id    number;
    l_old_card_type      number;
    l_equipment_required number;
    l_check_status       boolean := true;
    --
    l_check_city number;
    l_check_rel  number;
    --l_city_upd   number;
    --
    l_region number; --16.11.2010 № 26301
    --    l_simnum       varchar2(19) := trim(pi_simnum);
    l_simnum       varchar2(20) := trim(pi_abonent_type.imsi);
    l_isimsi       number := 0;
    l_sim_row      T_TMC_SIM%rowtype;
    l_callsign_row T_CALLSIGN%rowtype;
    l_count        number;
    l_status       T_ORG_TMC_STATUS.STATUS%type;
    l_card_owner   T_ORG_TMC_STATUS.ORG_ID%type;
    l_tech_at_id   T_TARIFF2.TECH_AT_ID%type;
    l_is_tech      T_TARIFF2.IS_TECH%type;
    l_tmc_id       number;
    l_cost         number;
    l_sum          number;
  
    l_type         varchar2(1);
    l_fullinfo     number;
    l_cl_category  number;
    l_tar_category number;
    l_str          varchar2(2000);
    l_tmc_perm     number;
    --48598
    l_is_related      t_tmc_sim.is_related%type;
    l_id_mrf          number;
    l_old_ab_status   number;
    l_org_reg_id      number;
    l_tariff_type_old number;
    l_tariff_type_new number;
  begin
  
    l_str := 'abonent_id: ' || pi_abonent_type.abonent_id || ',
                is_bad:' || pi_abonent_type.is_bad || ',
                client_id:' || pi_abonent_type.client_id || ',
                org_id:' || pi_abonent_type.org_id || ',
                phone_make:' || pi_abonent_type.phone_make || ',
                phone_model:' || pi_abonent_type.phone_model || ',
                phone_imei:' || pi_abonent_type.phone_imei || ',
                status:' || pi_abonent_type.status || ',
                paid:' || pi_abonent_type.paid || ',
                cost:' || pi_abonent_type.cost || ',
                tar_id:' || pi_abonent_type.tar_id || ',
                tar_name:' || pi_abonent_type.tar_name || ',
                imsi:' || pi_abonent_type.imsi || ',
                callsign:' || pi_abonent_type.callsign || ',
                citynum:' || pi_abonent_type.citynum || ',
                simcolor:' || pi_abonent_type.simcolor || ',
                err_code:' || pi_abonent_type.err_code || ',
                err_msg:' || pi_abonent_type.err_msg || ',
                            kl_region_id:' ||
             pi_abonent_type.kl_region_id || ',
                dog_date:' || pi_abonent_type.dog_date || ',
                user_id:' || pi_abonent_type.user_id || ',
                note:' || pi_abonent_type.note || ',
                related:' || pi_abonent_type.related || ',
                equipment_required:' ||
             pi_abonent_type.equipment_required || ',
                equipment_model_id:' ||
             pi_abonent_type.equipment_model_id || ',
              pi_abonent.reg_id_ps' ||
             pi_abonent_type.reg_id_ps || ',
                equipment_cost:' ||
             pi_abonent_type.equipment_cost || ',
                client_account:' ||
             pi_abonent_type.CLIENT_ACCOUNT || ',
                usb_ser:' || pi_abonent_type.usb_ser;
  
    logging_pkg.info(l_str, c_package || 'Check_Abonent');
    --|| 'pi_tmc_phone(1).model_id:'||pi_tmc_phone(1).model_id
  
    po_errors := '';
    ----- Проверяем заполнение обяз. полей
    --    if (pi_org_id is null or pi_org_id = -1) then
    if (pi_abonent_type.org_id is null or pi_abonent_type.org_id = -1) then
      po_errors := po_errors || l_errs(1);
    end if;
  
    if (pi_abonent_type.dog_date is null) then
      po_errors := po_errors || l_errs(2);
    end if;
  
    if (pi_abonent_type.tar_id is null or pi_abonent_type.tar_id = -1) then
      po_errors := po_errors || l_errs(3);
    end if;
  
    if (l_simnum is null) then
      po_errors := po_errors || l_errs(4);
    end if;
  
    -- e.komissarov 04/05/2010
    if pi_is_alien_ab = 0 then
      if (getvalue(pi_abonent_type.callsign) is null) then
        po_errors := po_errors || l_errs(5);
      else
        -- e.komissarov 04/05/2010
        if length(to_char(pi_abonent_type.callsign)) <> 11 then
          po_errors := po_errors || l_errs(31);
        end if;
      end if;
    end if;
    --50060
    -- проверяем соответствие расчетного центра организации выбранному значению
    if nvl(pi_abonent_type.ABONENT_id, 0) > 0 then
      select count(*)
        into l_count
        from t_org_calc_center occ
        join t_CALC_CENTER cc
          on cc.cc_is_ps = 1
         and cc.cc_id = occ.cc_id
        left join t_abonent a
          on a.cc_id = cc.cc_id
         and a.ab_id = pi_abonent_type.abonent_id
        left join t_abon_bad ab
          on ab.id_cc = cc.cc_id
         and ab.id = pi_abonent_type.abonent_id
       where occ.org_id = pi_abonent_type.org_id;
    else
      select count(*)
        into l_count
        from t_org_calc_center occ
        join t_CALC_CENTER cc
          on cc.cc_is_ps = 1
         and cc.cc_id = occ.cc_id
        join t_dic_region dr
          on cc.cc_region_id = dr.reg_id
        join t_dic_mvno_region m
          on m.reg_id = dr.reg_id
       where occ.org_id = pi_abonent_type.org_id
         and m.id = pi_abonent_type.reg_id_ps --dr.kl_region = pi_abonent.KL_REGION_ID
      ;
    end if;
    if (l_count = 0) then
      po_errors := po_errors || l_errs(40);
      logging_pkg.debug('pi_abonent.org_id=' || pi_abonent_type.org_id ||
                        ' pi_abonent.reg_id_ps' ||
                        pi_abonent_type.reg_id_ps,
                        'Check_Abonent');
    end if;
  
    if (pi_abonent_type.simcolor is null) then
      po_errors := po_errors || l_errs(7);
    end if;
    --поскольку галка связанности на вэбе лажает
    begin
      select (Case
               when ts.is_related is Null then
                0
               when ts.is_related = 2 then
                0
               else
                ts.is_related
             end)
        into l_is_related
        from t_tmc_sim ts
       where ts.sim_imsi = l_simnum
          or ts.sim_iccid = l_simnum;
    exception
      when no_data_found then
        po_errors := l_errs(8);
        return 0;
    end;
  
    /*if (pi_abonent_type.related = 0 and pi_abonent_type.cc_id is null) then
      po_errors := po_errors || l_errs(21);
    end if;*/
  
    --Если это юрик
    if pi_abonent_type.client_id is not null then
      select t.client_type, t.fullinfo_id
        into l_type, l_fullinfo
        from t_clients t
       where t.client_id = pi_abonent_type.client_id;
      if (l_type = 'J') then
        select t.jur_category
          into l_cl_category
          from t_juristic t
         where t.juristic_id = l_fullinfo;
        select decode(t.pay_type, 71, 1, 2)
          into l_tar_category
          from t_tarif_by_at_id t
         where t.at_id = pi_abonent_type.tar_id;
        if l_cl_category <> l_tar_category then
          po_errors := po_errors || l_errs(33);
        end if;
      end if;
    else
      l_type := pi_abonent_type.client_type;
    end if;
  
    -- patrick 13.09.2010
    /*   if pi_abonent_type.client_id is not null then
     select tc.client_type into l_client_type
      from t_clients tc where tc.client_id = pi_abonent_type.client_id;
    
     select t2.tariff_type into l_tar_type
      from t_tariff2 t2 where t2.id = get_last_tar_id_by_at_id(pi_abonent_type.tar_id);
    
     if (l_client_type = 'J' and l_tar_type <> 62) then
      po_errors := po_errors || l_errs(32);
     end if;
    end if; */
    -------------------------------------
    if (po_errors is not null) then
      return 0;
    end if;
  
    -- Задача № 35504
    -- проверяем, что тп не технологический
    select t2.is_tech
      into l_is_tech
      from t_tariff2 t2
     where t2.id = get_last_tar_id_by_at_id(pi_abonent_type.tar_id);
    if l_is_tech = 1 then
      po_errors := po_errors || l_errs(36);
    
      return 0;
    end if;
  
    --119642 Проверка что указанный цвет есть в тарифе
    select count(*)
      into l_count
      from t_serv_tar2 s
     where s.phone_color = pi_abonent_type.simcolor
       and s.tar_id = get_last_tar_id_by_at_id(pi_abonent_type.tar_id);
  
    if l_count = 0 then
      po_errors := po_errors || l_errs(44);
    
      return 0;
    end if;
  
    --16.11.2010 № 26301
    begin
      --55779
      /*select org.region_id
       into l_region
       from t_organizations org
      where org.org_id = pi_abonent_type.org_id;*/
      with tab as
       (select tor.org_id
          from t_org_relations tor
          join t_organizations o
            on o.org_id = tor.org_id
        connect by prior tor.org_pid = tor.org_id
         start with tor.org_id in (pi_abonent_type.org_id))
      select max(dmr.id), max(nvl(t.id_mrf, 0))
        into l_region, l_id_mrf
        from (select al.*, r.kl_name, r.reg_id, null name_mrf, null id_mrf
                from (tab) al
                join t_dic_region r
                  on r.org_id = al.org_id
                 and r.reg_id not in (91, 92)
              union
              select al.*, null, null, m.name_mrf, m.id id_mrf
                from (tab) al
                join t_dic_mrf m
                  on m.org_id = al.org_id
              union
              select al.*, r.kl_name, dr.reg_id, null name_mrf, null id_mrf
                from (tab) al
                join t_dic_region_info dr
                  on dr.rtmob_org_id = al.org_id
                join t_dic_region r
                  on r.reg_id = dr.reg_id
                 and r.reg_id not in (91, 92)) t
        join t_dic_mvno_region dmr
          on dmr.reg_id = t.reg_id
         and (pi_abonent_type.reg_id_ps is null or
             dmr.id = pi_abonent_type.reg_id_ps);
    
      select distinct tar.at_region_id
        into l_region
        from t_tarif_by_at_id tar
       where (tar.at_id = pi_abonent_type.tar_id or
             pi_abonent_type.tar_id is null)
         and tar.at_region_id = l_region;
    exception
      when no_data_found then
        po_errors := po_errors || l_errs(34);
    end;
    -- определяем тип номера карты (IMSI, ICCID)
    l_isimsi := Util.Is_Card_Num_Imsi(l_simnum);
  
    --08.07.2011 №32021
    begin
      select t.tmc_perm
        into l_tmc_perm
        from t_tmc_sim ts
        join t_tmc t
          on t.tmc_id = ts.tmc_id
       where (l_isimsi = 1 and ts.sim_imsi = l_simnum or
             l_isimsi = 0 and ts.sim_iccid = l_simnum);
    exception
      when no_data_found then
        po_errors := l_errs(8);
        return 0;
    end;
  
    if nvl(l_tmc_perm, 5000) <> 5000 then
      po_errors := po_errors || l_errs(35);
    end if;
  
    -- Малышевский 38023
    if nvl(pi_abonent_type.cost, 0) +
       nvl(pi_abonent_type.equipment_cost, 0) <>
       nvl(pi_abonent_type.paid, 0) /*and nvl(pi_is_alien_ab, 0) = 0*/
     then
      po_errors := po_errors || l_errs(38);
      return 0;
    end if;
  
    if (pi_abonent_type.abonent_id is not null and
       pi_abonent_type.abonent_id <> -1 and pi_abonent_type.is_bad = 0) then
      -- Если зашли сюда, значит редактируем валидное подключение
      -- Проверяем изменилась ли организация или параметры сим-карты
      begin
        select nvl(s.sim_imsi, r.ruim_imsi),
               s.sim_iccid,
               nvl(s.sim_callsign, r.ruim_callsign),
               s.sim_callsign_city,
               s.sim_color,
               nvl(s.tar_id, r.tar_id),
               t.tmc_type,
               a.org_id,
               a.ab_status
          into l_old_card_imsi,
               l_old_card_iccid,
               l_old_card_callsign,
               l_old_card_citynum,
               l_old_card_color,
               l_old_card_tar_id,
               l_old_card_type,
               l_old_org_id,
               l_old_ab_status
          from t_abonent a
          join t_tmc t
            on a.ab_tmc_id = t.tmc_id
          left join t_tmc_sim s
            on t.tmc_type = 8
           and t.tmc_id = s.tmc_id
          left join t_tmc_ruim r
            on t.tmc_type = 9
           and t.tmc_id = r.tmc_id
         where a.ab_id = pi_abonent_type.abonent_id
           and a.is_deleted = 0;
      exception
        when no_data_found then
          logging_pkg.error(pi_message  => 'Ошибка в процедуре CheckAbonent. Абонент не найден. ' ||
                                           l_str,
                            pi_location => c_package || 'Check_Abonent');
          logging_pkg.Raise_App_Err(-1000,
                                    'Ошибка в процедуре CheckAbonent. Абонент не найден.');
      end;
    
      -- Проверка, что по подключению в статусе "Ошибка АСР" не изменена организация
      if l_old_ab_status = 103 and l_old_org_id <> pi_abonent_type.org_id then
        po_errors := po_errors || l_errs(12);
        return 0;
      end if;
    
      if (not ((l_isimsi = 1 and l_old_card_imsi <> l_simnum) or
          (l_isimsi = 0 and l_old_card_iccid <> l_simnum) or
          l_old_card_callsign <> pi_abonent_type.callsign or
          l_old_card_tar_id <> pi_abonent_type.tar_id or
          l_old_org_id <> pi_abonent_type.org_id or
          (l_old_card_type = 51 and
          (l_old_card_citynum is not null and
          pi_abonent_type.citynum is null or
          l_old_card_citynum is null and
          pi_abonent_type.citynum is not null or
          l_old_card_citynum <> pi_abonent_type.citynum) or
          l_old_card_color <> pi_abonent_type.simcolor OR
          l_old_org_id <> pi_abonent_type.org_id))) then
        return 1; -- данные карты не изменились при редактировании - проверять не нужно
      elsif (l_old_card_callsign = pi_abonent_type.callsign and
            l_old_org_id = pi_abonent_type.org_id) then
        l_check_status := false; -- карта и организация таже, поэтому не проверяем складской статус
      end if;
    
    else
      -- Проверка, что по подключению в статусе "Ошибка" не изменена организация
      if pi_abonent_type.is_bad = 1 then
        select ab.id_org
          into l_old_org_id
          from t_abon_bad ab
         where ab.id = pi_abonent_type.abonent_id;
        if l_old_org_id <> pi_abonent_type.org_id then
          po_errors := po_errors || l_errs(12);
          return 0;
        end if;
      end if;
      if pi_is_alien_ab = 0 or
         (pi_is_alien_ab = 1 and pi_abonent_type.callsign is not null) then
        select decode(pi_abonent_type.citynum, null, 9001, 9002)
          into l_old_card_type
          from dual;
        --l_old_card_color := get_color(pi_abonent_type.callsign, 7);
        l_old_card_color := tmc_sim.get_color_sim(pi_abonent_type.callsign,
                                                  po_err_num,
                                                  po_err_msg);
      end if;
    end if;
    --48598--подтягиваем признак связанности из симки а не с вэба
    if (l_is_related = 1 /*pi_abonent_type.related = 1*/
       ) then
      -- связанная карта
    
      -- Проверяем наличие карты в системе
      begin
        select s.*
          into l_sim_row
          from t_tmc_sim s
          join t_callsign c
            on c.tmc_id = s.callsign_id
         where ((c.federal_callsign = pi_abonent_type.callsign) and
               (l_isimsi = 1 and s.sim_imsi = l_simnum or
               l_isimsi = 0 and s.sim_iccid = l_simnum))
           and rownum <= 1;
      exception
        when no_data_found then
          po_errors := l_errs(8);
          return 0;
      end;
    
      -- проверяем соответствие аб. номера номеру карты
      if (l_isimsi = 1 and l_simnum <> l_sim_row.sim_imsi or
         l_isimsi = 0 and
         (l_sim_row.sim_iccid is null or l_simnum <> l_sim_row.sim_iccid) or
         pi_abonent_type.callsign <> l_sim_row.sim_callsign) then
        po_errors := po_errors || l_errs(9);
        return 0;
      end if;
    
      -- проверяем городской номер
      if (pi_abonent_type.citynum is null and
         l_sim_row.sim_callsign_city is not null) then
        po_errors := po_errors || l_errs(6);
      elsif (pi_abonent_type.citynum is not null and
            l_sim_row.sim_callsign_city is null) then
        po_errors := po_errors || l_errs(15);
      elsif (l_sim_row.sim_callsign_city is not null and
            pi_abonent_type.citynum is not null and
            l_sim_row.sim_callsign_city <> pi_abonent_type.citynum) then
        po_errors := po_errors || l_errs(10);
      end if;
    
      -- проверяем цвет
      if (pi_abonent_type.simcolor <> l_sim_row.sim_color) then
        po_errors := po_errors || l_errs(11);
      end if;
    
      -- проверяем соответствие ТП
      if (pi_abonent_type.tar_id <> l_sim_row.tar_id) then
        if pi_abonent_type.client_type = 'J' then
          -- Если ТП не совпадает, то возможно
          -- Корректировка ТП при продаже MVNO
          -- Возможна только у физиков для связанных сим
          po_errors := po_errors || l_errs(14);
        else
          -- Проверяем наличие права на корректировку ТП
          if (not Security_pkg.Check_Rights_str('EISSD.CONNECTIONS.GSM.CHANGE_TP',
                                                pi_abonent_type.org_id,
                                                pi_worker_id,
                                                po_err_num,
                                                po_err_msg)) then
            po_errors  := po_errors || l_errs(43);
            po_err_num := null;
            po_err_msg := null;
          end if;
        end if;
        /*-- подключаются по ТП "Единый" -- больше нет Единого ТП
        select t2.is_tech
          into l_is_tech
          from t_tariff2 t2
         where t2.id = get_last_tar_id_by_at_id(l_sim_row.tar_id);
        if (l_is_tech = 0) then
          po_errors := po_errors || l_errs(14);
        else
          -- проверим возможность перехода с Единого ТП на указанный ТП
          select t2.tech_at_id
            into l_tech_at_id
            from t_tariff2 t2
           where t2.id = get_last_tar_id_by_at_id(pi_abonent_type.tar_id);
          -- e.komissarov 07/08/2009 Должны быть выполнены оба условия одновременно
          if (l_tech_at_id is null or l_tech_at_id <> l_sim_row.tar_id) then
            po_errors := po_errors || l_errs(24);
          else
            -- проверяем актуальность ТП, на который осуществляется переход
            if (get_tar_id_by_at_id(pi_abonent_type.tar_id,
                                    pi_abonent_type.dog_date) is null) then
              po_errors := po_errors || l_errs(16);
            end if;
          end if;
        end if;*/
      end if;
      -- проверяем актуальность ТП
      if (get_tar_id_by_at_id(pi_abonent_type.tar_id,
                              pi_abonent_type.dog_date) is null) then
        po_errors := po_errors || l_errs(16);
      end if;
    
      --Neverov 31.08.2010
      --Проверяем стоимость подключения.
      if pi_abonent_type.cost is not null then
        l_cost := get_tmc_cost_by_color(pi_abonent_type.tar_id,
                                        300,
                                        sysdate,
                                        1,
                                        l_old_card_type,
                                        l_old_card_color,
                                        l_type);
      
        logging_pkg.info('tar_cost=' || l_cost || ' cost=' ||
                         pi_abonent_type.cost / 100 || ' card_type=' ||
                         l_old_card_type || ' card_color=' ||
                         l_old_card_color || ' callsign=' ||
                         pi_abonent_type.callsign || ' citynum=' ||
                         pi_abonent_type.citynum,
                         c_package || 'Check_Abonent');
      
        if l_cost <> pi_abonent_type.cost / 100 and
           pi_abonent_type.equipment_required = 0 then
          --Делить на 100 ОБЯЗАТЕЛЬНО!
          logging_pkg.error('Cost error. ' || l_str || ',
                            tar_cost: ' ||
                            l_cost || ',
                            ab_paid: ' ||
                            pi_abonent_type.paid || ',
                            ab_cost: ' ||
                            pi_abonent_type.cost || ',
                            old_card_type: ' ||
                            l_old_card_type || ',
                            old_card_color: ' ||
                            l_old_card_color,
                            c_package || 'Check_Abonent');
          po_errors := l_errs(25);
          return 0;
        end if;
      end if;
    
      if (l_check_status) then
        -- проверяем наличие заявки на смену ТП у подключаемой карты
        select count(*)
          into l_count
          from t_sim_change_tar sct
         where sct.sim_id = l_sim_row.tmc_id
           and sct.asr_sync_status in (6500);
      
        if (l_count > 0) then
          po_errors := po_errors || l_errs(17);
        end if;
      
        -- patrick 30.06.2010 если мы правим подключение, то не надо проверять наличие сим-карты,
        -- НО только в том случае, если сим-карта не меняется.
      
        -- Neveroff 28.12.2010 тут реализовано только для валидных подключений. Поэтому добавил в условие is_bad
        -- иначе вываливалось будто карта находится на складе другой организации (Из валидных бралось подключение
        -- по ИД не валидного)
        if pi_abonent_type.abonent_id is not null and
           pi_abonent_type.is_bad = 0 then
          begin
            select nvl(s.sim_imsi, r.ruim_imsi)
              into l_old_card_imsi
              from t_abonent a
              join t_tmc t
                on a.ab_tmc_id = t.tmc_id
              left join t_tmc_sim s
                on t.tmc_type = 8
               and t.tmc_id = s.tmc_id
              left join t_tmc_ruim r
                on t.tmc_type = 9
               and t.tmc_id = r.tmc_id
             where a.ab_id = pi_abonent_type.abonent_id
               and a.is_deleted = 0;
          exception
            when no_data_found then
              l_old_card_imsi := '-1';
          end;
        end if;
      
        if pi_abonent_type.abonent_id is null or
           (pi_abonent_type.imsi <> l_old_card_imsi) then
          -- Проверяем статус карты на складе,
          -- а также владельца
          begin
            select ots.status, ots.org_id
              into l_status, l_card_owner
              from t_org_tmc_status ots, t_tmc t
             where t.tmc_id = l_sim_row.tmc_id
                  -- e.komissarov 06/07/2009 нужно учесть тип ТМЦ
               and t.tmc_type = constants_pkg.c_tmc_sim_id
               and ots.tmc_id = t.tmc_id;
            if (l_status <> 11) then
              po_errors := po_errors || l_errs(13);
            end if;
            if (pi_abonent_type.org_id <> l_card_owner) then
              po_errors := po_errors || l_errs(12);
            end if;
          exception
            when no_data_found then
              po_errors := po_errors || l_errs(13);
          end;
        end if; -- patrick
      
      end if;
    else
      -- не связанная карта
      -- проверяем наличие телефонного номера
      if pi_is_alien_ab = 0 or
         (pi_is_alien_ab = 1 and pi_abonent_type.callsign is not null) then
        begin
          select *
            into l_callsign_row
            from t_callsign c
           where c.federal_callsign = pi_abonent_type.callsign
             and rownum <= 1;
        exception
          when no_data_found then
            po_errors := l_errs(18);
            return 0;
        end;
      
        -- проверяем цвет
        if (pi_abonent_type.simcolor <> l_callsign_row.color) then
          po_errors := po_errors || l_errs(11);
        end if;
      
        -- если у телефонного номера прописан коммутатор,
        -- то проверим, может ли организация подключать
        -- к данному коммутатору
        if (l_callsign_row.id_comm is not null) then
          select o.region_id
            into l_org_reg_id
            from t_organizations o
           where o.org_id = pi_abonent_type.org_id;
          select count(*)
            into l_count
            from t_commut_cc ccc
           where ccc.id_comm = l_callsign_row.id_comm
             and ccc.id_cc =
                 get_ccid_by_klregionid(pi_abonent_type.reg_id_ps,
                                        l_org_reg_id);
        
          if (l_count = 0) then
            po_errors := l_errs(22);
            return 0;
          end if;
        
          select count(*)
            into l_count
            from t_commutators c
            left join t_tarif_by_at_id t
              on t.at_id = pi_abonent_type.tar_id
           where c.id_comm = l_callsign_row.id_comm
             and c.id_region = t.at_region_id;
        
          if (l_count = 0) then
            po_errors := l_errs(42);
            return 0;
          end if;
        
        end if;
      
        -- проверяем, не занят ли номер
        if (l_callsign_row.is_related <> 0 and
           pi_abonent_type.abonent_id is null) then
          po_errors := l_errs(19);
          return 0;
        end if;
      
        -- проверяем городской номер
        if (pi_abonent_type.citynum is null and
           l_callsign_row.callsign_city_id is not null) then
          po_errors := po_errors || l_errs(6);
        elsif (pi_abonent_type.citynum is not null and
              (l_callsign_row.callsign_city_id is null and
              l_callsign_row.city_callsign is null)) then
          begin
            select t.is_related, t.tmc_id
              into l_check_rel, l_check_city
              from t_callsign_city tc
              left join t_callsign t
                on tc.tmc_id = t.callsign_city_id
             where tc.CALLSIGN_CITY = pi_abonent_type.citynum;
            if l_check_city is not null and l_check_rel = 1 then
              po_errors := po_errors || l_errs(15);
              /*          elsif l_check_city is null then
                          l_city_upd := 1;
                        else
                          l_city_upd := 2;
              */
            end if;
          exception
            when no_data_found then
              po_errors := l_errs(15);
              return 0;
          end;
        elsif (l_callsign_row.tmc_id is not null and
              pi_abonent_type.citynum is not null and
              l_callsign_row.city_callsign <> pi_abonent_type.citynum) then
          begin
            select t.is_related, t.tmc_id
              into l_check_rel, l_check_city
              from t_callsign_city tc
              left join t_callsign t
                on tc.tmc_id = t.callsign_city_id
             where tc.CALLSIGN_CITY = pi_abonent_type.citynum;
            if l_check_city is not null and l_check_rel = 1 then
              po_errors := po_errors || l_errs(10);
              /*         elsif l_check_city is null then
                l_city_upd := 3;
              else
                l_city_upd := 4;*/
            end if;
          exception
            when no_data_found then
              po_errors := l_errs(15);
              return 0;
          end;
          --        po_errors := po_errors || l_errs(10);
        end if;
      end if;
      -- проверяем наличие сим-карты в системе
      begin
        select *
          into l_sim_row
          from t_tmc_sim s
         where (l_isimsi = 1 and s.sim_imsi = l_simnum or
               l_isimsi = 0 and s.sim_iccid = l_simnum)
           and rownum <= 1;
      exception
        when no_data_found then
          po_errors := l_errs(8);
          return 0;
      end;
    
      -- проверяем связанность карты
      if (l_sim_row.is_related = 1) then
        po_errors := l_errs(20);
        return 0;
      end if;
      if pi_is_alien_ab = 0 or
         (pi_is_alien_ab = 1 and pi_abonent_type.callsign is not null) then
        -- проверим соответствие коммутатора у карты с коммутатором телефонного номера
        if ((l_callsign_row.id_comm is null and
           l_sim_row.id_comm is not null) or (l_callsign_row.id_comm is not null and
           l_sim_row.id_comm is null) or
           l_callsign_row.id_comm <> l_sim_row.id_comm) then
          po_errors := l_errs(23);
          return 0;
        end if;
      end if;
      -- patrick 30.06.2010 если мы правим подключение, то не надо проверять наличие сим-карты,
      -- НО только в том случае, если сим-карта не меняется.
      if pi_abonent_type.abonent_id is not null then
        begin
          select nvl(s.sim_imsi, r.ruim_imsi)
            into l_old_card_imsi
            from t_abonent a
            join t_tmc t
              on a.ab_tmc_id = t.tmc_id
            left join t_tmc_sim s
              on t.tmc_type = 8
             and t.tmc_id = s.tmc_id
            left join t_tmc_ruim r
              on t.tmc_type = 9
             and t.tmc_id = r.tmc_id
           where a.ab_id = pi_abonent_type.abonent_id
             and a.is_deleted = 0;
        exception
          when no_data_found then
            l_old_card_imsi := '-1';
        end;
      end if;
    
      if pi_abonent_type.abonent_id is null or
         (pi_abonent_type.imsi <> l_old_card_imsi) then
        -- Проверяем статус карты на складе,
        -- а также владельца
        begin
          select ots.status, ots.org_id
            into l_status, l_card_owner
            from t_org_tmc_status ots, t_tmc t
           where t.tmc_id = l_sim_row.tmc_id
                -- e.komissarov 06/07/2009 нужно учесть тип ТМЦ
             and t.tmc_type = constants_pkg.c_tmc_sim_id
             and ots.tmc_id = t.tmc_id;
          if (l_status <> 11) then
            po_errors := po_errors || l_errs(13);
          end if;
          if (pi_abonent_type.org_id <> l_card_owner) then
            po_errors := po_errors || l_errs(12);
          end if;
        exception
          when no_data_found then
            po_errors := po_errors || l_errs(13);
        end;
      end if; -- patrick
    
      -- Проверяем стоимость подключения
      if pi_abonent_type.cost is not null then
        l_cost := get_tmc_cost_by_color(pi_abonent_type.tar_id,
                                        300,
                                        sysdate,
                                        1,
                                        l_old_card_type,
                                        l_old_card_color,
                                        l_type);
      
        logging_pkg.info('tar_cost=' || l_cost || ' cost=' ||
                         pi_abonent_type.cost / 100 || ' card_type=' ||
                         l_old_card_type || ' card_color=' ||
                         l_old_card_color || ' callsign=' ||
                         pi_abonent_type.callsign || ' citynum=' ||
                         pi_abonent_type.citynum,
                         /*c_package ||*/
                         'Check_Abonent');
      
        if l_cost <> pi_abonent_type.cost / 100 and
           pi_abonent_type.equipment_required = 0 then
          --Делить на 100 ОБЯЗАТЕЛЬНО!
          logging_pkg.error('Cost error. ' || l_str || ',

                        tar_cost: ' || l_cost || ',
                        old_card_type: ' ||
                            l_old_card_type || ',
                        old_card_color: ' ||
                            l_old_card_color,
                            c_package || 'Check_Abonent');
          po_errors := l_errs(25);
          return 0;
        end if;
      end if;
    
    end if;
    --46006
    if nvl(pi_abonent_type.equipment_required, 0) = 0 then
      select t.equipment_required
        into l_equipment_required
        from t_tariff2 t
       where t.id = get_last_tar_id_by_at_id(pi_abonent_type.tar_id);
      if nvl(l_equipment_required, 0) = -4 then
        po_errors := po_errors || l_errs(39);
      end if;
    end if;
    --ДЛЯ МОДЕМОВ!!!
    -- 35948 (35953) Брендированное оборудование (телефоны) - добавлен case
    if pi_abonent_type.equipment_required in (4, 7003) then
      case pi_abonent_type.equipment_required
        when 4 then
          -- Проверяем, нет ли на такой серийник подключения
          select count(*)
            into l_count
            from t_tmc_modem_usb tmu
           where tmu.usb_ser = pi_abonent_type.usb_ser;
          if l_count <> 0 then
            po_errors := po_errors || l_errs(41);
          end if;
        
          select abs(t.equipment_required)
            into l_equipment_required
            from t_tariff2 t
           where t.id = get_last_tar_id_by_at_id(pi_abonent_type.tar_id);
        
          --Если в тарифе оборудование не указано, а в подключении указано
          if (l_equipment_required = 0 and
             pi_abonent_type.equipment_required > 0) then
            po_errors := po_errors || l_errs(26);
          end if;
          --Если в тарифе оборудование указано, а в подключении не указано
          if (
             
             /*l_equipment_required <> 0 and
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          (pi_abonent_type.equipment_required is null or
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          pi_abonent_type.equipment_required = 0 or
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          pi_abonent_type.equipment_required = -1)*/
             
              pi_abonent_type.equipment_required > 0 and
              (pi_abonent_type.equipment_model_id is null or
              pi_abonent_type.equipment_model_id = -1)) then
          
            po_errors := po_errors || l_errs(27);
          end if;
          --Если типы оборудования не сопадают
          if (l_equipment_required <> pi_abonent_type.equipment_required and
             pi_abonent_type.equipment_required > 0) then
            po_errors := po_errors || l_errs(28);
          end if;
          --Если указан модем...
          --if (pi_abonent_type.equipment_required = 4) then
          --...Получаем ТМЦ со склада...
          if pi_abonent_type.equipment_tmc_id is null then
            l_tmc_id := usb_modems.get_first_modem_by_model(pi_abonent_type.equipment_model_id,
                                                            pi_abonent_type.org_id,
                                                            pi_worker_id);
          else
            select max(t.tmc_id)
              into l_tmc_id
              from t_tmc t
              join t_tmc_modem_usb tu
                on t.tmc_id = tu.tmc_id
              join t_org_tmc_status ot
                on t.tmc_id = ot.tmc_id
              left join t_tmc_reservation re
                on re.tmc_id = t.tmc_id
             where t.is_deleted = 0
               and ot.org_id = pi_abonent_type.org_id
               and tu.usb_model = pi_abonent_type.equipment_model_id
               and (re.tmc_id is null or re.date_end < sysdate)
               and t.tmc_type = 4
               and ot.status = 11
               and t.tmc_id = pi_abonent_type.equipment_tmc_id;
          end if;
          logging_pkg.info('check_abonent l_tmc_id = ' || l_tmc_id ||
                           ' equipment = ' ||
                           pi_abonent_type.equipment_required ||
                           ' model = ' ||
                           pi_abonent_type.equipment_model_id ||
                           ' serial_number = ' || pi_abonent_type.usb_ser,
                           'check_abonent');
        
          if (NVL(l_tmc_id, 0) = 0) then
            --Если не находим
            po_errors := po_errors || l_errs(29);
          else
          
            l_cost := get_tmc_cost_by_color(pi_abonent_type.tar_id,
                                            300,
                                            sysdate,
                                            1,
                                            l_old_card_type,
                                            l_old_card_color,
                                            l_type);
          
            select /*NVL(t.tmc_tmp_cost, 0) +*/
             NVL(tum.usb_full_cost, 0)
              into l_sum
              from t_tmc_modem_usb tum, t_tmc t, t_org_tmc_status ots
            
             where tum.tmc_id = t.tmc_id
               and ots.tmc_id = t.tmc_id
               and ots.status = 11
               and t.tmc_type = 4
                  /*and tum.real_owner_id = pi_abonent_type.org_id*/
               and t.org_id = pi_abonent_type.org_id
               and t.tmc_id = l_tmc_id
               and tum.usb_model = pi_abonent_type.equipment_model_id;
          
            /*        logging_pkg.info('l_cost = '||l_cost||'
                               l_sum = '||l_sum||'
                               paid = '||pi_abonent_type.paid||'
                               s_cost = '||(nvl(l_cost,0)/100) + l_sum,
                               'check_abonent');
            */
            if (l_cost + l_sum <> pi_abonent_type.paid / 100) then
              po_errors := po_errors || l_errs(30);
            end if;
          end if;
        
        when 7003 then
          -- Проверяем, нет ли на такой серийник подключения
          select count(*)
            into l_count
            from t_tmc_phone tp
           where tp.serial_number = pi_tmc_phone(1).serial_number;
          if l_count <> 0 then
            po_errors := po_errors || l_errs(41);
          end if;
          -- Проверяем, есть ли нужный телефон на складе
          if pi_tmc_phone is null then
            po_errors := po_errors || l_errs(37);
            logging_pkg.info('l_tmc_id = ' || l_tmc_id || ' equipment = ' ||
                             pi_abonent_type.equipment_required ||
                             ' model = null1',
                             'check_abonent');
          else
            if pi_tmc_phone.count = 0 then
              po_errors := po_errors || l_errs(37);
              logging_pkg.info('l_tmc_id = ' || l_tmc_id ||
                               ' equipment = ' ||
                               pi_abonent_type.equipment_required ||
                               ' model = null2',
                               'check_abonent');
            else
              if pi_tmc_phone(1).tmc_id is null then
                -- Получаем ИД ТМЦ телефона
                -- Проверяем, есть ли нужный телефон на складе
                l_tmc_id := tmc_phones.get_first_phone_by_model(pi_tmc_phone(1)
                                                                .model_id,
                                                                pi_abonent_type.org_id,
                                                                pi_worker_id);
              else
                select max(t.tmc_id)
                  into l_tmc_id
                  from t_tmc t, t_tmc_phone tp, t_org_tmc_status ot
                 where t.tmc_id = tp.tmc_id
                   and t.tmc_id = ot.tmc_id
                   and t.is_deleted = 0
                   and ot.org_id = pi_abonent_type.org_id
                   and tp.model_id = pi_tmc_phone(1).model_id
                   and t.tmc_type = 7003
                   and ot.status = 11
                   and t.tmc_id = pi_tmc_phone(1).tmc_id;
              end if;
            
              logging_pkg.info('l_tmc_id = ' || l_tmc_id ||
                               ' equipment = ' ||
                               pi_abonent_type.equipment_required ||
                               ' model = ' || pi_tmc_phone(1).model_id ||
                               ' serial_number = ' || pi_tmc_phone(1)
                               .serial_number,
                               'check_abonent');
            
              if (NVL(l_tmc_id, 0) = 0) then
                --Если не находим
                po_errors := po_errors || l_errs(29);
              else
                l_cost := get_tmc_cost_by_color(pi_abonent_type.tar_id,
                                                300,
                                                sysdate,
                                                1,
                                                l_old_card_type,
                                                l_old_card_color,
                                                l_type);
              
                select NVL(pmc.cost_with_nds, 0)
                  into l_sum
                  from t_tmc_phone      tp,
                       t_tmc            t,
                       t_org_tmc_status ots,
                       t_model_phone    mp,
                       -- 39401
                       t_phone_model_cost pmc
                 where tp.tmc_id = t.tmc_id
                   and ots.tmc_id = t.tmc_id
                   and ots.status = 11
                   and t.tmc_type = 7003
                   and t.org_id = pi_abonent_type.org_id
                      
                   and t.tmc_id = l_tmc_id
                   and tp.model_id = pi_tmc_phone(1).model_id
                   and tp.model_id = mp.model_id
                      -- 39401
                   and mp.model_id = pmc.model_id
                   and sysdate between pmc.ver_date_beg and
                       nvl(pmc.ver_date_end, sysdate)
                   and pmc.region_id in
                       (select distinct o.region_id
                        
                          from t_organizations o
                         where o.org_id = t.org_id);
              
                if (l_cost + l_sum <> pi_abonent_type.paid / 100) then
                  po_errors := po_errors || l_errs(30);
                end if;
              end if;
            end if;
          end if;
        else
          null;
      end case;
    end if;
  
    -- Проверяем условие: менять только препейд на препейд и постпейд на постпейд
    -- Достаем ТП карты    
    if pi_abonent_type.related = 1 then
      select distinct t.tariff_type
        into l_tariff_type_old
        from t_tmc_sim ts
        join t_tarif_by_at_id t
          on ts.tar_id = t.at_id
       where ts.sim_imsi = l_simnum
          or ts.sim_iccid = l_simnum;
      select distinct t.tariff_type
        into l_tariff_type_new
        from t_tarif_by_at_id t
       where t.at_id = pi_abonent_type.tar_id;
    
      if l_tariff_type_old <> l_tariff_type_new then
        po_errors := po_errors || l_errs(45);
      end if;
    end if;
  
    if (po_errors is not null) then
      return 0;
    else
      return 1;
    end if;
  
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      --      rollback;
      logging_pkg.error(pi_message  => po_err_msg || ' ' || l_str,
                        pi_location => c_package || 'Check_Abonent');
      return 0;
    
  end Check_Abonent;

  -------------------- Возвращение Несовпадений по идентификатору сверки -----------------------
  function Get_Mismatch_By_Id(pi_mismatch_id in number,
                              pi_region_id   in number,
                              pi_worker_id   in number,
                              po_err_num     out pls_integer,
                              po_err_msg     out t_Err_Msg)
    return sys_refcursor is

    cur sys_refcursor;

  begin
    -- Проверка прав доступа
    if (not Security_pkg.Check_User_Right_str('EISSD.CONNECTIONS.SVERKY',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;

    open cur for
      select tab.*
        from (
              -- подключения ЕИССД имеют некоторые различия от подключений АСР
              select m.mis_rev_id,
                      m.asr_eissd_order_id,
                      m.asr_remote_subsciber_id,
                      m.asr_dealer_id,
                      (null) asr_org_name,
                      m.asr_phone_num,
                      m.asr_sim_num,
                      m.asr_region_id,
                      m.asr_tar_id,
                      (null) asr_title,
                      m.asr_category,
                      m.asr_eissd_arg_date,
                      m.asr_subscr_create_date,
                      m.asr_subscr_activate_date,
                      m.asr_encashment,
                      m.eissd_order_id,
                      m.eissd_remote_subsciber_id,
                      m.eissd_dealer_id,
                      oo.org_name as eissd_org_name,
                      m.eissd_phone_num,
                      m.eissd_sim_num,
                      m.eissd_region_id,
                      m.eissd_tar_id,
                      tt.title as eissd_tar_name,
                      m.eissd_category,
                      m.eissd_eissd_arg_date,
                      m.eissd_subscr_create_date,
                      m.eissd_subscr_activate_date,
                      m.eissd_encashment /*(null) asr_org_name,*/ /*(null) asr_title,*/ /*oo.org_name as eissd_org_name,*/ /*tt.title as eissd_tar_name*/
                from T_MISMATCH      m,
                      T_ORGANIZATIONS oo,
                      T_ABSTRACT_TAR  taa,
                      T_TARIFF2       tt
               where oo.org_id = (case
                       when m.eissd_dealer_id <> -1 and
                            m.asr_dealer_id is not null then
                        m.eissd_dealer_id
                     end)
                 and m.eissd_tar_id = taa.at_remote_id
                 and taa.at_id = tt.at_id
                 and ((oo.region_id = pi_region_id and
                     pi_region_id is not null) or pi_region_id is null)
              union
              -- подключения есть в ЕИССД, но нет в выгрузке АСР
              select m.mis_rev_id,
                      m.asr_eissd_order_id,
                      m.asr_remote_subsciber_id,
                      m.asr_dealer_id,
                      (null) asr_org_name,
                      m.asr_phone_num,
                      m.asr_sim_num,
                      m.asr_region_id,
                      m.asr_tar_id,
                      (null) asr_title,
                      m.asr_category,
                      m.asr_eissd_arg_date,
                      m.asr_subscr_create_date,
                      m.asr_subscr_activate_date,
                      m.asr_encashment,
                      m.eissd_order_id,
                      m.eissd_remote_subsciber_id,
                      m.eissd_dealer_id,
                      oo.org_name as eissd_org_name,
                      m.eissd_phone_num,
                      m.eissd_sim_num,
                      m.eissd_region_id,
                      m.eissd_tar_id,
                      tt.title as eissd_tar_name,
                      m.eissd_category,
                      m.eissd_eissd_arg_date,
                      m.eissd_subscr_create_date,
                      m.eissd_subscr_activate_date,
                      m.eissd_encashment /*(null) asr_org_name,*/ /*(null) asr_title,*/ /*oo.org_name as eissd_org_name,*/ /*tt.title as eissd_tar_name*/
                from T_MISMATCH      m,
                      T_ORGANIZATIONS oo,
                      T_ABSTRACT_TAR  taa,
                      T_TARIFF2       tt
               where oo.org_id = (case
                       when m.eissd_dealer_id <> -1 and
                            m.asr_dealer_id is null then
                        m.eissd_dealer_id
                     end)
                 and m.eissd_tar_id = taa.at_remote_id
                 and taa.at_id = tt.at_id
                 and ((oo.region_id = pi_region_id and
                     pi_region_id is not null) or pi_region_id is null)
              union
              -- подключения есть в выгрузке АСР, но нет в ЕИССД
              select m.mis_rev_id,
                      m.asr_eissd_order_id,
                      m.asr_remote_subsciber_id,
                      m.asr_dealer_id,
                      o.org_name as asr_org_name,
                      m.asr_phone_num,
                      m.asr_sim_num,
                      m.asr_region_id,
                      m.asr_tar_id,
                      t.title as asr_title,
                      m.asr_category,
                      m.asr_eissd_arg_date,
                      m.asr_subscr_create_date,
                      m.asr_subscr_activate_date,
                      m.asr_encashment,
                      m.eissd_order_id,
                      m.eissd_remote_subsciber_id,
                      m.eissd_dealer_id,
                      (null) eissd_org_name,
                      m.eissd_phone_num,
                      m.eissd_sim_num,
                      m.eissd_region_id,
                      m.eissd_tar_id,
                      (null) eissd_tar_name,
                      m.eissd_category,
                      m.eissd_eissd_arg_date,
                      m.eissd_subscr_create_date,
                      m.eissd_subscr_activate_date,
                      m.eissd_encashment /*o.org_name as asr_org_name,*/ /*t.title as asr_title,*/ /*(null) eissd_org_name,*/ /*(null) eissd_tar_name*/
                from T_MISMATCH      m,
                      T_ORGANIZATIONS o,
                      T_ABSTRACT_TAR  ta,
                      T_TARIFF2       t
               where o.org_id = (case
                       when m.asr_dealer_id <> -1 and
                            m.eissd_dealer_id is null then
                        m.asr_dealer_id
                     end)
                 and m.asr_tar_id = ta.at_remote_id
                 and ta.at_id = t.at_id
                 and ((o.region_id = pi_region_id and
                     pi_region_id is not null) or pi_region_id is null)) tab
       where tab.mis_rev_id = pi_mismatch_id;

    return cur;

  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;

  end Get_Mismatch_By_Id;

  -----------------------------------------------------------------------
  -- Сохранение Юрикофф
  -----------------------------------------------------------------------
  function Save_Abonent_Jur(Pi_Client_id in number,
                            Pi_org_id    in Number,
                            pi_abonent   in abonent_tab_jur,
                            --                            pi_services  in service_tab,
                            pi_savetype  in number,
                            pi_worker_id in T_USERS.USR_ID%type,
                            pi_cost      in number,
                            Pi_Pack_Id   in number,
                            pi_is_common in number,
                            po_is_ok     out number,
                            po_err_num   out pls_integer,
                            po_err_msg   out varchar2) return sys_refcursor is
  begin
    return Save_Abonent_Jur(Pi_Client_id,
                            Pi_org_id,
                            pi_abonent,
                            --                            pi_services  in service_tab,
                            pi_savetype,
                            pi_worker_id,
                            pi_cost,
                            Pi_Pack_Id,
                            pi_is_common,
                            0,
                            po_is_ok,
                            po_err_num,
                            po_err_msg);
  end;

  function Save_Abonent_Jur(Pi_Client_id in number,
                            Pi_org_id    in Number,
                            pi_abonent   in abonent_tab_jur,
                            --                            pi_services  in service_tab,
                            pi_savetype    in number,
                            pi_worker_id   in T_USERS.USR_ID%type,
                            pi_cost        in number,
                            Pi_Pack_Id     in number,
                            pi_is_common   in number,
                            pi_is_alien_ab in number,
                            po_is_ok       out number,
                            po_err_num     out pls_integer,
                            po_err_msg     out varchar2) return sys_refcursor is
    res1         sys_refcursor;
    res          num_tab := Num_Tab();
    l_Abonent    abonent_type;
    l_Is_Ok      number;
    l_pack_Id    number := Pi_Pack_Id;
    l_account_id t_abonent.client_remote_id%type;
    --l_cc_id   number;
    --    OraRecord abonent_type_jur:=pi_abonent;
  BEGIN
    logging_pkg.info('Вошли. Pi_org_id: ' || Pi_org_id || ';
    Pi_Client_id: ' || Pi_Client_id || ';
    pi_worker_id: ' || pi_worker_id || ';
    pi_abonent.Count: ' || pi_abonent.Count || ';
    pi_savetype: ' || pi_savetype || ';
    Pi_paid: ' || pi_abonent(1).paid || ';
    pi_cost: ' || pi_cost,
                     'Save_Abonent_Jur');
    If l_pack_Id Is Null then
      Insert into t_ab_package
        (package_id)
      values
        (Null)
      returning package_id into l_pack_Id;
    End If;

    For OraRecord in (Select t.*, ts.is_related, RowNum rn
                        from Table(pi_abonent) t
                        left join t_tmc_sim ts
                          on ts.sim_imsi = t.imsi) loop
      /*If OraRecord.rn=1 then
        l_sum_paid_saldo:=OraRecord.paid;
      End If;
      l_sum_paid:=least(l_sum_paid_saldo,OraRecord.cost);
      l_sum_paid_saldo:=l_sum_paid_saldo-l_sum_paid;*/

      logging_pkg.info('abonent_id  =>' || OraRecord.abonent_id || ',
    is_bad      =>' || OraRecord.is_bad || ',
    phone_make  =>' || OraRecord.phone_make || ',
    phone_model =>' || OraRecord.phone_model || ',
    phone_imei  =>' || OraRecord.phone_imei || ',
    status      =>' || OraRecord.status || ',
    paid        =>' || OraRecord.paid || ',
    cost        =>' || OraRecord.cost || ',
    tar_id      =>' || OraRecord.tar_id || ',
    tar_name    =>' || OraRecord.tar_name || ',
    imsi        =>' || OraRecord.imsi || ',
    callsign    =>' || OraRecord.callsign || ',
    citynum     =>' || OraRecord.citynum || ',
    simcolor    =>' || OraRecord.simcolor || ',
    err_code    =>' || OraRecord.err_code || ',
    err_msg     =>' || OraRecord.err_msg || ',
                                                  kl_region_id       =>' ||
                       OraRecord.kl_region_id || ',
    note        =>' || OraRecord.note || ',
    related     =>' || OraRecord.related || ',
    is_related  =>' || OraRecord.is_related || ',
    equipment_required  =>' ||
                       OraRecord.equipment_required || ',
    equipment_model_id  =>' ||
                       OraRecord.equipment_model_id || ',
    equipment_cost      =>' ||
                       OraRecord.equipment_cost || ',
    services    =>' || OraRecord.services.Count || ',
    Account_Id  =>' || OraRecord.Account_Id || ',
    client_account  =>' || OraRecord.client_account || ',

    limit_type  =>' || OraRecord.limit_type || ',
    limit_warning  =>' || OraRecord.limit_warning || ',
    limit_block  =>' || OraRecord.limit_block || ',
    limit_date_start  =>' ||
                       OraRecord.limit_date_start || ',
    CODE_WORD  =>' || OraRecord.CODE_WORD || ',
    OUT_ACCOUNT  =>' || OraRecord.OUT_ACCOUNT || ',
    seller_active_id  =>' ||
                       OraRecord.seller_active_id || ',
    limit_date_end  =>' || OraRecord.limit_date_end,

                       'Save_Abonent_Jur');
      --l_cc_id   := get_ccid_by_klregionid(oraRecord.KL_REGION_ID);
      l_Abonent := abonent_type( /*abonent_id         =>*/OraRecord.abonent_id,
                                /*is_bad             =>*/
                                OraRecord.is_bad,
                                /*client_id          =>*/
                                Pi_Client_id,
                                /*org_id             =>*/
                                Pi_org_id,
                                /*phone_make         =>*/
                                OraRecord.phone_make,
                                /*phone_model        =>*/
                                OraRecord.phone_model,
                                /*phone_imei         =>*/
                                OraRecord.phone_imei,
                                /*status             =>*/
                                OraRecord.status,
                                /*paid               =>*/
                                OraRecord.paid,
                                /*cost               =>*/
                                OraRecord.cost,
                                /*tar_id             =>*/
                                OraRecord.tar_id,
                                /*tar_name           =>*/
                                OraRecord.tar_name,
                                /*imsi               =>*/
                                OraRecord.imsi,
                                /*callsign           =>*/
                                OraRecord.callsign,
                                /*citynum            =>*/
                                (Case
                                  when OraRecord.citynum = 0 then
                                   Null
                                  else
                                   OraRecord.citynum
                                end),
                                /*simcolor           =>*/
                                OraRecord.simcolor,
                                /*err_code           =>*/
                                OraRecord.err_code,
                                /*err_msg            =>*/
                                OraRecord.err_msg,
                                /*cc_id              =>*/
                                --l_cc_id,
                                null,
                                /*dog_date           =>*/
                                sysdate,
                                /*user_id            =>*/
                                OraRecord.user_id,
                                /*note               =>*/
                                OraRecord.note,
                                /*Для массовых подключений признак связанности поднимаю из БД, на основании данных в СИМ*/
                                /*related            =>*/
                                (Case
                                  when OraRecord.is_related is Null then
                                   0
                                  when OraRecord.is_related = 2 then
                                   0
                                  else
                                   OraRecord.is_related
                                end), -- признак связанной или несвязаннной карты
                                /*related            => OraRecord.related,*/
                                /*equipment_required =>*/
                                OraRecord.equipment_required,
                                null,
                                OraRecord.equipment_model_id,
                                /*equipment_model_id =>*/
                                --OraRecord.equipment_tmc_id,
                                /*equipment_cost     =>*/
                                OraRecord.equipment_cost,
                                /*client_account     =>*/
                                OraRecord.client_account,
                                -- 51312 Бюджетирование
                                OraRecord.limit_type,
                                OraRecord.limit_warning,
                                OraRecord.limit_block,
                                OraRecord.limit_date_start,
                                OraRecord.limit_date_end,
                                OraRecord.client_id,
                                OraRecord.seller_active_id,
                                OraRecord.WISH_DATE,
                                OraRecord.agree_work_info,
                                OraRecord.agree_cancel_dog,
                                OraRecord.agree_pay_off_debt,
                                OraRecord.ATTACHED_DOC,
                                OraRecord.NUMBER_SHEETS,
                                OraRecord.usb_ser,
                                OraRecord.TRANSFER_PHONE,
                                OraRecord.OPERATION_DONOR,
                                OraRecord.NPID,
                                OraRecord.TYPE_PROCESS_CBDPN,
                                OraRecord.operator_recipient,
                                'J',
                                OraRecord.channel_id,
                                OraRecord.CODE_WORD,
                                OraRecord.OUT_ACCOUNT,
                                OraRecord.kl_region_id,
                                OraRecord.CREDIT_LIMIT,
                                OraRecord.ALARM_LIMIT,
                                OraRecord.IS_PERSONAL_ACCOUNT,
                                OraRecord.reg_id_ps,
                                OraRecord.usl_number,
                                OraRecord.fix_address_id,
                                OraRecord.fix_name,
                                OraRecord.ab_comment,
                                OraRecord.req_delivery_id,
                                OraRecord.request_id,
                                OraRecord.consent_msg,
                                OraRecord.method_connect);
      l_Is_Ok   := Save_Abonent3(pi_abonent   => l_Abonent,
                                 pi_services  => OraRecord.services,
                                 pi_savetype  => pi_savetype,
                                 pi_worker_id => pi_worker_id,
                                 pi_cost      => OraRecord.cost,
                                 pi_is_common => pi_is_common,
                                 -- 40261 Брендированное оборудование
                                 pi_tmc_phone   => OraRecord.tmc_phone,
                                 pi_is_alien_ab => pi_is_alien_ab,
                                 po_is_ok       => po_is_ok,
                                 po_err_num     => po_err_num,
                                 po_err_msg     => po_err_msg);
      If NVL(po_err_num, 0) <> 0 then
        logging_pkg.info(po_err_msg, 'Save_Abonent_Jur');
      End If;
      res.extend;
      If po_is_ok > 0 then
        res(res.Count) := l_Is_Ok;
        begin
          -- Ищем лицевой счет по пачке, если на вход пришел пустой
          if nvl(OraRecord.Account_Id, 0) = 0 then
            select max(ab.client_remote_id)
              into l_account_id
              from t_abonent ab
             where ab.package_id = l_pack_Id;
          end if;
          Update t_abonent ab
             set ab.client_remote_id = Decode(OraRecord.Account_Id,
                                              0,
                                              l_account_id,
                                              -1,
                                              null,
                                              (case
                                                when length(to_char(OraRecord.Account_Id)) = 11 and
                                                     substr(to_char(OraRecord.Account_Id),
                                                            1,
                                                            1) = '7' then
                                                 to_number(substr(to_char(OraRecord.Account_Id),
                                                                  2))
                                                else
                                                 OraRecord.Account_Id
                                              end)),
                 ab.package_id       = l_pack_Id,
                 ab.correlationid    = decode(pi_is_alien_ab,
                                              1,
                                              l_pack_id,
                                              null)
           Where ab.ab_id = l_Is_Ok;
          Update t_abon_bad ab
             set ab.package_id    = l_pack_Id,
                 ab.correlationid = decode(pi_is_alien_ab,
                                           1,
                                           l_pack_id,
                                           null)
           Where ab.id = l_Is_Ok;

          if pi_is_alien_ab = 1 then
            if OraRecord.abonent_id is null then
              insert into t_abonent_out_status
                (AB_ID, STATUS, WORKER_ID, TEXT, action)
              values
                (l_pack_Id,
                 decode(OraRecord.status, 96, 6, 97, 5, 0),
                 pi_worker_id,
                 null,
                 'CreateRequest');
            else
              insert into t_abonent_out_status
                (AB_ID, STATUS, WORKER_ID, TEXT, action)
              values
                (l_pack_Id,
                 decode(OraRecord.status, 96, 6, 97, 5, 0),
                 pi_worker_id,
                 null,
                 'UpdateRequest');
            end if;
          end if;
        exception
          when others then
            null;
        End;
      Else
        /*res(res.Count) := -1;*/
        res(res.Count) := l_Is_Ok;
        Update t_abon_bad ab
           set ab.package_id    = l_pack_Id,
               ab.correlationid = decode(pi_is_alien_ab, 1, l_pack_id, null)
         Where ab.id = l_Is_Ok;
      End If;
      po_is_ok := Null;
    End Loop;

    Update t_abonent ab
       set ab.ab_status = Decode(RowNum, 1, ab.ab_status, 106)
     Where ab.ab_status = 102
       and ab.Package_Id = l_pack_Id
       and ab.client_remote_id is Null;

    Open res1 for
      select /*+ PRECOMPUTE_SUBQUERY */
       column_value,
       case
         when ab.id is not null then
          0
         else
          1
       end is_ok,
       nvl(a.package_id, ab.package_id) package_id
        from TABLE(res)
        left join t_abonent a
          on a.ab_id = column_value
        left join t_abon_bad ab
          on ab.id = column_value;

    logging_pkg.info('Ok', 'Save_Abonent_Jur');
    return res1;
    /*  Exception
    when others then
     logging_pkg.info(sqlerrm,'Save_Abonent_Jur');*/
  End;

  ----------------------------------------------------------------------------
  --  Возвращает список абонентов по идентификатору пакета
  ----------------------------------------------------------------------------
  function Get_Abonent_By_Pack_Id(pi_pack_id   in Number,
                                  pi_worker_id in Number,
                                  po_err_num   out pls_integer,
                                  po_err_msg   out t_Err_Msg,
                                  pi_is_bad    in number)
    return sys_refcursor is

    res      sys_refcursor;
    l_org_id t_organizations.org_id%type;
  begin

    /*    if (not Security_pkg.Check_Rights(5003,
                                      l_org_id,
                                      pi_worker_id,
                                      po_err_num,
                                      po_err_msg)) then
          return null;
        end if;
    */
    logging_pkg.debug('pi_pack_id = ' || pi_pack_id,
                      'Get_Abonent_By_Pack_Id');

    select max(org_id)
      into l_org_id
      from (select max(ab.org_id) org_id
              from t_abonent ab
             Where ab.package_id = pi_pack_id
               and ab.is_deleted = 0
               and ab.is_canceled = 0
            union
            select max(b.id_org) org_id
              from t_abon_bad b
             where b.package_id = pi_pack_id
               and b.is_deleted = 0)
     where org_id is not null;

    -- берем абонента
    open res for
      select a.ab_id abonent_id,
             a.client_id,
             a.ab_tmc_id tmc_id,
             a.ab_reg_date reg_date,
             a.ab_mod_date mod_date,
             a.phone_make,
             a.phone_model,
             a.phone_imei,
             a.ab_status status,
             a.ab_paid paid,
             a.ab_cost cost,
             a.err_code,
             getasrerror(a.err_code, a.err_msg) err_msg,
             a.org_id,
             a.cc_id,
             a.org_id which_org_ab_id,
             (select ooo.org_name
                from t_organizations ooo
               where ooo.org_id = a.org_id) which_org_ab_name,
             nvl2(a.tar_id,
                  (select concat(dv1.dv_name,
                                 concat(' ',
                                        concat(dv3.dv_name,
                                               concat('  ', dv2.dv_name))))
                     from t_tariff2    t,
                          t_dic_values dv1,
                          t_dic_values dv2,
                          t_dic_values dv3
                    where t.id = get_tar_id_by_at_id(a.tar_id, a.ab_dog_date)
                      and t.at_id = a.tar_id
                      and t.type_vdvd_id = dv1.dv_id
                      and t.tariff_type = dv2.dv_id
                      and t.pay_type = dv3.dv_id),
                  'не известен') conn_name,
             a.tar_id tar_id,
             ab_tar_name tar_name,
             nvl2(a.tar_id,
                  (select t.type_vdvd_id
                     from t_tariff2 t
                    where t.id = get_tar_id_by_at_id(a.tar_id, ab_dog_date)),
                  null) type_vdvd_id,
             a.imsi imsi,
             a.callsign cs,
             a.city_num,
             a.sim_color,
             a.ab_dog_date dog_date,
             a.is_bad,
             a.id_op,
             a.id_bad ab_id_bad,
             a.ab_servs,
             (select trim(cc.cc_name)
                from t_calc_center cc
               where cc.cc_id = a.cc_id) cc_name,
             a.change_status_date,
             '' dog_number,
             a.archive_mark,
             a.user_id user_id,
             a.user_id worker,
             a.tmc_type,
             a.usb_full_cost,
             a.usb_model,
             a.is_related,
             a.is_common,
             transfer_phone,
             transfer_date,
             operation_donor,
             null cg_id,
             sim_type,
             client_account,
             sim_type_name,
             phone_serial_number,
             phone_name_model,
             phone_cost,
             phone_bonus_name,
             phone_bonus_id,
             phone_model_id,
             usb_model_name,
             usb_ser,
             limit_type,
             limit_warning,
             limit_block,
             limit_date_start,
             limit_date_end,
             budget_client,
             operator_recipient,
             model_equip,
             LSMStransStatus,
             LSMSaddTransStatus,
             description,
             mnp_reg,
             mnc_code,
             mnp_oper_don_name,
             mnp_oper_don_code,
             mnp_oper_rec_name,
             mnp_oper_rec_code,
             SMSNOTICEFLAG,
             EMAILNOTICEFLAG,
             date_create_cbdpn,
             correlationid,
             COMMUN_DESTINATION,
             reject_reason,
             channel_id,
             req_delivery_id,
             request_id,
             consent_msg,
             agr_num,
             tariff_type,
             sim_iccid,
             method_connect
        from (select ab1.ab_id,
                     null id_bad,
                     ab1.client_id,
                     ab1.ab_reg_date,
                     ab1.ab_mod_date,
                     ab1.phone_make,
                     ab1.phone_model,
                     ab1.phone_imei,
                     ab1.ab_status,
                     ab1.ab_paid,
                     ab1.ab_cost,
                     ab1.ab_tmc_id,
                     --                     ab1.ab_sync,
                     ab1.err_code,
                     ab1.err_msg,
                     ab1.org_id,
                     ab1.remote_id,
                     ab1.change_status_date,
                     ab1.cc_id,
                     ab1.ab_dog_date,
                     0 is_bad,
                     ab1.id_op,
                     nvl(sim.tar_id, ruim.tar_id) tar_id,
                     tar.title ab_tar_name,
                     null ab_servs,
                     ab1.archive_mark,
                     nvl(nvl(sim.sim_imsi, sim.sim_iccid), ruim.ruim_imsi) imsi,
                     nvl(sim.sim_callsign, ruim.ruim_callsign) callsign,
                     to_number(sim.sim_callsign_city) city_num,
                     sim.sim_color,
                     ab1.user_id,
                     tt.tmc_type,
                     tum.usb_full_cost,
                     tum.usb_model usb_model,
                     nvl(sim.is_related, 1) is_related,
                     ab1.package_id,
                     ab1.is_common,
                     ab1.transfer_phone,
                     ab1.transfer_date,
                     ab1.operation_donor,
                     sim.sim_type,
                     ab1.client_account,
                     dic_st.name sim_type_name,
                     tp.serial_number as phone_serial_number,
                     mp.name_model as phone_name_model,
                     -- 39401
                     pmc.cost_with_nds as phone_cost,
                     bp.bonus_name     as phone_bonus_name,
                     bp.bonus_id       as phone_bonus_id,
                     mp.model_id       as phone_model_id,
                     -- 42523 Модель модема
                     mmu.usb_model as usb_model_name,
                     tum.usb_ser,
                     tb.limit_type,
                     tb.limit_warning,
                     tb.limit_block,
                     tb.limit_date_start,
                     tb.limit_date_end,
                     tb.client_id as budget_client,
                     ab1.operator_recipient,
                     nvl(mmu.usb_model, mp.name_model) model_equip,
                     LSMStransStatus,
                     LSMSaddTransStatus,
                     description,
                     mnp_reg.mnp_reg,
                     mnp_reg.mnc_code,
                     mnp_oper_don.name mnp_oper_don_name,
                     mnp_oper_don.operatorcode mnp_oper_don_code,
                     mnp_oper_rec.name mnp_oper_rec_name,
                     mnp_oper_rec.operatorcode mnp_oper_rec_code,
                     SMSNOTICEFLAG,
                     EMAILNOTICEFLAG,
                     ab1.date_create_cbdpn date_create_cbdpn,
                     ab1.correlationid,
                     ab1.Commun_Destination,
                     ab1.reject_reason,
                     ab1.channel_id,
                     ab1.req_delivery_id,
                     abr.request_id,
                     ab1.consent_msg,
                     ab1.agr_num,
                     at.tariff_type,
                     sim.sim_iccid,
                     ab1.method_connect
                from t_abonent ab1
                join t_tmc t
                  on ab1.ab_tmc_id = t.tmc_id
                left join t_tmc_sim sim
                  on t.tmc_type = 8
                 and t.tmc_id = sim.tmc_id
                left join t_tmc_ruim ruim
                  on t.tmc_type = 9
                 and t.tmc_id = ruim.tmc_id
                left join t_tmc tt
                  on ab1.equipment_tmc_id = tt.tmc_id
                left join t_tmc_modem_usb tum
                  on tum.tmc_id = tt.tmc_id
                 and tt.tmc_type = 4
                left join t_dic_sim_type dic_st
                  on dic_st.id = sim.sim_type
              -----------
                left join t_modem_model_usb mmu
                  on tum.usb_model = mmu.id
                left join t_abonent_phone ap
                  on ap.ab_id = ab1.ab_id
                left join t_tmc_phone tp
                  on tp.tmc_id = ap.tmc_id
                left join t_model_phone mp
                  on tp.model_id = mp.model_id
                left join t_bonus_phone bp
                  on bp.bonus_id = ap.bonus_id
                left join t_phone_model_cost pmc
                  on mp.model_id = pmc.model_id
                 and sysdate between pmc.ver_date_beg and
                     nvl(pmc.ver_date_end, sysdate)
                 and pmc.region_id in
                     (select distinct o.region_id
                        from t_organizations o
                       where o.org_id = AB1.ORG_ID)
                left join t_abonent_budget tb
                  on tb.ab_id = ab1.ab_id
                left join t_tarif_by_at_id at
                  on sim.tar_id = at.at_id
                left join t_dic_mnp_region mnp_reg
                  on mnp_reg.reg_id = at.at_region_id
                left join t_dic_mnp_operator mnp_oper_don
                  on mnp_oper_don.id = ab1.operation_donor
                left join t_dic_mnp_operator mnp_oper_rec
                  on mnp_oper_rec.id = ab1.operator_recipient
                left join t_tarif_by_at_id tar
                  on tar.at_id = sim.tar_id
                left join t_abonent_to_request abr
                  on abr.ab_id=ab1.ab_id
                 and abr.is_send in (0,1) 
                 and abr.request_id is not null  
              ------------
               where ab1.is_deleted = 0
                 and t.tmc_type in (8, 9)
                 and ab1.package_id = pi_pack_id
              union all
              select null            ab_id,
                     ab2.id          id_bad,
                     ab2.id_client   client_id,
                     ab2.reg_date    ab_reg_date,
                     ab2.mod_date    ab_mod_date,
                     ab2.phone_make,
                     ab2.phone_model,
                     ab2.phone_imei,
                     101             ab_status,
                     ab2.paid        ab_paid,
                     ab2.cost        ab_cost,
                     ab2.id_tmc      ab_tmc_id,
                     --0 ab_sync,
                     nvl2(ab2.err_msg, -100, null) err_code,
                     ab2.err_msg,
                     ab2.id_org org_id,
                     null remote_id,
                     null change_status_date,
                     ab2.id_cc cc_id,
                     ab2.dog_date ab_dog_date,
                     1 is_bad,
                     ab2.id_op,
                     ab2.id_tar tar_id,
                     tar.title ab_tar_name,
                     ab2.service_area ab_servs,
                     ab2.archive_mark,
                     ab2.imsi,
                     ab2.callsign,
                     ab2.city_num,
                     ab2.sim_color,
                     ab2.user_id,
                     ab2.equipment_required tmc_type,
                     /*tum.usb_full_cost,
                     tum.usb_model,*/
                     ab2.equipment_cost / 100 as usb_full_cost,
                     ab2.equipment_model_id as usb_model,
                     ab2.is_related,
                     ab2.package_id,
                     ab2.is_common,
                     ab2.transfer_phone,
                     ab2.transfer_date,
                     ab2.operation_donor,
                     sim.sim_type,
                     ab2.client_account,
                     dic_st.name sim_type_name,
                     ab2.phone_serial_number as phone_serial_number,
                     mp.name_model as phone_name_model,
                     -- 39401
                     pmc.cost_with_nds as phone_cost,
                     bp.bonus_name     as phone_bonus_name,
                     bp.bonus_id       as phone_bonus_id,
                     mp.model_id       as phone_model_id,
                     -- 42523 Модель модема
                     mmu.usb_model as usb_model_name,
                     ab2.equipment_ser usb_ser,
                     tbb.limit_type,
                     tbb.limit_warning,
                     tbb.limit_block,
                     tbb.limit_date_start,
                     tbb.limit_date_end,
                     tbb.client_id as budget_client,
                     ab2.operator_recipient,
                     nvl(mmu.usb_model, mp.name_model) model_equip,
                     LSMStransStatus,
                     LSMSaddTransStatus,
                     description,
                     mnp_reg.mnp_reg,
                     mnp_reg.mnc_code,
                     mnp_oper_don.name mnp_oper_don_name,
                     mnp_oper_don.operatorcode mnp_oper_don_code,
                     mnp_oper_rec.name mnp_oper_rec_name,
                     mnp_oper_rec.operatorcode mnp_oper_rec_code,
                     SMSNOTICEFLAG,
                     EMAILNOTICEFLAG,
                     ab2.date_create_cbdpn date_create_cbdpn,
                     ab2.correlationid,
                     ab2.COMMUN_DESTINATION,
                     ab2.reject_reason,
                     ab2.channel_id,
                     ab2.req_delivery_id,
                     ab2.request_id,
                     ab2.consent_msg,
                     null agr_num,
                     at.tariff_type,
                     sim.sim_iccid,
                     ab2.method_connect
                from t_abon_bad ab2
                left join t_tmc t
                  on t.tmc_id = ab2.equipment_model_id
              /*left join t_tmc_modem_usb tum
               on tum.tmc_id = t.tmc_id
              and t.tmc_type = 4*/
                left join t_tmc_sim sim
                  on sim.tmc_id = ab2.id_tmc
                left join t_dic_sim_type dic_st
                  on dic_st.id = sim.sim_type
              -----------------------
                left join t_model_phone mp
                  on mp.model_id = ab2.phone_model_id
                left join t_bonus_phone bp
                  on bp.bonus_id = ab2.phone_bonus_id
              -- 39401
                left join t_phone_model_cost pmc
                  on mp.model_id = pmc.model_id
                 and sysdate between pmc.ver_date_beg and
                     nvl(pmc.ver_date_end, sysdate)
                 and pmc.region_id in
                     (select distinct o.region_id
                        from t_organizations o
                       where o.org_id = AB2.ID_ORG)
                left join t_modem_model_usb mmu
                  on ab2.equipment_model_id = mmu.id
                left join t_abon_bad_budget tbb
                  on tbb.bad_id = ab2.id
                left join t_tarif_by_at_id at
                  on ab2.id_tar = at.at_id
                left join t_dic_mnp_region mnp_reg
                  on mnp_reg.reg_id = at.at_region_id
                left join t_dic_mnp_operator mnp_oper_don
                  on mnp_oper_don.id = ab2.operation_donor
                left join t_dic_mnp_operator mnp_oper_rec
                  on mnp_oper_rec.id = ab2.operator_recipient
                left join t_tarif_by_at_id tar
                  on tar.at_id = ab2.id_tar
              ------------------------
               where ab2.is_deleted = 0
                 and ab2.package_id = pi_pack_id) a
       where a.is_bad = nvl(pi_is_bad, a.is_bad)
         and a.org_id is not null
         and not (a.org_id = -1);

    return res;

  exception

    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Abonent_By_Pack_Id;

  ----------------------------------------------------------------------------
  --  Возвращает список абонентов по идентификатору пакета
  --  Перевызов! Нужна организация для проверки прав.
  -- 64814(MNP)
  ----------------------------------------------------------------------------
  function Get_Abonent_By_Pack_Id(pi_pack_id   in Number,
                                  pi_worker_id in Number,
                                  pi_org_id    in number,
                                  po_err_num   out pls_integer,
                                  po_err_msg   out t_Err_Msg,
                                  pi_is_bad    in number)
    return sys_refcursor is

  begin

    if (not Security_pkg.Check_Rights_str('EISSD.CONNECTIONS.GSM.VIEW',
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      return null;
    end if;

    return tmc_ab.Get_Abonent_By_Pack_Id(pi_Pack_id   => pi_pack_id,
                                         pi_worker_id => pi_worker_id,
                                         po_err_num   => po_err_num,
                                         po_err_msg   => po_err_msg,
                                         pi_is_bad    => pi_is_bad);
  end;

  ----------------------------------------------------------------------------
  --поиск юридического клиента среди данных ЕИССД
  --по ИНН + (КПП), подключенному номеру телефона, лицевому счету, номеру договора
  ----------------------------------------------------------------------------
  function Search_Jur_Client(pi_jur_inn      in varchar2, --ИНН
                             pi_jur_kpp      in varchar2, --КПП
                             pi_ogrn         in t_juristic.jur_ogrn%type,
                             pi_fed_callsign in number, --№ тел
                             pi_account      in varchar2, --лицевой счет
                             pi_agr_num      in varchar2, --№ договора
                             pi_worker_id    in number,
                             po_err_num      out pls_integer,
                             po_err_msg      out t_Err_Msg)
    return sys_refcursor is
    res sys_refcursor;
  begin
    if (pi_jur_inn is not null or pi_jur_kpp is not null or
       pi_ogrn is not null) then
      open res for
        select c.client_id ce_id,
               1 ce_is_corp,
               j.jur_name fullname,
               j.jur_inn INN,
               j.jur_kpp KPP,
               j.jur_ogrn ogrn,
               ab.client_account accnum,
               ab.agr_num c_number,
               trunc(ab.ab_reg_date) reg_dt,
               null balance
          from T_CLIENTS C
          join t_juristic j
            on c.fullinfo_id = j.juristic_id
          left join t_abonent ab
            on c.client_id = ab.client_id
         where (pi_jur_inn is null or j.jur_inn = pi_jur_inn)
           and (pi_jur_kpp is null or j.jur_kpp = pi_jur_kpp)
           and (pi_ogrn is null or j.jur_ogrn = pi_ogrn);
    elsif pi_fed_callsign is not null then
      open res for
        select j.juristic_id ce_id,
               1 ce_is_corp,
               j.jur_name fullname,
               j.jur_inn INN,
               j.jur_kpp KPP,
               j.jur_ogrn ogrn,
               ab.client_account accnum,
               ab.agr_num c_number,
               trunc(ab.ab_reg_date) reg_dt,
               null balance
          from T_CLIENTS C
          join t_juristic j
            on c.fullinfo_id = j.juristic_id
          left join t_abonent ab
            on c.client_id = ab.client_id
          left join t_tmc_sim sim
            on ab.ab_tmc_id = sim.tmc_id
         where (pi_fed_callsign is null or
               sim.sim_callsign = pi_fed_callsign and
               ab.ab_status in (104, 105) and ab.is_deleted = 0 and
               ab.is_canceled = 0);
    elsif pi_agr_num is not null then
      open res for
        select j.juristic_id ce_id,
               1 ce_is_corp,
               j.jur_name fullname,
               j.jur_inn INN,
               j.jur_kpp KPP,
               j.jur_ogrn ogrn,
               ab.client_account accnum,
               ab.agr_num c_number,
               trunc(ab.ab_reg_date) reg_dt,
               null balance
          from T_CLIENTS C
          join t_juristic j
            on c.fullinfo_id = j.juristic_id
          left join t_abonent ab
            on c.client_id = ab.client_id
         where (pi_agr_num is null or ab.agr_num = pi_agr_num);
    elsif pi_account is not null then
      open res for
        select j.juristic_id ce_id,
               1 ce_is_corp,
               j.jur_name fullname,
               j.jur_inn INN,
               j.jur_kpp KPP,
               j.jur_ogrn ogrn,
               ab.client_account accnum,
               ab.agr_num c_number,
               trunc(ab.ab_reg_date) reg_dt,
               null balance
          from T_CLIENTS C
          join t_juristic j
            on c.fullinfo_id = j.juristic_id
          left join t_abonent ab
            on c.client_id = ab.client_id
         where (pi_account is null or ab.client_account = pi_account);
    end if;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      /*open res for
      select 1 from dual where 1 <> 1;*/
      return null;
  end;
  ----------------------------------------------------------------------------
  /*  ----------------------------------------------------------------------------
  function Search_Jur_Client(pi_jur_inn      in varchar2, --ИНН
                             pi_jur_kpp      in varchar2, --КПП
                             pi_fed_callsign in number, --№ тел
                             pi_account      in varchar2, --лицевой счет
                             pi_agr_num      in varchar2, --№ договора
                             pi_worker_id    in number,
                             po_err_num      out pls_integer,
                             po_err_msg      out t_Err_Msg)
    return sys_refcursor is
    res sys_refcursor;
  begin
    if pi_jur_inn is not null then
      open res for
        select j.juristic_id            ce_id,
               null                     ce_sex,
               null                     ce_is_vip,
               1                        ce_is_corp,
               null                     ce_birthday,
               null                     ce_psp_series,
               null                     ce_psp_number,
               null                     ce_psp_issue_date,
               null                     ce_psp_issuer,
               j.jur_name               ce_name,
               j.jur_name               ce_surname,
               null                     ce_givname,
               null                     ce_taxid,
               j.jur_ogrn               ce_ogrn,
               j.jur_kpp                ce_kpp,
               j.jur_okpo               ce_okpo,
               j.jur_okved              ce_okved,
               b.bnk_id                 ce_bank_code,
               b.bnk_name               ce_bank_name,
               null                     ce_branch_name,
               j.jur_threasury_name     ce_treasury_name,
               j.jur_threasury_acc      ce_treasury_acc,
               j.jur_corr_akk           ce_corr_acc, -- корр
               j.jur_settlement_account ce_settl_acc, -- расч. счет
               null                     cm_is_person,
               null                     cm_category_id,
               null                     cm_category_name,
               j.jur_name_short         ch_fullname,
               c.ver                    ch_ver_num,
               1                        ch_state,
               null                     law_ad_raw_text,
               null                     law_ad_region,
               null                     law_ad_district,
               a1.addr_city             law_ad_city,
               null                     law_ad_settle,
               a1.addr_street           law_ad_street,
               null                     law_ad_home,
               a1.addr_building         law_ad_build,
               a1.addr_office           law_ad_flat,
               null                     law_ad_zip,
               null                     fact_ad_raw_text,
               null                     fact_ad_region,
               null                     fact_ad_district,
               a2.addr_city             fact_ad_city,
               null                     fact_ad_settle,
               a2.addr_street           fact_ad_street,
               a2.addr_corp             fact_ad_home,
               a2.addr_building         fact_ad_build,
               a2.addr_office           fact_ad_flat,
               null                     fact_ad_zip,
               null                     balance,
               null                     Stat_PSTN,
               null                     Tar_cnt
          from T_CLIENTS C
          join t_juristic j on c.fullinfo_id = j.juristic_id
          left join T_ADDRESS A1 on A1.ADDR_ID = j.jur_address_id
          left join T_ADDRESS A2 on A2.ADDR_ID = j.jur_fact_address_id
          left join t_abonent ab on c.client_id = ab.client_id
          left join t_tmc_sim sim on ab.ab_tmc_id = sim.tmc_id
          left join banks b on b.bnk_bik = j.jur_bank_bik
         where (pi_jur_inn is null or j.jur_inn = pi_jur_inn);
    elsif pi_jur_kpp is not null then
      open res for
        select j.juristic_id            ce_id,
               null                     ce_sex,
               null                     ce_is_vip,
               1                        ce_is_corp,
               null                     ce_birthday,
               null                     ce_psp_series,
               null                     ce_psp_number,
               null                     ce_psp_issue_date,
               null                     ce_psp_issuer,
               j.jur_name               ce_name,
               j.jur_name               ce_surname,
               null                     ce_givname,
               null                     ce_taxid,
               j.jur_ogrn               ce_ogrn,
               j.jur_kpp                ce_kpp,
               j.jur_okpo               ce_okpo,
               j.jur_okved              ce_okved,
               b.bnk_id                 ce_bank_code,
               b.bnk_name               ce_bank_name,
               null                     ce_branch_name,
               j.jur_threasury_name     ce_treasury_name,
               j.jur_threasury_acc      ce_treasury_acc,
               j.jur_corr_akk           ce_corr_acc, -- корр
               j.jur_settlement_account ce_settl_acc, -- расч. счет
               null                     cm_is_person,
               null                     cm_category_id,
               null                     cm_category_name,
               j.jur_name_short         ch_fullname,
               c.ver                    ch_ver_num,
               1                        ch_state,
               null                     law_ad_raw_text,
               null                     law_ad_region,
               null                     law_ad_district,
               a1.addr_city             law_ad_city,
               null                     law_ad_settle,
               a1.addr_street           law_ad_street,
               null                     law_ad_home,
               a1.addr_building         law_ad_build,
               a1.addr_office           law_ad_flat,
               null                     law_ad_zip,
               null                     fact_ad_raw_text,
               null                     fact_ad_region,
               null                     fact_ad_district,
               a2.addr_city             fact_ad_city,
               null                     fact_ad_settle,
               a2.addr_street           fact_ad_street,
               a2.addr_corp             fact_ad_home,
               a2.addr_building         fact_ad_build,
               a2.addr_office           fact_ad_flat,
               null                     fact_ad_zip,
               null                     balance,
               null                     Stat_PSTN,
               null                     Tar_cnt
          from T_CLIENTS C
          join t_juristic j on c.fullinfo_id = j.juristic_id
          left join T_ADDRESS A1 on A1.ADDR_ID = j.jur_address_id
          left join T_ADDRESS A2 on A2.ADDR_ID = j.jur_fact_address_id
          left join t_abonent ab on c.client_id = ab.client_id
          left join t_tmc_sim sim on ab.ab_tmc_id = sim.tmc_id
          left join banks b on b.bnk_bik = j.jur_bank_bik
         where (pi_jur_kpp is null or j.jur_kpp = pi_jur_kpp);
    elsif pi_fed_callsign is not null then
      open res for
        select j.juristic_id            ce_id,
               null                     ce_sex,
               null                     ce_is_vip,
               1                        ce_is_corp,
               null                     ce_birthday,
               null                     ce_psp_series,
               null                     ce_psp_number,
               null                     ce_psp_issue_date,
               null                     ce_psp_issuer,
               j.jur_name               ce_name,
               j.jur_name               ce_surname,
               null                     ce_givname,
               null                     ce_taxid,
               j.jur_ogrn               ce_ogrn,
               j.jur_kpp                ce_kpp,
               j.jur_okpo               ce_okpo,
               j.jur_okved              ce_okved,
               b.bnk_id                 ce_bank_code,
               b.bnk_name               ce_bank_name,
               null                     ce_branch_name,
               j.jur_threasury_name     ce_treasury_name,
               j.jur_threasury_acc      ce_treasury_acc,
               j.jur_corr_akk           ce_corr_acc, -- корр
               j.jur_settlement_account ce_settl_acc, -- расч. счет
               null                     cm_is_person,
               null                     cm_category_id,
               null                     cm_category_name,
               j.jur_name_short         ch_fullname,
               c.ver                    ch_ver_num,
               1                        ch_state,
               null                     law_ad_raw_text,
               null                     law_ad_region,
               null                     law_ad_district,
               a1.addr_city             law_ad_city,
               null                     law_ad_settle,
               a1.addr_street           law_ad_street,
               null                     law_ad_home,
               a1.addr_building         law_ad_build,
               a1.addr_office           law_ad_flat,
               null                     law_ad_zip,
               null                     fact_ad_raw_text,
               null                     fact_ad_region,
               null                     fact_ad_district,
               a2.addr_city             fact_ad_city,
               null                     fact_ad_settle,
               a2.addr_street           fact_ad_street,
               a2.addr_corp             fact_ad_home,
               a2.addr_building         fact_ad_build,
               a2.addr_office           fact_ad_flat,
               null                     fact_ad_zip,
               null                     balance,
               null                     Stat_PSTN,
               null                     Tar_cnt
          from T_CLIENTS C
          join t_juristic j on c.fullinfo_id = j.juristic_id
          left join T_ADDRESS A1 on A1.ADDR_ID = j.jur_address_id
          left join T_ADDRESS A2 on A2.ADDR_ID = j.jur_fact_address_id
          left join t_abonent ab on c.client_id = ab.client_id
          left join t_tmc_sim sim on ab.ab_tmc_id = sim.tmc_id
          left join banks b on b.bnk_bik = j.jur_bank_bik
         where (pi_fed_callsign is null or
               sim.sim_callsign = pi_fed_callsign and ab.ab_status in (104, 105) and
               ab.is_deleted = 0 and ab.is_canceled = 0);
    elsif pi_agr_num is not null then
      open res for
        select j.juristic_id            ce_id,
               null                     ce_sex,
               null                     ce_is_vip,
               1                        ce_is_corp,
               null                     ce_birthday,
               null                     ce_psp_series,
               null                     ce_psp_number,
               null                     ce_psp_issue_date,
               null                     ce_psp_issuer,
               j.jur_name               ce_name,
               j.jur_name               ce_surname,
               null                     ce_givname,
               null                     ce_taxid,
               j.jur_ogrn               ce_ogrn,
               j.jur_kpp                ce_kpp,
               j.jur_okpo               ce_okpo,
               j.jur_okved              ce_okved,
               b.bnk_id                 ce_bank_code,
               b.bnk_name               ce_bank_name,
               null                     ce_branch_name,
               j.jur_threasury_name     ce_treasury_name,
               j.jur_threasury_acc      ce_treasury_acc,
               j.jur_corr_akk           ce_corr_acc, -- корр
               j.jur_settlement_account ce_settl_acc, -- расч. счет
               null                     cm_is_person,
               null                     cm_category_id,
               null                     cm_category_name,
               j.jur_name_short         ch_fullname,
               c.ver                    ch_ver_num,
               1                        ch_state,
               null                     law_ad_raw_text,
               null                     law_ad_region,
               null                     law_ad_district,
               a1.addr_city             law_ad_city,
               null                     law_ad_settle,
               a1.addr_street           law_ad_street,
               null                     law_ad_home,
               a1.addr_building         law_ad_build,
               a1.addr_office           law_ad_flat,
               null                     law_ad_zip,
               null                     fact_ad_raw_text,
               null                     fact_ad_region,
               null                     fact_ad_district,
               a2.addr_city             fact_ad_city,
               null                     fact_ad_settle,
               a2.addr_street           fact_ad_street,
               a2.addr_corp             fact_ad_home,
               a2.addr_building         fact_ad_build,
               a2.addr_office           fact_ad_flat,
               null                     fact_ad_zip,
               null                     balance,
               null                     Stat_PSTN,
               null                     Tar_cnt
          from T_CLIENTS C
          join t_juristic j on c.fullinfo_id = j.juristic_id
          left join T_ADDRESS A1 on A1.ADDR_ID = j.jur_address_id
          left join T_ADDRESS A2 on A2.ADDR_ID = j.jur_fact_address_id
          left join t_abonent ab on c.client_id = ab.client_id
          left join t_tmc_sim sim on ab.ab_tmc_id = sim.tmc_id
          left join banks b on b.bnk_bik = j.jur_bank_bik
         where (pi_agr_num is null or ab.agr_num = pi_agr_num);
    elsif pi_account is not null then
      open res for
        select j.juristic_id            ce_id,
               null                     ce_sex,
               null                     ce_is_vip,
               1                        ce_is_corp,
               null                     ce_birthday,
               null                     ce_psp_series,
               null                     ce_psp_number,
               null                     ce_psp_issue_date,
               null                     ce_psp_issuer,
               j.jur_name               ce_name,
               j.jur_name               ce_surname,
               null                     ce_givname,
               null                     ce_taxid,
               j.jur_ogrn               ce_ogrn,
               j.jur_kpp                ce_kpp,
               j.jur_okpo               ce_okpo,
               j.jur_okved              ce_okved,
               b.bnk_id                 ce_bank_code,
               b.bnk_name               ce_bank_name,
               null                     ce_branch_name,
               j.jur_threasury_name     ce_treasury_name,
               j.jur_threasury_acc      ce_treasury_acc,
               j.jur_corr_akk           ce_corr_acc, -- корр
               j.jur_settlement_account ce_settl_acc, -- расч. счет
               null                     cm_is_person,
               null                     cm_category_id,
               null                     cm_category_name,
               j.jur_name_short         ch_fullname,
               c.ver                    ch_ver_num,
               1                        ch_state,
               null                     law_ad_raw_text,
               null                     law_ad_region,
               null                     law_ad_district,
               a1.addr_city             law_ad_city,
               null                     law_ad_settle,
               a1.addr_street           law_ad_street,
               null                     law_ad_home,
               a1.addr_building         law_ad_build,
               a1.addr_office           law_ad_flat,
               null                     law_ad_zip,
               null                     fact_ad_raw_text,
               null                     fact_ad_region,
               null                     fact_ad_district,
               a2.addr_city             fact_ad_city,
               null                     fact_ad_settle,
               a2.addr_street           fact_ad_street,
               a2.addr_corp             fact_ad_home,
               a2.addr_building         fact_ad_build,
               a2.addr_office           fact_ad_flat,
               null                     fact_ad_zip,
               null                     balance,
               null                     Stat_PSTN,
               null                     Tar_cnt
          from T_CLIENTS C
          join t_juristic j on c.fullinfo_id = j.juristic_id
          left join T_ADDRESS A1 on A1.ADDR_ID = j.jur_address_id
          left join T_ADDRESS A2 on A2.ADDR_ID = j.jur_fact_address_id
          left join t_abonent ab on c.client_id = ab.client_id
          left join t_tmc_sim sim on ab.ab_tmc_id = sim.tmc_id
          left join banks b on b.bnk_bik = j.jur_bank_bik
         where (pi_account is null or ab.client_account = pi_account);
    end if;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      \*open res for
      select 1 from dual where 1 <> 1;*\
      return null;
  end;*/
  ----------------------------------------------------------------------------

  ------------------------------------------------------------------------------------------------
  -- Проверка параметров списка подключений
  ------------------------------------------------------------------------------------------------
  function Check_Abonent_tab(pi_abonent_tab in abonent_tab,
                             pi_worker_id   in T_USERS.USR_ID%type,
                             po_errors      out sys_refcursor,
                             po_err_num     out pls_integer,
                             po_err_msg     out t_Err_Msg) return number is
    res          number := 1;
    l_errors     varchar2(1000);
    l_errors_tab string_tab := string_tab();
  begin
    savepoint sp_Check_Abonent_tab;
    logging_pkg.info('Вошли -- ' || pi_abonent_tab.count,
                     'Check_Abonent_tab');
    For oraRecord in (Select abonent_type(t.abonent_id, -- идентификатор
                                          t.is_bad, -- принак ошибочного
                                          t.client_id, -- ид клиента
                                          t.org_id, -- ид организации
                                          t.phone_make,
                                          t.phone_model,
                                          t.phone_imei,
                                          t.status, -- статус абонента
                                          t.paid, -- оплачено
                                          t.cost, -- расчётная стоимость по тарифу
                                          t.tar_id, -- ид тарифа
                                          t.tar_name, -- наименование тарифа (для ошибочного)
                                          t.imsi, -- imsi sim-карты
                                          t.callsign, -- msisdn
                                          t.citynum, -- городской номер (или пусто)
                                          t.simcolor, -- цвет номера
                                          t.err_code, -- код ошибки
                                          t.err_msg, -- сообщение об ошибке
                                          t.cc_id, -- ид расчётного центра
                                          t.dog_date, -- дата договора
                                          t.user_id, -- ид пользователя
                                          t.note, -- примечание
                                          /*Для массовой проверки признак связанности поднимаю из БД, на основании данных в СИМ*/
                                          (Case
                                            when ts.is_related is Null then
                                             0
                                            when ts.is_related = 2 then
                                             0
                                            else
                                             ts.is_related
                                          end), -- признак связанной или несвязаннной карты
                                          t.equipment_required, -- признак продажи с оборудованием (тип оборудования или 0 или null)
                                          t.equipment_tmc_id,
                                          t.equipment_model_id, -- ид модели оборудования
                                          t.equipment_cost,
                                          t.client_account,
                                          -- 51312 Бюджетирование
                                          t.limit_type,
                                          t.limit_warning,
                                          t.limit_block,
                                          t.limit_date_start,
                                          t.limit_date_end,
                                          t.limit_client_id,
                                          t.seller_active_id,

                                          wish_date,
                                          agree_work_info,
                                          agree_cancel_dog,
                                          agree_pay_off_debt,
                                          ATTACHED_DOC,
                                          NUMBER_SHEETS,
                                          usb_ser,
                                          TRANSFER_PHONE,
                                          OPERATION_DONOR,
                                          NPID,
                                          TYPE_PROCESS_CBDPN,
                                          operator_recipient,
                                          client_type,
                                          channel_id,
                                          CODE_WORD,
                                          OUT_ACCOUNT,

                                          t.kl_region_id,

                                          CREDIT_LIMIT,
                                          ALARM_LIMIT,
                                          IS_PERSONAL_ACCOUNT,
                                          reg_id_ps,
                                          usl_number,
                                          fix_address_id,
                                          FIX_NAME,
                                          t.ab_comment,
                                          t.req_delivery_id,
                                          t.request_id,
                                          t.consent_msg,
                                          t.method_connect) qwe,
                             -- 40261 Брендированное оборудование
                             tmc_phone_tab(tmc_phone_type(t.phone_make,
                                                          t.phone_model,
                                                          null)) phone
                        from table(pi_abonent_tab) t
                        left join t_tmc_sim ts
                          on ts.sim_imsi = t.imsi) loop
      res := res * Check_Abonent(oraRecord.qwe,
                                 pi_worker_id,
                                 -- 40261 Брендированное оборудование
                                 oraRecord.phone,
                                 0,
                                 l_errors,
                                 po_err_num,
                                 po_err_msg);
      If Trim(l_errors) is not Null then
        l_errors_tab.extend;
        l_errors_tab(l_errors_tab.count()) := l_errors;
      End If;
      if NVL(po_err_num, 0) <> 0 then
        open po_errors for
          select /*+ PRECOMPUTE_SUBQUERY */
           *
            from TABLE(l_errors_tab);
        return 0;
      End If;
    End Loop;
    open po_errors for
      Select * from table(l_errors_tab);
    return res;
  exception
    when no_data_found then
      open po_errors for
        Select * from table(l_errors_tab);
      po_err_num := -1;
      po_err_msg := 'Данные не найдены' || ' ' ||
                    dbms_utility.format_error_backtrace;
      rollback to sp_Check_Abonent_tab;
      return 0;
    when others then
      open po_errors for
        Select * from table(l_errors_tab);
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      rollback to sp_Check_Abonent_tab;
      return 0;
  End;
  ----------------------------------------------------------------------------
  ----------------------------------------------------------------------------
  --функция поиска Клиента Юридического лица по ID клиента.
  ----------------------------------------------------------------------------
  Function get_jur_inf_by_ClientID(pi_client_id in varchar2,
                                   --pi_worker_id in number,
                                   po_err_num out pls_integer,
                                   po_err_msg out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select j.juristic_id            ce_id,
             null                     ce_sex,
             null                     ce_is_vip,
             1                        ce_is_corp,
             null                     ce_birthday,
             null                     ce_psp_series,
             null                     ce_psp_number,
             null                     ce_psp_issue_date,
             null                     ce_psp_issuer,
             j.jur_name               ce_name,
             j.jur_name               ce_surname,
             null                     ce_givname,
             j.jur_inn                ce_taxid,
             j.jur_ogrn               ce_ogrn,
             j.jur_kpp                ce_kpp,
             j.jur_okpo               ce_okpo,
             j.jur_okved              ce_okved,
             b.bnk_id                 ce_bank_code,
             b.bnk_name               ce_bank_name,
             null                     ce_branch_name,
             j.jur_threasury_name     ce_treasury_name,
             j.jur_threasury_acc      ce_treasury_acc,
             j.jur_corr_akk           ce_corr_acc, -- корр
             j.jur_settlement_account ce_settl_acc, -- расч. счет
             null                     cm_is_person,
             null                     cm_category_id,
             null                     cm_category_name,
             j.jur_name_short         ch_fullname,
             c.ver                    ch_ver_num,
             1                        ch_state,
             null                     law_ad_raw_text,
             a1.region_id             law_ad_region,
             null                     law_ad_district,
             a1.addr_city             law_ad_city,
             null                     law_ad_settle,
             a1.addr_street           law_ad_street,
             null                     law_ad_home,
             a1.addr_building         law_ad_build,
             a1.addr_office           law_ad_flat,
             null                     law_ad_zip,
             null                     fact_ad_raw_text,
             a2.region_id             fact_ad_region,
             null                     fact_ad_district,
             a2.addr_city             fact_ad_city,
             null                     fact_ad_settle,
             a2.addr_street           fact_ad_street,
             a2.addr_corp             fact_ad_home,
             a2.addr_building         fact_ad_build,
             a2.addr_office           fact_ad_flat,
             null                     fact_ad_zip,
             null                     balance,
             null                     Stat_PSTN,
             null                     Tar_cnt,
             a1.addr_block            law_addr_block,
             a1.addr_structure        law_addr_structure,
             a1.addr_fraction         law_addr_fraction,
             a2.addr_block            fact_addr_block,
             a2.addr_structure        fact_addr_structure,
             a2.addr_fraction         fact_addr_fraction
        from T_CLIENTS C
        join t_juristic j
          on c.fullinfo_id = j.juristic_id
        left join T_ADDRESS A1
          on A1.ADDR_ID = j.jur_address_id
        left join T_ADDRESS A2
          on A2.ADDR_ID = j.jur_fact_address_id
      /*left join t_abonent ab
        on c.client_id = ab.client_id
      left join t_tmc_sim sim
        on ab.ab_tmc_id = sim.tmc_id*/
        left join banks b
          on b.bnk_bik = j.jur_bank_bik
       where c.CLIENT_ID = pi_client_id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := po_err_msg || ' ' || sqlerrm || ' ' ||
                    dbms_utility.format_error_backtrace;
      return null;
  End;
  ----------------------------------------------------------------------------
  function get_abonent_cg(pi_worker_id in number,
                          pi_ab_id     in number,
                          po_err_num   out pls_integer,
                          po_err_msg   out varchar2) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select acg.abonent_id, acg.cg_id, acg.cg_cat, acg.cg_name
        from t_abonent_cg acg
       where acg.abonent_id = pi_ab_id
      union
      select bcg.abonent_id, bcg.cg_id, bcg.cg_cat, bcg.cg_name
        from t_abon_bad_cg bcg
       where bcg.abonent_id = pi_ab_id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  ----------------------------------------------------------------------------
  function Get_Abonent_Status(pi_ab_id  in number,
                              pi_tmc_id in number, --обязат.параметр

                              pi_usl_id    in number,
                              pi_client_id in number,
                              pi_worker_id in number,
                              po_err_num   out pls_integer,
                              po_err_msg   out varchar2) return number is
    res number;
  begin
    select t.ab_status
      into res
      from t_abonent t
     where t.ab_tmc_id = pi_tmc_id
       and t.is_deleted = 0
       and (pi_ab_id is null or t.ab_id = pi_ab_id)
       and (pi_client_id is null or t.client_id = pi_client_id)
       and (pi_usl_id is null or t.usl_id = pi_usl_id);
    return res;
  exception
    when no_data_found then
      select max(t.ab_status) keep(dense_rank last order by t.ab_mod_date)
        into res
        from t_abonent t
       where t.ab_tmc_id = pi_tmc_id
         and t.is_deleted = 1
         and (pi_ab_id is null or t.ab_id = pi_ab_id)
         and (pi_client_id is null or t.client_id = pi_client_id)
         and (pi_usl_id is null or t.usl_id = pi_usl_id);
      return res;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace ||
                    '. pi_tmc_id = ' || pi_tmc_id;
      return null;
  end;
  ----------------------------------------------------------------------------

  ------------------------------------------------------------------------------
  -- Получение маркетингового предложения для абонента
  ------------------------------------------------------------------------------
  function get_mark_prop_by_number(pi_phone_num in t_callsign.federal_callsign%type,
                                   pi_worker_id in number,
                                   po_err_num   out pls_integer,
                                   po_err_msg   out varchar2) return varchar2 is
    res t_marketing_proposal.packet_text%type;
  begin

    select t.packet_text
      into res
      from t_marketing_proposal t
     where t.msisdn = pi_phone_num;

    return res;

  exception
    when no_data_found then
      po_err_num := '1';
      po_err_msg := 'Маркетинговое предложение для данного абонента отсутствует';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
  end get_mark_prop_by_number;

  ------------------------------------------------------------------------------
  -- 44357 Сохранение пакетного подключения GSM для физиков
  ------------------------------------------------------------------------------
  function Save_Abonent_Ph(Pi_Client_id in number,
                           Pi_org_id    in Number,
                           pi_abonent   in abonent_tab_ph,
                           pi_savetype  in number,
                           pi_worker_id in T_USERS.USR_ID%type,
                           pi_cost      in number,
                           pi_Pack_Id   in number,
                           pi_is_common in number,
                           po_is_ok     out number,
                           po_err_num   out pls_integer,
                           po_err_msg   out varchar2) return sys_refcursor is
  begin
    return Save_Abonent_Ph(Pi_Client_id,
                           Pi_org_id,
                           pi_abonent,
                           pi_savetype,
                           pi_worker_id,
                           pi_cost,
                           pi_Pack_Id,
                           pi_is_common,
                           0,
                           po_is_ok,
                           po_err_num,
                           po_err_msg);
  end;
  function Save_Abonent_Ph(Pi_Client_id   in number,
                           Pi_org_id      in Number,
                           pi_abonent     in abonent_tab_ph,
                           pi_savetype    in number,
                           pi_worker_id   in T_USERS.USR_ID%type,
                           pi_cost        in number,
                           pi_Pack_Id     in number,
                           pi_is_common   in number,
                           pi_is_alien_ab in number,
                           po_is_ok       out number,
                           po_err_num     out pls_integer,
                           po_err_msg     out varchar2) return sys_refcursor is
    res1      sys_refcursor;
    res       num_tab := Num_Tab();
    l_Abonent abonent_type;
    l_Is_Ok   number;
    l_pack_Id number := Pi_Pack_Id;
    ex_count exception;
    l_str varchar2(4000);
  BEGIN
    logging_pkg.info('Вошли. Pi_org_id: ' || Pi_org_id || ';
    Pi_Client_id: ' || Pi_Client_id || ';
    pi_worker_id: ' || pi_worker_id || ';
    pi_abonent.Count: ' || pi_abonent.Count || ';
    pi_savetype: ' || pi_savetype || ';
    Pi_paid: ' || pi_abonent(1).paid || ';
    pi_cost: ' || pi_cost || ';
    pi_pack_id ' || pi_Pack_Id,
                     'Save_Abonent_Ph');

    if pi_abonent.Count > 99 then
      raise ex_count;
    end if;

    If l_pack_Id Is Null then
      Insert into t_ab_package
        (package_id)
      values
        (Null)
      returning package_id into l_pack_Id;
    End If;

    begin
      select listagg(abonent_id || ';' || is_bad || ';' || phone_make || ';' ||
                     phone_model || ';' || phone_imei || ';' || status || ';' || paid || ';' || cost || ';' ||
                     tar_id || ';' || tar_name || ';' || imsi || ';' ||
                     callsign || ';' || citynum || ';' || simcolor || ';' ||
                     err_code || ';' || err_msg || ';' || kl_region_id || ';' || note || ';' ||
                     user_id || ';' || related || ';' || equipment_required || ';' ||
                     equipment_model_id || ';' || equipment_cost || ';' ||
                     Account_Id || ';' || client_account || ';' ||
                     limit_type || ';' || limit_warning || ';' ||
                     limit_block || ';' || limit_date_start || ';' ||
                     limit_date_end || ';' || client_id || ';' ||
                     seller_active_id || ';' || ATTACHED_DOC || ';' ||
                     NUMBER_SHEETS || ';' || usb_ser || ';' || WISH_DATE || ';' ||
                     AGREE_WORK_INFO || ';' || AGREE_CANCEL_DOG || ';' ||
                     AGREE_PAY_OFF_DEBT || ';' || TRANSFER_PHONE || ';' ||
                     OPERATION_DONOR || ';' || NPID || ';' ||
                     TYPE_PROCESS_CBDPN,
                     ',') within group(order by abonent_id)
        into l_str
        from table(pi_abonent);
      logging_pkg.info(l_str, 'Save_Abonent_Ph');
    exception
      when others then
        logging_pkg.error('log', 'Save_Abonent_Ph');
    end;

    For OraRecord in (Select t.*, ts.is_related, RowNum rn
                        from Table(pi_abonent) t
                        left join t_tmc_sim ts
                          on ts.sim_imsi = t.imsi
                          or ts.sim_iccid = t.imsi) loop

      logging_pkg.info('abonent_id  =>' || OraRecord.abonent_id || ',
    is_bad      =>' || OraRecord.is_bad || ',
    phone_make  =>' || OraRecord.phone_make || ',
    phone_model =>' || OraRecord.phone_model || ',
    phone_imei  =>' || OraRecord.phone_imei || ',
    status      =>' || OraRecord.status || ',
    paid        =>' || OraRecord.paid || ',
    cost        =>' || OraRecord.cost || ',
    tar_id      =>' || OraRecord.tar_id || ',
    tar_name    =>' || OraRecord.tar_name || ',
    imsi        =>' || OraRecord.imsi || ',
    callsign    =>' || OraRecord.callsign || ',
    citynum     =>' || OraRecord.citynum || ',
    simcolor    =>' || OraRecord.simcolor || ',
    err_code    =>' || OraRecord.err_code || ',
    err_msg     =>' || OraRecord.err_msg || ',
    kl_region_id       =>' || OraRecord.kl_region_id || ',
    note        =>' || OraRecord.note || ',
    related     =>' || OraRecord.related || ',
    is_related  =>' || OraRecord.is_related || ',
    equipment_required  =>' ||
                       OraRecord.equipment_required || ',
    equipment_model_id  =>' ||
                       OraRecord.equipment_model_id || ',
    equipment_cost      =>' ||
                       OraRecord.equipment_cost || ',
    services    =>' || OraRecord.services.Count || ',
    Account_Id  =>' || OraRecord.Account_Id || ',
    client_account  =>' || OraRecord.client_account || ',
    seller_active_id =>' ||
                       OraRecord.seller_active_id,
                       'Save_Abonent_Ph');
      l_Abonent := abonent_type(OraRecord.abonent_id,
                                OraRecord.is_bad,
                                Pi_Client_id,
                                Pi_org_id,
                                OraRecord.phone_make,
                                OraRecord.phone_model,
                                OraRecord.phone_imei,
                                OraRecord.status,
                                OraRecord.paid,
                                OraRecord.cost,
                                OraRecord.tar_id,
                                OraRecord.tar_name,
                                OraRecord.imsi,
                                OraRecord.callsign,
                                (Case
                                  when OraRecord.citynum = 0 then
                                   Null
                                  else
                                   OraRecord.citynum
                                end),
                                OraRecord.simcolor,
                                OraRecord.err_code,
                                OraRecord.err_msg,
                                OraRecord.cc_id,
                                sysdate,
                                OraRecord.user_id,
                                OraRecord.note,
                                (Case
                                  when OraRecord.is_related is Null then
                                   0
                                  when OraRecord.is_related = 2 then
                                   0
                                  else
                                   OraRecord.is_related
                                end), -- признак связанной или несвязаннной карты
                                OraRecord.equipment_required,
                                null,
                                OraRecord.equipment_model_id,
                                OraRecord.equipment_cost,
                                OraRecord.client_account,
                                -- 51312 Бюджетирование
                                OraRecord.limit_type,
                                OraRecord.limit_warning,
                                OraRecord.limit_block,
                                OraRecord.limit_date_start,
                                OraRecord.limit_date_end,
                                OraRecord.client_id,
                                --51299 - активные продавцы
                                OraRecord.seller_active_id,
                                OraRecord.wish_date,
                                OraRecord.agree_work_info,
                                OraRecord.agree_cancel_dog,
                                OraRecord.agree_pay_off_debt,
                                OraRecord.ATTACHED_DOC,
                                OraRecord.NUMBER_SHEETS,
                                OraRecord.usb_ser,
                                OraRecord.TRANSFER_PHONE,
                                OraRecord.OPERATION_DONOR,
                                OraRecord.NPID,
                                OraRecord.TYPE_PROCESS_CBDPN,
                                OraRecord.operator_recipient,
                                'P',
                                OraRecord.channel_id,
                                OraRecord.CODE_WORD,
                                OraRecord.OUT_ACCOUNT,
                                OraRecord.KL_REGION_ID,
                                OraRecord.CREDIT_LIMIT,
                                OraRecord.ALARM_LIMIT,
                                OraRecord.IS_PERSONAL_ACCOUNT,
                                OraRecord.reg_id_ps,
                                OraRecord.usl_number,
                                OraRecord.fix_address_id,
                                OraRecord.fix_name,
                                OraRecord.ab_comment,
                                OraRecord.req_delivery_id,
                                OraRecord.request_id,
                                OraRecord.consent_msg,
                                null);
      l_Is_Ok   := Save_Abonent3(pi_abonent   => l_Abonent,
                                 pi_services  => OraRecord.services,
                                 pi_savetype  => pi_savetype,
                                 pi_worker_id => pi_worker_id,
                                 pi_cost      => OraRecord.cost,
                                 pi_is_common => pi_is_common,
                                 -- 40261 Брендированное оборудование
                                 pi_tmc_phone   => OraRecord.tmc_phone,
                                 pi_is_alien_ab => pi_is_alien_ab,
                                 po_is_ok       => po_is_ok,
                                 po_err_num     => po_err_num,
                                 po_err_msg     => po_err_msg);
      If NVL(po_err_num, 0) <> 0 then
        logging_pkg.info(po_err_msg, 'Save_Abonent_Ph');
      End If;

      res.extend;
      If po_is_ok > 0 then
        res(res.Count) := l_Is_Ok;
        begin
          Update t_abonent ab
             set ab.client_remote_id = Decode(OraRecord.Account_Id,
                                              0,
                                              null,
                                              -1,
                                              null,
                                              OraRecord.Account_Id),
                 ab.package_id       = l_pack_Id,
                 ab.correlationid    = decode(pi_is_alien_ab,
                                              1,
                                              l_pack_id,
                                              null)
           Where ab.ab_id = l_Is_Ok;
          Update t_abon_bad ab
             set ab.package_id    = l_pack_Id,
                 ab.correlationid = decode(pi_is_alien_ab,
                                           1,
                                           l_pack_id,
                                           null)
           Where ab.id = l_Is_Ok;

          if pi_is_alien_ab = 1 then
            if OraRecord.abonent_id is null then
              insert into t_abonent_out_status
                (AB_ID, STATUS, WORKER_ID, TEXT, action)
              values
                (l_pack_Id,
                 decode(OraRecord.status, 96, 6, 97, 5, 0),
                 pi_worker_id,
                 null,
                 'CreateRequest');
            else
              insert into t_abonent_out_status
                (AB_ID, STATUS, WORKER_ID, TEXT, action)
              values
                (l_pack_Id,
                 decode(OraRecord.status, 96, 6, 97, 5, 0),
                 pi_worker_id,
                 null,
                 'UpdateRequest');
            end if;
          end if;
        exception
          when others then
            null;
        End;
      Else
        /*res(res.Count) := -1;*/
        res(res.Count) := l_Is_Ok;
        Update t_abon_bad ab
           set ab.package_id    = l_pack_Id,
               ab.correlationid = decode(pi_is_alien_ab, 1, l_pack_id, null)
         Where ab.id = l_Is_Ok;
      End If;
      po_is_ok := Null;
    End Loop;

    /*Update t_abonent ab
      set ab.ab_status = Decode(RowNum, 1, ab.ab_status, 102)
    Where ab.ab_status = 102
      and ab.Package_Id = l_pack_Id
      and ab.client_remote_id is Null;*/

    Open res1 for
      select /*+ PRECOMPUTE_SUBQUERY */
       column_value,
       case
         when ab.id is not null then
          0
         else
          1
       end is_ok
        from TABLE(res)
        left join t_abonent a
          on a.ab_id = column_value
        left join t_abon_bad ab
          on ab.id = column_value;

    return res1;

  exception
    when ex_count then
      po_err_num := 1;
      po_err_msg := 'Количество подключений в пакете не должно быть больше 99.';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg || ' ' || po_err_msg,
                        c_package || 'Save_Abonent_Ph');
      return null;
  End Save_Abonent_Ph;

  --------------------------------------------------------------------------------------
  -- 58952 Перевызов
  --------------------------------------------------------------------------------------
  function Get_Abonent_List_GSM_CDMA(pi_org_id       in T_ORGANIZATIONS.ORG_ID%type,
                                     pi_client_type  in char,
                                     pi_vdvd_type    in T_TARIFF2.TYPE_VDVD_ID%type,
                                     pi_pay_type     in T_TARIFF2.PAY_TYPE%type,
                                     pi_tariff_type  in T_TARIFF2.TARIFF_TYPE%type,
                                     pi_date_0       in T_ABONENT.AB_REG_DATE%type,
                                     pi_date_1       in T_ABONENT.AB_REG_DATE%type,
                                     pi_with_self    in number, -- включать pi_org_id
                                     pi_with_cur     in number, -- включать курируемых
                                     pi_with_cur_sub in number, -- включать курируемых субдиллерами
                                     pi_worker_id    in T_USERS.USR_ID%type,
                                     po_err_num      out pls_integer,
                                     po_err_msg      out t_Err_Msg,
                                     --pi_cc_id        in T_CALC_CENTER.CC_ID%type := null,
                                     pi_ab_st      in num_tab := num_tab(),
                                     pi_date_type  in number := 1, --0 - дата заключения договора , 1 - дата обработки в АСР , 2 - дата активности
                                     pi_gen_num    in number := 0,
                                     pi_is_related in number) -- 1-связанные, 2-несвязанные, 0-все вместе
   return sys_refcursor is
  begin
    return Get_Abonent_List_GSM_CDMA(array_num_2(rec_num_2(pi_org_id, 1)),
                                     0,
                                     null,
                                     pi_client_type,
                                     pi_vdvd_type,
                                     pi_pay_type,
                                     pi_tariff_type,
                                     pi_date_0,
                                     pi_date_1,
                                     pi_worker_id,
                                     po_err_num,
                                     po_err_msg,
                                     --pi_cc_id,
                                     pi_ab_st,
                                     pi_date_type,
                                     pi_gen_num,
                                     pi_is_related,
                                     null,
                                     null,
                                     null,
                                     null,
                                     null);
  end Get_Abonent_List_GSM_CDMA;

  -----------------------------------------------------------------------------
  -- Список подключений GSM/CDMA
  -- dresvyannikov - 44926
  -------------------------------------------------------------------------------
  function Get_Abonent_List_GSM_CDMA(pi_org_tab      in array_num_2,
                                     pi_block        in number,
                                     pi_org_relation in num_tab,
                                     pi_client_type  in char,
                                     pi_vdvd_type    in T_TARIFF2.TYPE_VDVD_ID%type,
                                     pi_pay_type     in T_TARIFF2.PAY_TYPE%type,
                                     pi_tariff_type  in T_TARIFF2.TARIFF_TYPE%type,
                                     pi_date_0       in T_ABONENT.AB_REG_DATE%type,
                                     pi_date_1       in T_ABONENT.AB_REG_DATE%type,
                                     pi_worker_id    in T_USERS.USR_ID%type,
                                     po_err_num      out pls_integer,
                                     po_err_msg      out t_Err_Msg,
                                     --pi_cc_id        in T_CALC_CENTER.CC_ID%type := null,
                                     pi_ab_st in num_tab := num_tab(),
                                     -- Задача № 87903. дата заключения договора = ab_reg_date
                                     pi_date_type  in number := 1, --1 - дата заключения договора , 0 - дата обработки в АСР , 2 - дата активности
                                     pi_gen_num    in number := 0,
                                     pi_is_related in number, -- 1-связанные, 2-несвязанные, 0-все вместе
                                     -- 58459
                                     pi_callsign      in varchar2,
                                     pi_sim_num       in varchar2,
                                     pi_ab_lastname   in varchar2,
                                     pi_ab_firstname  in varchar2,
                                     pi_ab_middlename in varchar2,
                                     pi_channel_tab   in num_tab := num_tab())
    return sys_refcursor is
    res              sys_refcursor;
    cl_type          char := null;
    l_org_id         pls_integer := null;
    org_tab          num_tab := num_tab();
    l_gen_doc_number t_Err_Msg := '';
    for_dealer       pls_integer;
    l_tab_ab_id      num_tab := null;
    l_tab_ab_id_bad  num_tab := null;
    query_select     varchar2(2000);

    c_is_related constant number := pi_is_related;
    c_pay_type   constant number := pi_pay_type;
    c_date_0     constant date := pi_date_0;
    c_date_1     constant date := pi_date_1;
    --c_cc_id      constant number := pi_cc_id;
    l_user_rtk boolean;
    l_user_rtm boolean;

  begin

    logging_pkg.debug('pi_org_id=' || ', pi_client_type=' ||
                      pi_client_type || ', pi_vdvd_type=' || pi_vdvd_type ||
                      ', pi_pay_type=' || pi_pay_type ||
                      ', pi_tariff_type=' || pi_tariff_type ||
                      ', pi_date_0=' || pi_date_0 || ', pi_date_1=' ||
                      pi_date_1 || ', pi_worker_id=' || pi_worker_id ||
                      /*', pi_cc_id=' || pi_cc_id || */
                      ', pi_ab_st=' || get_str_by_num_tab(pi_ab_st) ||
                      ', pi_date_type=' || pi_date_type || ', pi_gen_num=' ||
                      pi_gen_num || ', pi_is_related=' || pi_is_related ||
                      ', pi_callsign=' || pi_callsign || ', pi_sim_num=' ||
                      pi_sim_num || ', pi_ab_lastname=' || pi_ab_lastname ||
                      ', pi_ab_firstname=' || pi_ab_firstname ||
                      ', pi_ab_middlename=' || pi_ab_middlename,
                      'Get_Abonent_List_GSM_CDMA');

    if (Orgs.Is_Have_USI_Job(pi_worker_id) +
       Orgs.Is_Have_SP_Job(pi_worker_id) = 0) then
      for_dealer := 1;
    else
      for_dealer := 0;
    end if;
    -- checking access for operation for specified user
    if (not Security_pkg.Check_User_Right_str('EISSD.CONNECTIONS.GSM.LIST',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;

    /*if (pi_org_tab is not null and pi_org_tab.count > 0) then
      if (not Security_pkg.Check_Rights_Orgs_str('EISSD.CONNECTIONS.GSM.LIST',
                                                 get_num_tab_by_array_num_2(pi_org_tab),
                                                 pi_worker_id,
                                                 po_err_num,
                                                 po_err_msg)) then
        return null;
      end if;
    end if;*/

    if ((pi_client_type is not null) and
       (pi_client_type <> c_client_person) and
       (pi_client_type <> c_client_juristic)) then
      cl_type := null;
    else
      cl_type := pi_client_type;
    end if;

    if (pi_gen_num = 1) then
      l_gen_doc_number := Report.Get_Dogovor_Number(l_org_id, 8000);
    end if;

    if (pi_vdvd_type = 51 or pi_vdvd_type = 53) then

      -- 58952
      if pi_callsign is not null or pi_sim_num is not null or
         pi_ab_firstname is not null or pi_ab_lastname is not null or
         pi_ab_middlename is not null then

        /*org_tab := get_orgs_tab_for_multiset(pi_orgs => pi_org_tab,
        Pi_worker_id => pi_worker_id,
        pi_block => pi_block,
        pi_org_relation => pi_org_relation,
        pi_is_rtmob => 1);*/

        org_tab := ORGS.get_user_orgs_by_prm(pi_worker_id => pi_worker_id,
                                             pi_rel_tab   => null,
                                             pi_prm_tab   => num_tab(2000),
                                             pi_block     => 0, -- Когда поиск по параметрам, надо учитывать все
                                             po_err_num   => po_err_num,
                                             po_err_msg   => po_err_msg);

        --находим валидные подключения
        query_select := 'select ab.ab_id from t_abonent ab
                join t_tmc_sim t
                     on (ab.ab_tmc_id = t.tmc_id)
                join  t_clients c
                     on (ab.client_id = c.client_id)
                left join t_person p
                  on p.PERSON_ID = c.FULLINFO_ID
                left join t_juristic j
                  on j.JURISTIC_ID = c.FULLINFO_ID
                left join mv_org_tree m on m.org_id = ab.org_id
                left join t_dogovor d on d.org_rel_id = m.root_rel_id';

        query_select := query_select ||
                        ' where ab.org_id in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_org_tab))
                            and (ab.dog_id is null or ab.dog_id = d.dog_id)
                  and ab.is_deleted = 0
                  and nvl(ab.type_request,0) in (0,1)';

        if pi_callsign is not null then
          query_select := query_select || ' and t.sim_callsign = ''' ||
                          pi_callsign || '''';
        end if;

        if pi_sim_num is not null then
          if length(pi_sim_num) > 15 then
            -- iccid
            query_select := query_select || ' and t.SIM_ICCID = ''' ||
                            pi_sim_num || '''';
          else
            -- imsi
            query_select := query_select || ' and t.SIM_IMSI = ''' ||
                            pi_sim_num || '''';
          end if;
        end if;

        if pi_ab_firstname is not null then
          query_select := query_select ||
                          ' and upper(p.person_firstname) = ''' ||
                          upper(pi_ab_firstname) || '''';
        end if;

        if pi_ab_lastname is not null then
          query_select := query_select ||
                          ' and upper(p.person_lastname) = ''' ||
                          upper(pi_ab_lastname) || '''';
        end if;

        if pi_ab_middlename is not null then
          query_select := query_select ||
                          ' and upper(p.person_middlename) = ''' ||
                          upper(pi_ab_middlename) || '''';
        end if;

        ----------------- !!!
        --      logging_pkg.dump(query_select);
        ----------------- !!!

        execute immediate query_select bulk collect
          into l_tab_ab_id
          using org_tab;

        query_select := null;
        --находим ошибочные подключения, в случае если фильтруем по дате заключения договора(в остальных случаях ошибочные подключения не выводим)
        query_select := 'select distinct ab.id from T_ABON_BAD ab
                join t_clients c
                     on (ab.id_client = c.client_id)
                left join t_person p
                  on p.PERSON_ID = c.FULLINFO_ID
                left join t_juristic j
                  on j.JURISTIC_ID = c.FULLINFO_ID
                left join mv_org_tree m
                  on m.org_id = ab.id_org
                left join t_dogovor d
                  on d.org_rel_id = m.root_rel_id
                where ab.id_org in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_org_tab))
                  and (ab.dog_id is null or ab.dog_id = d.dog_id)
                  and ab.is_deleted = 0
                  and nvl(ab.type_req,0) in (0,1)';

        if pi_callsign is not null then
          query_select := query_select || ' and ab.CALLSIGN = ''' ||
                          pi_callsign || '''';
        end if;

        if pi_sim_num is not null then
          query_select := query_select || ' and ab.IMSI = ''' || pi_sim_num || '''';
        end if;

        if pi_ab_firstname is not null then
          query_select := query_select ||
                          ' and upper(p.person_firstname) = ''' ||
                          upper(pi_ab_firstname) || '''';
        end if;

        if pi_ab_lastname is not null then
          query_select := query_select ||
                          ' and upper(p.person_lastname) = ''' ||
                          upper(pi_ab_lastname) || '''';
        end if;

        if pi_ab_middlename is not null then
          query_select := query_select ||
                          ' and upper(p.person_middlename) = ''' ||
                          upper(pi_ab_middlename) || '''';
        end if;

        ----------------- !!!
        --      logging_pkg.dump(query_select);
        ----------------- !!!

        execute immediate query_select bulk collect
          into l_tab_ab_id_bad
          using org_tab;

      else
        org_tab := get_orgs_tab_for_multiset(pi_orgs         => pi_org_tab,
                                             Pi_worker_id    => pi_worker_id,
                                             pi_block        => pi_block,
                                             pi_org_relation => pi_org_relation,
                                             pi_is_rtmob     => 1);

        --находим валидные подключения
        query_select := 'select ab.ab_id from t_abonent ab
                join t_tmc_sim t
                     on (ab.ab_tmc_id = t.tmc_id)
                join  t_clients c
                     on (ab.client_id = c.client_id)
                join t_tarif_by_at_id tar
                     on (t.tar_id = tar.at_id)
                left join mv_org_tree m on m.org_id = ab.org_id
                left join t_dogovor d on d.org_rel_id = m.root_rel_id
                left join T_CALC_CENTER CC
                     on ab.cc_id = cc.cc_id
                left join t_dic_region REG
                      on cc.cc_region_id = reg.reg_id ';

        if nvl(pi_date_type, 1) in (0, 1) then
          query_select := query_select ||
                          ' where ab.org_id in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_org_tab))
                            and (ab.dog_id is null or ab.dog_id = d.dog_id)
                  and ab.is_deleted = 0 and ab.ab_status in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE (:l_ab_st))
                  and nvl(ab.type_request,0) in (0,1)
                  and ab.channel_id in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_channel_tab))';
        elsif (nvl(pi_date_type, 1) = 2) then
          query_select := query_select ||
                          ' join t_abonent_activated ab_ac
                                    on (ab.ab_id = ab_ac.abonent_id)
                                    where ab.org_id in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_org_tab))
                  and ab.is_deleted = 0 and ab.ab_status in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE (:l_ab_st))
                  and nvl(ab.type_request,0) in (0,1)
                  and ab.channel_id in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_channel_tab))';
        end if;

        if (pi_is_related <> 0) then
          query_select := query_select || ' and t.is_related = ' ||
                          c_is_related;
        end if;
        if (cl_type is not null) then
          query_select := query_select || ' and c.client_type = ''' ||
                          cl_type || '''';
        end if;
        if (pi_pay_type is not null) then
          query_select := query_select || ' and tar.pay_type = ' ||
                          c_pay_type;
        end if;

        if (nvl(pi_date_type, 1) = 0) then
          query_select := query_select ||
                          ' and ((ab.change_status_date is not null) and
                         (ab.change_status_date ) >= trunc(to_date(''' ||
                          c_date_0 ||
                          ''' , ''DD.MM.YY''))+ 2 / 24
                         and
                         ((ab.change_status_date is not null) and
                         (ab.change_status_date) < trunc(to_date(''' ||
                          c_date_1 || ''' , ''DD.MM.YY''))+ 2 / 24 + 1))'; /*+1*/
        end if;
        if (nvl(pi_date_type, 1) = 1) then
          query_select := query_select ||
                          ' and ((ab.ab_reg_date ) >= trunc(to_date(''' ||
                          c_date_0 ||
                          ''' , ''DD.MM.YY''))+ 2 / 24
                                             and (ab.ab_reg_date ) < trunc(to_date(''' ||
                          c_date_1 || ''' , ''DD.MM.YY''))+ 2 / 24 + 1)'; /*+1*/
        end if;
        if (nvl(pi_date_type, 1) = 2) then
          query_select := query_select ||
                          ' and ((ab_ac.data_activated ) >= trunc(to_date(''' ||
                          c_date_0 ||
                          ''' , ''DD.MM.YY''))+ 2 / 24
                                             and (ab_ac.data_activated ) < trunc(to_date(''' ||
                          c_date_1 || ''' , ''DD.MM.YY''))+ 2 / 24 + 1)'; /*+1*/
        end if;

        /*if (pi_cc_id is not null and pi_cc_id >= 0) then
          query_select := query_select || ' and AB.Cc_Id = ' || c_cc_id;
        end if;
        if (pi_cc_id is not null and pi_cc_id < 0) then
          query_select := query_select ||
                          'and (reg.reg_id is not null
                                              and ' ||
                          c_cc_id || ' = -100 * reg.reg_id) ';
        end if;*/

        ----------------- !!!
        --      logging_pkg.dump(query_select);
        ----------------- !!!

        execute immediate query_select bulk collect
          into l_tab_ab_id
          using org_tab, pi_ab_st, pi_channel_tab;

        query_select := null;
        --находим ошибочные подключения, в случае если фильтруем по дате заключения договора(в остальных случаях ошибочные подключения не выводим)
        if (nvl(pi_date_type, 1) = 1) then
          query_select := 'select ab.id from T_ABON_BAD ab
                join t_tarif_by_at_id tar
                     on (ab.id_tar = tar.at_id)
                join  t_clients c
                     on (ab.id_client = c.client_id)
                left join T_CALC_CENTER CC
                     on ab.id_cc = cc.cc_id
                left join t_dic_region REG
                      on cc.cc_region_id = reg.reg_id
                left join mv_org_tree m on m.org_id = ab.id_org
                left join t_dogovor d on d.org_rel_id = m.root_rel_id
                where ab.id_org in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_org_tab))
                  and (ab.dog_id is null or ab.dog_id = d.dog_id)
                  and ab.is_deleted = 0 and 101 in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_ab_st))
                  and nvl(ab.type_req,0) in (0,1)
                  and ((ab.reg_date) >= trunc(to_date(''' ||
                          c_date_0 ||
                          ''' , ''DD.MM.YY''))+ 2 / 24
                                             and (ab.reg_date ) < trunc(to_date(''' ||
                          c_date_1 ||
                          ''' , ''DD.MM.YY''))+ 2 / 24+1)
                  and ab.channel_id in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_channel_tab))';

          if (cl_type is not null) then
            query_select := query_select || ' and c.client_type = ''' ||
                            cl_type || '''';
          end if;

          /*if (c_cc_id is not null and c_cc_id >= 0) then
            query_select := query_select || ' and AB.id_cc = ' || c_cc_id;
          end if;
          if (c_cc_id is not null and c_cc_id < 0) then
            query_select := query_select ||
                            ' and (reg.reg_id is not null
                                              and ' ||
                            c_cc_id || ' = -100 * reg.reg_id) ';
          end if;*/
          if (pi_pay_type is not null) then
            query_select := query_select || ' and tar.pay_type = ' ||
                            c_pay_type;
          end if;
          if (pi_is_related <> 0) then
            query_select := query_select || ' and ab.is_related = ' ||
                            c_is_related;
          end if;

          ----------------- !!!
          --      logging_pkg.dump(query_select);
          ----------------- !!!

          execute immediate query_select bulk collect
            into l_tab_ab_id_bad
            using org_tab, pi_ab_st, pi_channel_tab;
        end if;
      end if;

      open res for
        select aa.*,
               (select cast(collect(rec_num_2(number_1 => s.id,
                                              number_2 => s.product_category)) as
                            array_num_2)
                  from tr_request_service s
                 where s.request_id = aa.request_id) req_services
          from (/*with tab as (select \*+ MATERIALIZE index (abo PK_T_ABONENT)*\
                              *
                               from T_ABONENT abo
                              where abo.ab_id in
                                    (select \*+ PRECOMPUTE_SUBQUERY *\
                                      *
                                       from TABLE(l_tab_ab_id)))*/
                 select distinct TT.ABONENT_ID,
                                 TT.CLIENT_ID,
                                 TT.CLIENT_TYPE,
                                 TT.PERSON_LASTNAME,
                                 TT.PERSON_FIRSTNAME,
                                 TT.PERSON_MIDDLENAME,
                                 TT.JUR_NAME,
                                 TT.REG_DATE,
                                 TT.MOD_DATE,
                                 TT.STATUS,
                                 TT.AB_STATUS_EX,
                                 TT.PAID,
                                 TT.COST,
                                 TT.ERR_CODE,
                                 TT.ERR_MSG,
                                 TT.WHICH_ORG_AB_NAME,
                                 TT.CONN_NAME,
                                 TT.TAR_NAME,
                                 TT.IMSI,
                                 TT.CS,
                                 TT.DOG_DATE,
                                 TT.IS_BAD,
                                 TT.AB_ID_BAD,
                                 tt.cc_id,
                                 TT.CC_NAME,
                                 tt.CHANGE_STATUS_DATE,
                                 TT.DOG_NUMBER,
                                 TT.ARCHIVE_MARK,
                                 TT.worker,
                                 TT.seller,
                                 tt.seller_emp_num,
                                 tt.is_common,
                                 tt.client_category,
                                 tt.billing_group,
                                 tt.billing_group_name,
                                 tt.data_activated,
                                 tt.model_equip,
                                 tt.seller_active_fio,
                                 tt.is_resident,
                                 tt.reg_deadline,
                                 tt.sim_type,
                                 tt.sim_type_name,
                                 tt.root_org_id,
                                 tt.root_org_name,
                                 tt.reg_id,
                                 tt.reg_name,
                                 tt.mrf_id,
                                 tt.name_mrf,
                                 tt.org_id,
                                 tt.tar_id,
                                 tt.status_name,
                                 tt.TRANSFER_PHONE,
                                 tt.channel_name,
                                 tt.out_account,
                                 tt.CREDIT_LIMIT,
                                 tt.ALARM_LIMIT,
                                 tt.ab_comment,
                                 tt.req_delivery_id,
                                 tt.request_id,
                                 tt.kl_reg_request,
                                 tt.client_account,
                                 tt.agr_num
                 -- валидные подключения GSM, CDMA
                   from (select tAB.AB_ID ABONENT_ID,
                                tAB.CLIENT_ID CLIENT_ID,
                                clients_c.client_type CLIENT_TYPE,
                                clients_p.person_lastname PERSON_LASTNAME,
                                clients_p.person_firstname PERSON_FIRSTNAME,
                                clients_p.person_middlename PERSON_MIDDLENAME,
                                clients_j.jur_name JUR_NAME,
                                tAB.AB_REG_DATE - 2 / 24 REG_DATE,
                                tAB.AB_MOD_DATE - 2 / 24 MOD_DATE,
                                tAB.AB_STATUS STATUS,
                                tAB.AB_STATUS_EX,
                                tAB.AB_PAID PAID,
                                tAB.AB_COST COST,
                                tAB.ERR_CODE ERR_CODE,
                                tAB.ERR_MSG ERR_MSG,

                                OOO.ORG_NAME WHICH_ORG_AB_NAME,
                                NVL2(NVL(SIM.TAR_ID, 0),
                                     (select CONCAT(DV1.DV_NAME,
                                                    CONCAT(' ',
                                                           CONCAT(DV3.DV_NAME,
                                                                  CONCAT('  ',
                                                                         DV2.DV_NAME))))
                                        from T_DIC_VALUES DV1,
                                             T_DIC_VALUES DV2,
                                             T_DIC_VALUES DV3
                                       where tar.type_vdvd_id = dv1.dv_id
                                         and tar.tariff_type = dv2.dv_id
                                         and tar.pay_type = dv3.dv_id),
                                     'не известен') CONN_NAME,
                                NVL2(NVL(SIM.TAR_ID, 0),
                                     tar.title || ' / ' || REG.KL_NAME,
                                     null) TAR_NAME,
                                NVL(SIM.SIM_IMSI, sim.sim_iccid) IMSI,
                                NVL(SIM.SIM_CALLSIGN, 0) CS,
                                tAB.AB_DOG_DATE - 2 / 24 DOG_DATE,
                                0 IS_BAD,
                                null AB_ID_BAD,
                                tAB.cc_id,
                                CC.CC_NAME,
                                tAB.CHANGE_STATUS_DATE - 2 / 24 CHANGE_STATUS_DATE,
                                l_gen_doc_number DOG_NUMBER,
                                tAB.ARCHIVE_MARK,
                                p.person_lastname || ' ' ||
                                p.person_firstname || ' ' ||
                                p.person_middlename worker,
                                pp.person_lastname || ' ' ||
                                pp.person_firstname || ' ' ||
                                pp.person_middlename seller,
                                pu.employee_number seller_emp_num,
                                tAB.is_common,
                                clients_j.jur_category client_category,
                                clients_j.jur_billing_group billing_group,
                                bill.jbg_name billing_group_name,
                                act.data_activated - 2 / 24 as data_activated,
                                nvl(mu.usb_model, mp.name_model) model_equip,
                                decode(sa.sa_id,
                                       null,
                                       null, -- 58777
                                       tsu.person_lastname || ' ' ||
                                       tsu.person_firstname || ' ' ||
                                       tsu.person_middlename || ' (' ||
                                       su.su_emp_num || ')' || '/' ||
                                       pers.person_lastname || ' ' ||
                                       pers.person_firstname || ' ' ||
                                       pers.person_middlename || ' (' ||
                                       sa.sa_emp_num || ')') seller_active_fio,
                                clients_c.is_resident,
                                clients_c.reg_deadline,
                                sim.sim_type,
                                dic_st.name sim_type_name,
                                orgR.Org_Id root_org_id,
                                orgR.Org_Name root_org_name,
                                reg1.reg_id,
                                reg1.kl_name reg_name,
                                reg1.mrf_id,
                                mrf1.name_mrf,
                                tab.org_id,
                                sim.tar_id,
                                dst.name status_name,
                                tab.TRANSFER_PHONE,
                                chan.group_name ||
                                decode(chan.group_name, null, null, '/') ||
                                chan.name || decode(chan.date_end,
                                                    null,
                                                    null,
                                                    ' (закрыт ') ||
                                chan.date_end ||
                                decode(chan.date_end, null, null, ')') channel_name,
                                tab.out_account,
                                tab.CREDIT_LIMIT,
                                tab.ALARM_LIMIT,
                                tab.ab_comment,
                                tab.REQ_DELIVERY_ID,
                                abr.request_id,
                                reg_req.kl_region kl_reg_request,
                                tab.client_account,
                                tab.agr_num
                           from (select abo.*
                                 from TABLE(l_tab_ab_id) l_t
                                 join T_ABONENT abo
                                on abo.ab_id =l_t.column_value) tAB
                           join t_tmc_sim sim
                             on tAB.ab_tmc_id = sim.tmc_id
                           left join t_dic_sim_type dic_st
                             on dic_st.id = sim.sim_type
                           join t_tarif_by_at_id tar
                             on tar.at_id = sim.tar_id
                           join t_clients clients_c
                             on tAB.client_id = clients_c.client_id
                           left join t_person clients_p
                             on clients_c.client_type = 'P'
                            and clients_c.fullinfo_id = clients_p.person_id
                           left join t_juristic clients_j
                             on clients_c.client_type = 'J'
                            and clients_c.fullinfo_id = clients_j.juristic_id
                           /*left join mv_org_tree rel
                             on rel.org_id = tab.org_id
                            and rel.org_pid <> -1
                            and rel.root_org_pid <> -1
                            and rel.org_reltype not in (1005, 1006, 1009)*/
                           /*left*/ join T_ORGANIZATIONS OOO
                             on tAB.org_id = ooo.org_id
                           left join t_dic_region reg1
                             on reg1.reg_id = ooo.region_id
                           left join t_organizations orgR
                             on orgR.Org_Id = ooo.ROOT_ORG_ID2
                           left join t_dic_mrf mrf1
                             on mrf1.id = reg1.mrf_id
                             or mrf1.org_id = ooo.org_id
                           left join T_CALC_CENTER CC
                             on tAB.cc_id = cc.cc_id
                           left join t_users u
                             on tAB.worker_id = u.usr_id
                           left join t_person p
                             on u.usr_person_id = p.person_id
                           left join t_users pu
                             on tAB.user_id = pu.usr_id
                           left join t_person pp
                             on pu.usr_person_id = pp.person_id
                           left join t_dic_mvno_region dmr
                             on dmr.id = tar.at_region_id
                            and dmr.reg_id = ooo.region_id
                           left join t_dic_region REG
                             on dmr.reg_id = reg.reg_id
                           left join t_abonent_activated act
                             on act.abonent_id = tAB.ab_id
                           left join jur_billing_group bill
                             on bill.jbg_id = clients_j.jur_billing_group
                           left join t_tmc_modem_usb usb
                             on usb.tmc_id = tab.equipment_tmc_id
                           left join t_modem_model_usb mu
                             on mu.id = usb.usb_model
                           left join t_tmc_phone allo
                             on allo.tmc_id = tab.equipment_tmc_id
                           left join t_model_phone mp
                             on mp.model_id = allo.model_id
                           left join t_seller_active sa
                             on sa.sa_id = tAB.seller_active_id
                           left join t_person pers
                             on pers.person_id = sa.sa_person_id
                         -- 58777
                           left join t_seller_active_rel sar
                             on sar.sa_id = sa.sa_id
                            and tab.ab_reg_date between sar.date_from and
                                nvl(sar.date_to, sysdate)
                           left join t_supervisor su
                             on su.su_id = sar.su_id
                           left join t_person tsu
                             on tsu.person_id = su.su_person_id
                           left join t_dic_ab_status dst
                             on dst.id = tab.AB_STATUS
                           left join t_dic_channels chan
                             on chan.channel_id = tab.channel_id
                           left join t_abonent_to_request abr
                             on abr.ab_id=tab.ab_id
                            and abr.request_id is not null 
                            and abr.is_send in (0,1)  
                           left join tr_request req
                             on req.id=abr.request_id
                           left join t_dic_region reg_req
                             on reg_req.reg_id=req.region_id
                         /* where ab.ab_id in (select \*+ PRECOMPUTE_SUBQUERY *\
                         *
                          from TABLE(l_tab_ab_id))*/
                         ) tt
                 union all
                 -- ошибочные подключения GSM и CDMA
                 select distinct null ABONENT_ID,
                                 AB.ID_CLIENT CLIENT_ID,
                                 clients_c.client_type CLIENT_TYPE,
                                 clients_p.person_lastname PERSON_LASTNAME,
                                 clients_p.person_firstname PERSON_FIRSTNAME,
                                 clients_p.person_middlename PERSON_MIDDLENAME,
                                 clients_j.jur_name JUR_NAME,
                                 AB.REG_DATE - 2 / 24 REG_DATE,
                                 AB.MOD_DATE - 2 / 24 MOD_DATE,
                                 101 STATUS,
                                 NULL AS AB_STATUS_EX,
                                 AB.PAID PAID,
                                 AB.COST COST,
                                 null ERR_CODE,
                                 AB.ERR_MSG ERR_MSG,
                                 OOO.ORG_NAME WHICH_ORG_AB_NAME,
                                 NVL2(ab.id_tar,
                                      (select CONCAT(DV1.DV_NAME,
                                                     CONCAT(' ',
                                                            CONCAT(DV3.DV_NAME,
                                                                   CONCAT('  ',
                                                                          DV2.DV_NAME))))
                                         from T_DIC_VALUES DV1,
                                              T_DIC_VALUES DV2,
                                              T_DIC_VALUES DV3
                                        where tar.type_vdvd_id = dv1.dv_id
                                          and tar.tariff_type = dv2.dv_id
                                          and tar.pay_type = dv3.dv_id),
                                      'не известен') CONN_NAME,
                                 tar.title || ' / ' || REG.KL_NAME TAR_NAME,
                                 AB.IMSI IMSI,
                                 AB.CALLSIGN CS,
                                 AB.DOG_DATE - 2 / 24 DOG_DATE,
                                 1 IS_BAD,
                                 AB.ID AB_ID_BAD,
                                 ab.id_cc cc_id,
                                 NVL(CC.CC_NAME, '') CC_NAME,
                                 null CHANGE_STATUS_DATE,
                                 l_gen_doc_number DOG_NUMBER,
                                 AB.ARCHIVE_MARK,
                                 p.person_lastname || ' ' ||
                                 p.person_firstname || ' ' ||
                                 p.person_middlename worker,
                                 pp.person_lastname || ' ' ||
                                 pp.person_firstname || ' ' ||
                                 pp.person_middlename seller,
                                 pu.employee_number seller_emp_num,
                                 ab.is_common,
                                 clients_j.jur_category client_category,
                                 clients_j.jur_billing_group billing_group,
                                 bill.jbg_name billing_group_name,
                                 null as data_activated,
                                 nvl(mu.usb_model, mp.name_model) model_equip,
                                 decode(sa.sa_id,
                                        null,
                                        null, -- 58777
                                        tsu.person_lastname || ' ' ||
                                        tsu.person_firstname || ' ' ||
                                        tsu.person_middlename || ' (' ||
                                        su.su_emp_num || ')' || '/' ||
                                        pers.person_lastname || ' ' ||
                                        pers.person_firstname || ' ' ||
                                        pers.person_middlename || ' (' ||
                                        sa.sa_emp_num || ')') seller_active_fio,
                                 clients_c.is_resident,
                                 clients_c.reg_deadline,
                                 ts.sim_type sim_type,
                                 dic_st.name sim_type_name,
                                 orgR.org_id root_org_id,
                                 orgr.org_name root_org_name,
                                 reg1.reg_id,
                                 reg1.kl_name reg_name,
                                 reg1.mrf_id,
                                 mrf1.name_mrf,
                                 ab.id_org org_id,
                                 ab.id_tar tar_id,
                                 dst.name status_name,
                                 ab.Transfer_Phone,
                                 chan.group_name ||
                                 decode(chan.group_name, null, null, '/') ||
                                 chan.name ||
                                 decode(chan.date_end,
                                        null,
                                        null,
                                        ' (закрыт ') || chan.date_end ||
                                 decode(chan.date_end, null, null, ')') channel_name,
                                 ab.out_account,
                                 ab.CREDIT_LIMIT,
                                 ab.ALARM_LIMIT,
                                 ab.ab_comment,
                                 ab.REQ_DELIVERY_ID,
                                 ab.request_id,
                                 reg_req.kl_region kl_reg_request,
                                 null client_account,
                                 null agr_num
                   from (select ab.* from TABLE(l_tab_ab_id_bad) l_t
                   join T_ABON_BAD AB
                     on l_t.column_value = ab.id
                     and nvl(ab.type_req, 0) in (0, 1)) ab
                   /*(select column_value from TABLE(l_tab_ab_id_bad)) tab
                   join T_ABON_BAD AB
                     on tab.column_value = ab.id
                    and nvl(ab.type_req, 0) in (0, 1)*/
                   left join t_tmc_sim ts
                     on ts.sim_imsi = ab.imsi
                     or ts.sim_iccid = ab.imsi
                   left join t_dic_sim_type dic_st
                     on dic_st.id = ts.sim_type
                   join t_tarif_by_at_id tar
                     on tar.at_id = ab.id_tar
                   join t_clients clients_c
                     on ab.id_client = clients_c.client_id
                   left join t_person clients_p
                     on clients_c.client_type = 'P'
                    and clients_c.fullinfo_id = clients_p.person_id
                   left join t_juristic clients_j
                     on clients_c.client_type = 'J'
                    and clients_c.fullinfo_id = clients_j.juristic_id

                   /*left*/ /*join mv_org_tree rel
                     on rel.org_id = ab.id_org
                    and rel.org_pid <> -1
                    and rel.root_org_pid <> -1
                    and rel.org_reltype not in (1005, 1006, 1009)*/
                   /*left*/ join T_ORGANIZATIONS OOO
                     on AB.id_org = ooo.org_id
                   left join t_dic_region reg1
                     on reg1.reg_id = ooo.region_id

                   left join t_organizations orgR
                     on orgR.org_id = ooo.ROOT_ORG_ID2

                   left join t_dic_mrf mrf1
                     on mrf1.id = reg1.mrf_id
                     or mrf1.org_id = ooo.org_id
                   left join T_CALC_CENTER CC
                     on ab.id_cc = cc.cc_id
                   left join t_users u
                     on ab.worker_id = u.usr_id
                   left join t_person p
                     on u.usr_person_id = p.person_id
                   left join t_users pu
                     on ab.user_id = pu.usr_id
                   left join t_person pp
                     on pu.usr_person_id = pp.person_id
                   left join t_dic_mvno_region dmr
                     on dmr.id = tar.at_region_id
                    and dmr.reg_id = ooo.region_id
                   left join t_dic_region REG
                     on dmr.reg_id = reg.reg_id
                   left join t_dic_mvno_region dmr
                     on dmr.reg_id = reg.reg_id
                   left join jur_billing_group bill
                     on bill.jbg_id = clients_j.jur_billing_group
                   left join t_modem_model_usb mu
                     on mu.id = ab.equipment_model_id
                    and ab.equipment_required = 4
                   left join t_model_phone mp
                     on mp.model_id = ab.phone_model_id
                    and ab.equipment_required = 7003
                   left join t_seller_active sa
                     on sa.sa_id = ab.seller_active_id
                   left join t_person pers
                     on pers.person_id = sa.sa_person_id
                 -- 58777
                   left join t_seller_active_rel sar
                     on sar.sa_id = sa.sa_id
                    and ab.dog_date between sar.date_from and
                        nvl(sar.date_to, sysdate)
                   left join t_supervisor su
                     on su.su_id = sar.su_id
                   left join t_person tsu
                     on tsu.person_id = su.su_person_id
                   left join t_dic_ab_status dst
                     on dst.id = 101
                   left join t_dic_channels chan
                     on chan.channel_id = ab.channel_id
                   left join tr_request req
                     on req.id=ab.request_id
                   left join t_dic_region reg_req
                     on reg_req.reg_id=req.region_id    ) aa
                  order by decode(nvl(pi_date_type, 1),
                                  0,
                                  change_status_date,
                                  1,
                                  dog_date,
                                  2,
                                  data_activated,
                                  null);


    else
      po_err_num := -1;
      po_err_msg := 'Неверный тип ТМЦ. ' ||
                    dbms_utility.format_error_backtrace;
      return null;
    end if;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Abonent_List_GSM_CDMA;

  -- Статистика по подключениям GSM/CDMA
  function Get_Abonents_Stat_GSM_CDMA(pi_org_tab      in array_num_2,
                                      pi_block        in number,
                                      pi_org_relation in num_tab,
                                      pi_client_type  in char,
                                      pi_vdvd_type    in T_TARIFF2.TYPE_VDVD_ID%type,
                                      pi_pay_type     in T_TARIFF2.PAY_TYPE%type,
                                      pi_tariff_type  in T_TARIFF2.TARIFF_TYPE%type,
                                      pi_date_0       in T_ABONENT.AB_REG_DATE%type,
                                      pi_date_1       in T_ABONENT.AB_REG_DATE%type,
                                      pi_worker_id    in T_USERS.USR_ID%type,
                                      po_err_num      out pls_integer,
                                      po_err_msg      out varchar2,
                                      --pi_cc_id        in T_CALC_CENTER.CC_ID%type := null,
                                      pi_date_type   in number := 1,
                                      pi_channel_tab in num_tab := num_tab())
    return sys_refcursor is

    res      sys_refcursor;
    cl_type  char := null;
    l_org_id pls_integer := null;
    org_tab  num_tab := num_tab();

    qwery_select    varchar2(3000);
    l_tab_ab_id     num_tab := null;
    l_tab_ab_id_bad num_tab := null;

    c_date_0 constant date := pi_date_0;
    c_date_1 constant date := pi_date_1;
    --c_cc_id    constant number := pi_cc_id;
    c_pay_type constant number := pi_pay_type;

  begin
    logging_pkg.debug('pi_client_type=' || pi_client_type || 'pi_org_tab=' ||
                      get_str_by_array_num_2(pi_org_tab) || ', ' ||
                      'pi_block=' || pi_block || ', ' ||
                      'pi_org_relation=' ||
                      get_str_by_num_tab(pi_org_relation) || ', ' ||
                      'pi_channel_tab=' ||
                      get_str_by_num_tab(pi_channel_tab) || ', ' ||
                      ', pi_vdvd_type=' || pi_vdvd_type ||
                      ', pi_pay_type=' || pi_pay_type ||
                      ', pi_tariff_type=' || pi_tariff_type ||
                      ', pi_date_0=' || pi_date_0 || ', pi_date_1=' ||
                      pi_date_1 || ', pi_worker_id=' || pi_worker_id ||
                      /*', pi_cc_id=' || pi_cc_id || */
                      ', pi_date_type=' || pi_date_type,
                      'Get_Abonents_Stat_GSM_CDMA');
    -- checking access for operation for specified user
    if (not Security_pkg.Check_User_Right_str('EISSD.CONNECTIONS.GSM.LIST',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then

      return null;
    end if;

    /*if (pi_org_tab is not null and pi_org_tab.count > 0) then
      if (not Security_pkg.Check_Rights_Orgs_str('EISSD.CONNECTIONS.GSM.LIST',
                                                 get_num_tab_by_array_num_2(pi_org_tab),
                                                 pi_worker_id,
                                                 po_err_num,
                                                 po_err_msg)) then
        return null;
      end if;
    end if;*/

    if ((pi_client_type is not null) and
       (pi_client_type <> c_client_person) and
       (pi_client_type <> c_client_juristic)) then

      cl_type := null;
    else
      cl_type := pi_client_type;
    end if;

    org_tab := get_orgs_tab_for_multiset(pi_orgs         => pi_org_tab,
                                         Pi_worker_id    => pi_worker_id,
                                         pi_block        => pi_block,
                                         pi_org_relation => pi_org_relation,
                                         pi_is_rtmob     => 1);

    -- берем список абонентов для организаций пользователя
    if (pi_vdvd_type = 51 or pi_vdvd_type = 53) then
      qwery_select := 'select ab.ab_id from T_ABONENT AB
                                     join T_TMC_SIM SIM
                                          on (AB.AB_TMC_ID = SIM.Tmc_Id)
                                     join T_TARIF_BY_AT_ID TAR
                                          on (SIM.Tar_Id = TAR.AT_ID)
                                     join T_CLIENTS CL
                                          on (cl.client_id = ab.client_id)
                                     left join T_CALC_CENTER CC
                                          on (ab.cc_id = cc.cc_id)
                                     left join t_dic_region REG
                                          on (cc.cc_region_id = reg.reg_id)
                                     left join mv_org_tree m
                                       on m.org_id = ab.org_id
                                     left join t_dogovor d
                                       on d.org_rel_id = m.root_rel_id';

      if (pi_date_type = 0 or pi_date_type = 1) then
        qwery_select := qwery_select ||
                        ' where ab.org_id in
                                                 (select /*+ PRECOMPUTE_SUBQUERY */
                                                         *   from TABLE(:l_org_tab))
                                   and (ab.dog_id is null or ab.dog_id = d.dog_id)
                                   and ab.is_deleted = 0 and ab.ab_status not in (113)
                                   and ab.channel_id in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_channel_tab)) ';
      elsif (pi_date_type = 2) then
        qwery_select := qwery_select ||
                        ' join T_ABONENT_ACTIVATED AB_AC
                                            on (ab.ab_id = ab_ac.abonent_id)
                                            and ab.org_id in
                                                 (select /*+ PRECOMPUTE_SUBQUERY */
                                                         *   from TABLE(:l_org_tab))
                                   and ab.is_deleted = 0
                                   and ab.channel_id in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_channel_tab)) ';
      end if;

      if (pi_date_type = 0) then
        qwery_select := qwery_select ||
                        ' and ((ab.change_status_date is not null) and
                                            (ab.change_status_date ) >= trunc(to_date(''' ||
                        c_date_0 ||
                        ''' , ''DD.MM.YY''))+ 2 / 24
                                        and (ab.change_status_date ) < trunc(to_date(''' ||
                        c_date_1 || ''' , ''DD.MM.YY''))+ 2 / 24 + 1)';
      end if;
      if (pi_date_type = 1) then
        qwery_select := qwery_select ||
                        ' and  ((ab.ab_reg_date is not null) and
                                              (ab.ab_reg_date ) >= trunc(to_date(''' ||
                        c_date_0 ||
                        ''' , ''DD.MM.YY''))+ 2 / 24 and
                                              (ab.ab_reg_date) < trunc(to_date(''' ||
                        c_date_1 || ''' , ''DD.MM.YY''))+ 2 / 24 + 1)';
      end if;
      if (pi_date_type = 2) then
        qwery_select := qwery_select ||
                        ' and  ((ab_ac.data_activated is not null) and
                                                ab_ac.data_activated  >= trunc(to_date(''' ||
                        c_date_0 ||
                        ''' , ''DD.MM.YY''))+ 2 / 24 and
                                                ab_ac.data_activated  < trunc (to_date(''' ||
                        c_date_1 || ''' , ''DD.MM.YY''))+ 2 / 24 + 1)';
      end if;
      if (pi_pay_type is not null) then
        qwery_select := qwery_select || ' and TAR.PAY_TYPE = ' ||
                        c_pay_type;
      end if;
      if (cl_type is not null) then
        qwery_select := qwery_select || ' and cl.client_type = ''' ||
                        cl_type || '''';
      end if;
      /*if (c_cc_id is not null) then
        qwery_select := qwery_select || ' and ((' || c_cc_id ||
                        ' >= 0 and ' || c_cc_id ||
                        ' = ab.cc_id) or
                                            (' ||
                        c_cc_id || '< 0 and reg.reg_id is not null and ' ||
                        c_cc_id || ' = -100 * reg.reg_id))';
      end if;*/

      execute immediate qwery_select bulk collect
        into l_tab_ab_id
        using org_tab, pi_channel_tab;

      qwery_select := null;
      --ошибочные подключения
      if (pi_date_type = 1) then
        qwery_select := 'select ab.id from T_ABON_BAD   AB,
                                 T_CLIENTS           CL,
                                 T_CALC_CENTER       CC,
                                 t_dic_region           REG,
                                 T_TARIF_BY_AT_ID    TAR,
                                 mv_org_tree m ,
                                 t_dogovor d
                                 where ab.id_tar = tar.at_id
                                   and ab.id_cc = cc.cc_id(+)
                                   and cc.cc_region_id = reg.reg_id(+)
                                   and (cl.client_id = ab.id_client)
                                   and ab.id_org in
                                                 (select /*+ PRECOMPUTE_SUBQUERY */
                                                         *   from TABLE(:l_org_tab))
                                   and m.org_id = ab.id_org(+)
                                   and m.root_rel_id = d.org_rel_id(+)
                                   and (ab.dog_id is null or ab.dog_id = d.dog_id)
                                   and ab.is_deleted = 0  and  ((ab.dog_date is not null) and
                                              (ab.reg_date ) >= trunc(to_date(''' ||
                        c_date_0 ||
                        ''' , ''DD.MM.YY''))+ 2 / 24 and
                                              (ab.reg_date) < trunc(to_date(''' ||
                        c_date_1 ||
                        ''' , ''DD.MM.YY'')) + 2 / 24+ 1)
                                   and ab.channel_id in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_channel_tab))';

        if (pi_pay_type is not null) then
          qwery_select := qwery_select || ' and TAR.PAY_TYPE = ' ||
                          c_pay_type;
        end if;
        if (cl_type is not null) then
          qwery_select := qwery_select || ' and cl.client_type = ''' ||
                          cl_type || '''';
        end if;
        /*if (c_cc_id is not null) then
          qwery_select := qwery_select || ' and ((' || c_cc_id ||
                          ' >= 0 and ' || c_cc_id ||
                          ' = ab.id_cc) or
                                            (' ||
                          c_cc_id || '< 0 and reg.reg_id is not null and ' ||
                          c_cc_id || ' = -100 * reg.reg_id))';
        end if;*/
        execute immediate qwery_select bulk collect
          into l_tab_ab_id_bad
          using org_tab, pi_channel_tab;
      end if;

      open res for
        SELECT DV.ID AS "STATUS",
               NVL(ABON.PAID, 0) AS PAID,
               NVL(ABON.COUNT, 0) AS "count"
          FROM T_DIC_AB_STATUS DV
          LEFT JOIN (SELECT A.AB_STATUS "STATUS",
                            SUM(A.AB_PAID) "PAID",
                            COUNT(*) "COUNT"
                       FROM (SELECT AB.AB_ID,
                                    AB.AB_STATUS,
                                    NVL(AB.AB_PAID, 0) AS AB_PAID,
                                    AB.CC_ID,
                                    AB.CLIENT_ID,
                                    AB.AB_DOG_DATE,
                                    AB.CHANGE_STATUS_DATE,
                                    AB.ORG_ID
                               FROM T_ABONENT AB
                              WHERE AB.AB_ID IN
                                    (SELECT * FROM TABLE(l_tab_ab_id))
                                and nvl(ab.type_request, 0) in (0, 1)
                             UNION ALL
                             SELECT AB.ID,
                                    101          AB_STATUS,
                                    AB.PAID      AB_PAID,
                                    AB.ID_CC     CC_ID,
                                    AB.ID_CLIENT CLIENT_ID,
                                    AB.DOG_DATE  AB_DOG_DATE,
                                    NULL         CHANGE_STATUS_DATE,
                                    AB.ID_ORG    ORG_ID
                               FROM T_ABON_BAD AB
                              WHERE AB.ID IN
                                    (SELECT * FROM TABLE(l_tab_ab_id_bad))
                                and nvl(ab.type_req, 0) in (0, 1)) A
                      GROUP BY A.AB_STATUS
                      ORDER BY STATUS ASC) ABON
            ON ABON.STATUS = DV.ID
         WHERE DV.ID not in (99, 113, 114, 115)
         ORDER BY DV.ID;
    else
      po_err_num := -1;
      po_err_msg := 'Неверный тип ТМЦ. ' ||
                    dbms_utility.format_error_backtrace;
      return null;

    end if;
    return res;

  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Abonents_Stat_GSM_CDMA;

  -- Статистика по подключениям GSM/CDMA (единый договор)
  function Get_Abon_Stat_GSM_CDMA_common(pi_org_tab      in array_num_2,
                                         pi_block        in number,
                                         pi_org_relation in num_tab,
                                         pi_client_type  in char,
                                         pi_vdvd_type    in T_TARIFF2.TYPE_VDVD_ID%type,
                                         pi_pay_type     in T_TARIFF2.PAY_TYPE%type,
                                         pi_tariff_type  in T_TARIFF2.TARIFF_TYPE%type,
                                         pi_date_0       in T_ABONENT.AB_REG_DATE%type,
                                         pi_date_1       in T_ABONENT.AB_REG_DATE%type,
                                         pi_worker_id    in T_USERS.USR_ID%type,
                                         po_err_num      out pls_integer,
                                         po_err_msg      out t_Err_Msg,
                                         --pi_cc_id        in T_CALC_CENTER.CC_ID%type := null,
                                         pi_date_type   in number := 1,
                                         pi_channel_tab in num_tab := num_tab())
    return sys_refcursor is

    res      sys_refcursor;
    cl_type  char := null;
    l_org_id pls_integer := null;
    org_tab  num_tab := num_tab();

    qwery_select varchar2(2000);
    l_tab_ab_id  num_tab := null;

    c_date_0 constant date := pi_date_0;
    c_date_1 constant date := pi_date_1;
    --c_cc_id    constant number := pi_cc_id;
    c_pay_type constant number := pi_pay_type;

  begin
    logging_pkg.debug('pi_org_id=' || ', pi_client_type=' ||
                      pi_client_type || ', pi_vdvd_type=' || pi_vdvd_type ||
                      ', pi_pay_type=' || pi_pay_type ||
                      ', pi_tariff_type=' || pi_tariff_type ||
                      ', pi_date_0=' || pi_date_0 || ', pi_date_1=' ||
                      pi_date_1 || ', pi_worker_id=' || pi_worker_id ||
                      /*', pi_cc_id=' || pi_cc_id || */
                      ', pi_date_type=' || pi_date_type,
                      'Get_Abon_Stat_GSM_CDMA_common');
    -- checking access for operation for specified user
    if (not Security_pkg.Check_User_Right_str('EISSD.CONNECTIONS.GSM.LIST',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then

      return null;
    end if;

    /*if (pi_org_tab is not null and pi_org_tab.count > 0) then
      if (not Security_pkg.Check_Rights_Orgs_str('EISSD.CONNECTIONS.GSM.LIST',
                                                 get_num_tab_by_array_num_2(pi_org_tab),
                                                 pi_worker_id,
                                                 po_err_num,
                                                 po_err_msg)) then
        return null;
      end if;
    end if;*/

    if ((pi_client_type is not null) and
       (pi_client_type <> c_client_person) and
       (pi_client_type <> c_client_juristic)) then

      cl_type := null;
    else
      cl_type := pi_client_type;
    end if;

    org_tab := get_orgs_tab_for_multiset(pi_orgs         => pi_org_tab,
                                         Pi_worker_id    => pi_worker_id,
                                         pi_block        => pi_block,
                                         pi_org_relation => pi_org_relation,
                                         pi_is_rtmob     => 1);

    -- берем список абонентов для организаций пользователя
    if (pi_vdvd_type = 51 or pi_vdvd_type = 53) then
      qwery_select := 'select ab.ab_id from T_ABONENT AB
                                     join T_TMC_SIM SIM
                                          on (AB.AB_TMC_ID = SIM.Tmc_Id)
                                     join T_TARIF_BY_AT_ID TAR
                                          on (SIM.Tar_Id = TAR.AT_ID)
                                     join T_CLIENTS CL
                                          on (cl.client_id = ab.client_id)
                                     left join T_CALC_CENTER CC
                                          on (ab.cc_id = cc.cc_id)
                                     left join t_dic_region REG
                                          on (cc.cc_region_id = reg.reg_id)
                                     left join mv_org_tree m
                                       on m.org_id = ab.org_id
                                     left join t_dogovor d
                                       on d.org_rel_id = m.root_rel_id';

      if (pi_date_type = 0 or pi_date_type = 1) then
        qwery_select := qwery_select ||
                        ' where ab.org_id in
                                                 (select /*+ PRECOMPUTE_SUBQUERY */
                                                         *   from TABLE(:l_org_tab))
                                   and (ab.dog_id is null or ab.dog_id = d.dog_id)
                                   and ab.is_deleted = 0 and ab.ab_status in (104, 105)
                                   and ab.channel_id in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_channel_tab))';
      elsif (pi_date_type = 2) then
        qwery_select := qwery_select ||
                        ' join T_ABONENT_ACTIVATED AB_AC
                                            on (ab.ab_id = ab_ac.abonent_id)
                                            and ab.org_id in
                                                 (select /*+ PRECOMPUTE_SUBQUERY */
                                                         *   from TABLE(:l_org_tab))
                                   and ab.is_deleted = 0 and ab.ab_status in (104, 105)
                                   and ab.channel_id in (select /*+ PRECOMPUTE_SUBQUERY */ * from TABLE(:l_channel_tab))';
      end if;

      if (pi_date_type = 0) then
        qwery_select := qwery_select ||
                        ' and ((ab.change_status_date is not null) and
                                            (ab.change_status_date) >= trunc(to_date(''' ||
                        c_date_0 ||
                        ''' , ''DD.MM.YY''))+ 2 / 24
                                        and (ab.change_status_date ) < trunc(to_date(''' ||
                        c_date_1 || ''' , ''DD.MM.YY'')) + 2 / 24 + 1)'; /*+ 1*/
      end if;
      if (pi_date_type = 1) then
        qwery_select := qwery_select ||
                        ' and  ((ab.ab_reg_date is not null) and
                                              (ab.ab_reg_date ) >= trunc(to_date(''' ||
                        c_date_0 ||
                        ''' , ''DD.MM.YY''))+ 2 / 24 and
                                              (ab.ab_reg_date) < trunc(to_date(''' ||
                        c_date_1 || ''' , ''DD.MM.YY''))+ 2 / 24 + 1)'; /*+ 1*/
      end if;
      if (pi_date_type = 2) then
        qwery_select := qwery_select ||
                        ' and  ((ab_ac.data_activated is not null) and
                                                ab_ac.data_activated >= trunc(to_date(''' ||
                        c_date_0 ||
                        ''' , ''DD.MM.YY''))+ 2 / 24 and
                                                ab_ac.data_activated  < trunc (to_date(''' ||
                        c_date_1 || ''' , ''DD.MM.YY'')) + 2 / 24 + 1)'; /*+ 1*/
      end if;
      if (cl_type is not null) then
        qwery_select := qwery_select || ' and cl.client_type = ''' ||
                        cl_type || '''';
      end if;
      if (pi_pay_type is not null) then
        qwery_select := qwery_select || ' and TAR.PAY_TYPE = ' ||
                        c_pay_type;
      end if;
      /*if (c_cc_id is not null) then
        qwery_select := qwery_select || ' and ((' || c_cc_id ||
                        ' >= 0 and ' || c_cc_id ||
                        ' = ab.cc_id) or
                                            (' ||
                        c_cc_id || '< 0 and reg.reg_id is not null and ' ||
                        c_cc_id || ' = -100 * reg.reg_id))';
      end if;*/

      execute immediate qwery_select bulk collect
        into l_tab_ab_id
        using org_tab, pi_channel_tab;

      open res for
        select a.is_common, sum(A.AB_PAID) "PAID", count(*) "COUNT"
          from ( -- валидные подключения GSM, CDMA
                select AB.AB_ID,
                        AB.AB_STATUS,
                        AB.AB_PAID,
                        AB.CC_ID,
                        AB.CLIENT_ID,
                        AB.AB_DOG_DATE,
                        AB.CHANGE_STATUS_DATE,
                        AB.ORG_ID,
                        -- 28.10.2010 25247
                        decode(ab.is_common, null, 0, ab.is_common) is_common
                  from T_ABONENT AB
                 where ab.ab_id in (SELECT /*+ PRECOMPUTE_SUBQUERY */
                                     *
                                      FROM TABLE(l_tab_ab_id))) A
         group by a.is_common
         order by a.is_common asc;
      return res;
    else
      po_err_num := -1;
      po_err_msg := 'Неверный тип ТМЦ. ' ||
                    dbms_utility.format_error_backtrace;
      return null;
    end if;
    return res;

  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Abon_Stat_GSM_CDMA_common;

  ------------------------------------------------------------------------------
  -- Получение маркетингового предложения для абонента по списку номеров устройств
  ------------------------------------------------------------------------------
  function get_mark_prop_by_number_list(pi_phone_num in STRING_TAB,
                                        pi_worker_id in number,
                                        po_err_num   out pls_integer,
                                        po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin

    open res for
      select nn.phone, t.packet_text
        from (select column_value as phone from table(pi_phone_num)) nn
        left join t_marketing_proposal t
          on t.msisdn = nn.phone;

    return res;

  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
  end get_mark_prop_by_number_list;
  ------------------------------------------------------------------------------
  --изменяет статус абонентов пришедших извне(сторонних операторов)
  ------------------------------------------------------------------------------
  procedure Change_Status_Outside_Abonent(pi_ab_id   in t_abonent.ab_id%type,
                                          pi_status  in number,
                                          po_err_num out pls_integer,
                                          po_err_msg out varchar2) is
  begin
    update t_abonent_outside t
       set t.status = pi_status
     where t.ab_id = pi_ab_id;
    insert into t_abonent_out_status
      (ab_id, status)
    values
      (pi_ab_id, pi_status);
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
  end;
  ------------------------------------------------------------------------------------
  -- Корректировка данных подключения GSM/UMTS
  ------------------------------------------------------------------------------------
  procedure correctAbonent(pi_abonent_id in number,
                           pi_client_id  in number,
                           pi_set_id     in number,
                           pi_ab_comment in t_abonent.ab_comment%type,
                           pi_worker_id  in number,
                           po_err_num    out pls_integer,
                           po_err_msg    out t_Err_Msg) is
    l_ab_status number;
  begin
    -- Проверка прав
    if (not Security_pkg.Check_User_Right_str('EISSD.CONNECTIONS.GSM.DATA.EDIT',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return;
    end if;

    -- Достаем данные подлючения (статус)
    begin
      select a.ab_status
        into l_ab_status
        from t_abonent a
       where a.ab_id = pi_abonent_id;
    exception
      when no_data_found then
        po_err_num := 1;
        po_err_msg := 'Подключение не найдено.';
    end;

    if l_ab_status in (104, 105) then
      update t_abonent a
         set a.client_id  = pi_client_id,
             a.set_id     = nvl(pi_set_id, a.set_id),
             a.ab_comment = pi_ab_comment
       where a.ab_id = pi_abonent_id;
    else
      po_err_num := 2;
      po_err_msg := 'Запрещенно редактировать подключение, которое не было одобрено.';
    end if;

  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
  end correctAbonent;

  ---------------------------------------------------------------------------
  -- 74975 Ветка (РТК - 0/РТМ - 1) по ИД абонента
  ---------------------------------------------------------------------------
  function getBranchByAb(pi_ab_id     in T_ABONENT.AB_ID%type,
                         pi_worker_id in T_USERS.USR_ID%type,
                         po_err_num   out pls_integer,
                         po_err_msg   out t_Err_Msg,
                         pi_is_bad    in number := 0) return number is
    res          number;
    l_org_id     T_ORGANIZATIONS.ORG_ID%type;
    L_OP_ID      T_TMC_OPERATIONS.OP_ID%TYPE;
    l_org_rel_id number;
    l_is_org_rtm number;
  begin

    select MAX(ABON.ORG_ID), MAX(ID_OP), MAX(OIR.IS_ORG_RTM)
      into L_ORG_ID, L_OP_ID, L_IS_ORG_RTM
      from (select AB.ORG_ID, AB.ID_OP
              from T_ABONENT AB
             Where ab.ab_id = pi_ab_id
               and pi_is_bad = 0
            union
            select AB_B.ID_ORG ORG_ID, AB_B.ID_OP
              from T_ABON_BAD AB_B
             where ab_b.id = pi_ab_id
               and pi_is_bad = 1) abon,
           t_org_is_rtmob oir
     where abon.org_id = oir.org_id;

    if L_IS_ORG_RTM is not null then
      res := L_IS_ORG_RTM;
    else
      -- Ищем договор, по которому симка пришла на склад
      select td.org_rel_id
        into l_org_rel_id
        from t_tmc_operations o, t_dogovor td
       where o.op_id = L_OP_ID
         and o.op_dog_id = td.dog_id(+);
      if l_org_rel_id is not null then
        select oir.is_org_rtm
          into res
          from mv_org_tree t, t_org_is_rtmob oir
         where t.root_org_pid = oir.org_id
           and t.id = l_org_rel_id;
      end if;
    end if;

    return res;

  exception
    when NO_DATA_FOUND then
      po_err_num := 1;
      po_err_msg := 'Абонент не найден (' || pi_ab_id || ')' ||
                    dbms_utility.format_error_backtrace;
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error('pi_ab_id=' || pi_ab_id || ' ' || po_err_msg,
                        c_package || 'get_abonent_by_id');
      return null;
  end getBranchByAb;
  --------------------------------------------------------------------------
  -- Является ли регион организации мигрируемым?
  --------------------------------------------------------------------------
  function isOrgTele2(pi_org_id  in number,
                      po_err_num out pls_integer,
                      po_err_msg out t_Err_Msg) return number is
    res number;
  begin
    select count(*)
      into res
      from t_organizations o
      join t_dic_region_info dri
        on (o.region_id = dri.reg_id or
           (pi_org_id = 2004855 and dri.reg_id = 43))
     where o.org_id = pi_org_id
       and dri.is_reg_tele2 = 1
       and rownum = 1;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return 1;
  end isOrgTele2;
  --------------------------------------------------------------------------

  --------------------------------------------------------------------------
  -- Получение региона MVNO по региону ЕИССД
  --------------------------------------------------------------------------
  function getMVNORegion(pi_reg_id  in number,
                         po_err_num out pls_integer,
                         po_err_msg out t_Err_Msg) return number is
    res number;
  begin
    select distinct t.id
      into res
      from t_dic_mvno_region t
     where t.reg_id = pi_reg_id;
    return res;
  exception
    when no_data_found then
      po_err_num := 1;
      po_err_msg := 'Неверный ИД региона';
      return null;
  end getMVNORegion;

end TMC_AB;
/
