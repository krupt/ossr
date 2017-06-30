CREATE OR REPLACE package body request_list is

  --фильтры заявок на подключение

  --------------------------------------------------------------
  --Фильтр по услугам в заявках ФЛ
  --%param pi_worker_id  Идентификатор пользователя                                              
  --%param pi_contractNum Номер договора
  --%param pi_request_id Номер заявки
  --%param pi_mrf_order_num Номер заявки в ИС МРФ
  --%param pi_only_mine Показывать только мои заявки (признак)
  --%param pi_FirstName Имя клиента
  --%param pi_LastName Фамилия клиента
  --%param pi_MiddleName Отчество клиента
  --%param pi_IDCardSeria Серия документа
  --%param pi_IDCardNumber Номер документа
  --%param pi_ELK_ACCOUNT Логин ЕЛК
  --%param pi_PERSONAL_ACCOUNT Лицевой счет
  --%param pi_phone Номер телефона (контактного)
  --%param pi_uslNumber Номер устройства/телефона
  --%param pi_mainEquipment Номер основного устройства/порта
  --%param pi_addr_id Локальный идентификатор улицы
  --%param pi_house_id Локальный идентификатор дома
  --%param pi_house_num Номер дома
  --%param pi_flat_id Номер квартиры
  --%param pi_services Категория услуги
  --%param pi_org_tab Подразделение
  --%param pi_org_child_include учитывать подчиненные
  --%param pi_type_request Тип заявки(метка)
  --        {*} null - все
  --        {*} '0' - без меток
  --        {*} 'connect_friend' - Подключи друга
  --        {*} 'anketa_mpz' - Анкетирование МПЗ
  --        {*} 'telemarketing' - Телемаркетинг
  --        {*} 'callback_request' - Обратный звонок
  --        {*} 'sms_request' - Заявка по sms
  --        {*} 'is_quick_request' - Быстрая заявка    
  --        {*} 'address_is_not_directory' - адрес не из справочника      
  --        {*} 'elk_request' - елк заявка
  --%param pi_addr_label Топ городов
  --%param pi_type_operation Тип операции: установка/смена технологии
  -- {*} 0 - установка
  -- {*} 2 - смена технологияя
  --%param pi_DateTimeBeg Дата начала периода
  --%param pi_DateTimeEnd Дата окончания периода
  --%param pi_sorting_date Тип периода
  -- {*} 0 - Фактическая дата приема заявки в ИС
  -- {*} 1 - Период последнего изменения
  -- {*} 2 - Период поступления в текущую фазу
  -- {*} 3 - Период приема заявки в ЕИССД/МПЗ
  --%param pi_device_id Оборудование в заявке
  --%param pi_Channels Список каналов поступления заявки
  --%param pi_States Список состояний заявки
  --%param pi_systems Список систем поступления заявки
  --%param pi_type_system Тип системы поступления заявки
  -- {*} 0 - региональна 
  -- {*} 1 -федеральная 
  -- {*} 2 - все    
  --%param pi_tech_posib Результат проверки ТхВ
  --        {*} null - любая
  --        {*} 1 Техническая возможность есть
  --        {*} 2 Технической возможности нет
  --        {*} 3 по адресу/телефону есть услуги того же типа
  --        {*} 4 проверка не проводилась  
  --        {*} 5 Техническая возможность неизвестна (по = U)
  --%param pi_send Обработка заявок сторонними системами
  --        {*} null - все
  --        {*} 'export_to_mrf'  Не отправлена в ИС МРФ
  --%param pi_tag_id Тип тарифного плана
  --%param pi_opt_tag Тип опции
  --%param pi_req_type Тип фильтра «По дате и времени для обратного звонка:» 
  --        {*} null - все
  --        {*} 1 «просроченные заявки» - на момент времени форм отчета дата и время окончания периода звонка уже наступили
  --        {*} 2 «текущие заявки» - на момент времени форм отчета дата и время начала периода звонка уже наступили, а время окончания периода звонка еще не наступило
  --        {*} 3 «время для звонка наступит в течение 8 час.» - на момент времени форм отчета дата и время начала периода для звонка должны наступить в течение 8 часов
  --        {*} 4 «будущие заявки» - на момент времени форм отчета дата начала периода для звонка уже наступила, а время начала периода звонка еще не наступило, или дата и время начала периода звонка еще не наступили  
  --        {*} 5 «заявки без даты обратного звонка» - заявки, у которых дата и время звонка не указаны
  --%param pi_num_page номер страницы                                                                                  
  --%param pi_count_req кол-во записей на странице
  --%param pi_column Номер колонки для сортировки
  --%param pi_sorting Тип сортировки: 0-по возрастанияю, 1-по убыванию
  --%param po_all_count Общее кол-во заявок
  --%param po_err_num Код ошибки
  --%param po_err_msg сообщение об ошибке
  --------------------------------------------------------------
  function get_list_phys_service_request(pi_worker_id        in number,
                                         pi_contractNum      in varchar2,
                                         pi_request_id       in tr_request.request_id%type,
                                         pi_mrf_order_num    in varchar2,
                                         pi_only_mine        in number,
                                         pi_FirstName        in t_person.person_firstname%type,
                                         pi_LastName         in t_person.person_lastname%type,
                                         pi_MiddleName       in t_person.person_middlename%type,
                                         pi_IDCardSeria      in t_documents.doc_series%type,
                                         pi_IDCardNumber     in t_documents.doc_number%type,
                                         pi_ELK_ACCOUNT      in varchar2,
                                         pi_PERSONAL_ACCOUNT in varchar2,
                                         pi_phone            in t_person.person_phone%type,
                                         pi_uslNumber        in tr_request_service_detail.usl_number%type,
                                         pi_mainEquipment    in tr_request_service_detail.main_equipment%type,
                                         ------------
                                         pi_addr_id   in number,
                                         pi_house_id  in number,
                                         pi_house_num in varchar2,
                                         pi_flat_id   in varchar2,
                                         ------------
                                         pi_services          in num_tab,
                                         pi_org_tab           in num_tab,
                                         pi_org_child_include in number,
                                         pi_type_request      in varchar2,
                                         pi_addr_label        in number,
                                         pi_type_operation    in num_tab,
                                         pi_DateTimeBeg       in date,
                                         pi_DateTimeEnd       in date,
                                         pi_sorting_date      in number,
                                         pi_device_id         in number,
                                         pi_device_card_type  in number, -- тип оборудования из Лиры
                                         pi_Channels          in num_tab,
                                         pi_States            in num_tab,
                                         pi_systems           in num_tab,
                                         pi_type_system       in number,
                                         pi_tech_posib        in number,
                                         pi_send              in request_param_type,
                                         pi_tag_id            in number,
                                         pi_opt_tag           in number,
                                         pi_req_type          in number,
                                         ---------
                                         pi_num_page  in number,
                                         pi_count_req in number,
                                         pi_column    in number,
                                         pi_sorting   in number,
                                         po_all_count out number,
                                         po_err_num   out number,
                                         po_err_msg   out varchar2)
    return sys_refcursor is
    l_func_name varchar2(100) := 'request_list.get_list_phys_service_request';
  
    res            sys_refcursor;
    l_order_asc    number; -- по возрастанияю
    l_order_desc   number; -- по убыванию
    l_max_num_page number;
    l_num_page     number;
  
    l_org_num_2  array_num_2 := array_num_2();
    l_org_tab    num_tab;
    User_Orgs    Num_Tab;
    User_Orgs2   Num_Tab;
    l_reg_org    ARRAY_NUM_2;
    User_reg_org ARRAY_NUM_2;
  
    l_callback_view number := 0;
    l_sms_view      number := 0;
    l_date_cr_beg   date;
    l_date_cr_end   date;
    l_date_ch_beg   date;
    l_date_ch_end   date;
    l_tag_tab       num_tab;
    l_service_tab   num_tab := pi_services;
  
    l_cnt number;
  
    l_all_req1  num_tab; -- по датам
    l_all_req2  num_tab;
    l_all_req3  num_tab;
    l_all_req4  num_tab;
    l_all_req5  num_tab;
    l_all_req6  num_tab;
    l_all_req7  num_tab;
    l_all_req8  num_tab;
    l_all_req9  num_tab;
    l_all_req10 num_tab;
  
    l_all_req             num_tab;
    l_col_request         request_Order_Tab;
    l_request_id          number;
    l_filt_callback_hours number;
  begin
    logging_pkg.debug(pi_addr_id || ';' || pi_house_id || ';' ||
                      pi_house_num || ';' || pi_flat_id,
                      l_func_name);
    if pi_org_tab is not null then
      select rec_num_2(number_1 => column_value,
                       number_2 => pi_org_child_include) bulk collect
        into l_org_num_2
        from table(pi_org_tab);
    else
      l_org_num_2 := array_num_2(rec_num_2(1, nvl(pi_org_child_include, 1)));
    end if;
  
    l_org_tab := get_orgs_tab_for_multiset(pi_orgs            => l_org_num_2,
                                           Pi_worker_id       => pi_worker_id,
                                           pi_block           => 1,
                                           pi_org_relation    => null,
                                           pi_is_rtmob        => 0,
                                           pi_tm_1009_include => 1);
  
    User_Orgs := SECURITY_PKG.Get_User_Orgs_Tab_By_Right_str(pi_worker_id,
                                                             'SD.REQUEST.PHYS.VIEW_LIST',
                                                             null);
  
    User_Orgs2 := intersects(l_org_tab, User_Orgs);
  
    l_reg_org := SECURITY_PKG.get_region_by_worker_right2(pi_worker_id => pi_worker_id,
                                                          pi_right_str => string_tab('SD.REQUEST.PHYS.VIEW_LIST'),
                                                          pi_org_id    => l_org_tab);
  
    User_reg_org := intersect_num2(User_Orgs2, l_reg_org);
  
    If pi_sorting = 0 then
      l_order_asc := NVL(pi_column, 2);
    else
      l_order_desc := NVL(pi_column, 2);
    end If;
  
    if pi_DateTimeBeg is not null and nvl(pi_sorting_date, 0) in (0, 3) then
      l_date_cr_beg := pi_DateTimeBeg - Constant_pkg.c_GMT;
    else
      l_date_cr_beg := to_date('01.01.1900', 'dd.mm.yyyy');
    end if;
  
    if pi_DateTimeEnd is not null and nvl(pi_sorting_date, 0) in (0, 3) then
      l_date_cr_end := pi_DateTimeEnd - Constant_pkg.c_GMT;
    else
      l_date_cr_end := to_date('31.12.2999', 'dd.mm.yyyy');
    end if;
  
    if pi_DateTimeBeg is not null and nvl(pi_sorting_date, 0) in (1, 2) then
      l_date_ch_beg := pi_DateTimeBeg - Constant_pkg.c_GMT;
    else
      l_date_ch_beg := to_date('01.01.1900', 'dd.mm.yyyy');
    end if;
  
    if pi_DateTimeEnd is not null and nvl(pi_sorting_date, 0) in (1, 2) then
      l_date_ch_end := pi_DateTimeEnd - Constant_pkg.c_GMT;
    else
      l_date_ch_end := to_date('31.12.2999', 'dd.mm.yyyy');
    end if;
  
    if (pi_DateTimeBeg is not null or pi_DateTimeEnd is not null) then
      if pi_sorting_date = 0 then
        select /*+ index(p IDX_REQUEST_SERVICE_DATCR_F) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_create_fact >= l_date_cr_beg
           and p.date_create_fact < l_date_cr_end;
      
      elsif pi_sorting_date = 1 then
        select /*+ index(p IDX_REQUEST_SERVICE_DATCH_F) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_change_fact >= l_date_ch_beg
           and p.date_change_fact < l_date_ch_end;
      
      elsif pi_sorting_date = 2 then
        --по дате перехода в текущее состояние
        select /*+ index(p IDX_TR_PRODUCT_DCHST) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_change_state >= l_date_ch_beg
           and p.date_change_state < l_date_ch_end;
      elsif pi_sorting_date = 3 then
        select /*+ index(p IDX_TR_PRODUCT_DCR) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_create >= l_date_cr_beg
           and p.date_create < l_date_cr_end;
      end if;
    end if;
  
    select count(*)
      into l_cnt
      from table(pi_States) s
     where s.column_value in (17, 18, 20, 21, 22, 23);
  
    --определим показывать коллбак или нет
    -- состояние колбек или метка обратный звонок
    if l_cnt > 0 or nvl(pi_type_request, '0') = 'callback_request' then
      l_callback_view := 1;
    end if;
  
    if l_cnt > 0 or nvl(pi_type_request, '0') = 'sms_request' then
      l_sms_view := 1;
      l_service_tab.extend;
      l_service_tab(l_service_tab.count) := 13;
    end if;
  
    --Топ городов
    if pi_addr_label is not null then
      select distinct p.id bulk collect
        into l_all_req9
        from table(l_all_req1) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request r
          on r.id = p.request_id
        join t_address a
          on a.addr_id = r.address_id
        join (select ao.id
                from t_address_object ao
              connect by prior ao.id = ao.parent_id
               start with ao.id in
                          (select al.local_id
                             from t_addr_label al
                            where al.lable_id = pi_addr_label)) aa
          on aa.id = a.addr_obj_id;
      l_all_req1 := l_all_req9;
    end if;
  
    if (pi_FirstName is not null or pi_LastName is not null or
       pi_MiddleName is not null or pi_phone is not null) then
    
      select id bulk collect
        into l_all_req2
        from (select t.id
                from (select p.id,
                             pp.person_firstname,
                             pp.person_lastname,
                             pp.person_middlename,
                             pp.person_phone,
                             pp.person_home_phone
                        from tr_request r
                        join tr_request_service p
                          on r.id = p.request_id
                        join t_clients cl
                          on cl.client_id = r.client_id
                         and cl.client_type = 'P'
                        join t_clients p_cl
                          on p_cl.client_id = r.contact_person_id
                         and p_cl.client_type = 'P'
                        join t_person pp
                          on p_cl.fullinfo_id = pp.person_id
                       where lower(pp.person_firstname) =
                             lower(trim(pi_FirstName))
                          or lower(pp.person_lastname) =
                             lower(trim(pi_LastName))
                          or lower(pp.person_middlename) =
                             lower(trim(pi_MiddleName))
                          or pp.person_phone = pi_phone
                          or pp.person_home_phone = pi_phone) t
               where (pi_FirstName is null or
                     lower(t.person_firstname) = lower(trim(pi_FirstName)))
                 and (pi_LastName is null or
                     lower(t.person_lastname) = lower(trim(pi_LastName)))
                 and (pi_MiddleName is null or lower(t.person_middlename) =
                     lower(trim(pi_MiddleName)))
                 and (pi_phone is null or t.person_phone = pi_phone or
                     t.person_home_phone = pi_phone)
              union
              select t.id
                from (select p.id,
                             pp.person_firstname,
                             pp.person_lastname,
                             pp.person_middlename,
                             pp.person_phone,
                             pp.person_home_phone
                        from tr_request r
                        join tr_request_service p
                          on r.id = p.request_id
                        join t_clients cl
                          on cl.client_id = r.client_id
                         and cl.client_type = 'P'
                        join t_person pp
                          on cl.fullinfo_id = pp.person_id
                       where lower(pp.person_firstname) =
                             lower(trim(pi_FirstName))
                          or lower(pp.person_lastname) =
                             lower(trim(pi_LastName))
                          or lower(pp.person_middlename) =
                             lower(trim(pi_MiddleName))) t
               where (pi_FirstName is null or
                     lower(t.person_firstname) = lower(trim(pi_FirstName)))
                 and (pi_LastName is null or
                     lower(t.person_lastname) = lower(trim(pi_LastName)))
                 and (pi_MiddleName is null or lower(t.person_middlename) =
                     lower(trim(pi_MiddleName))));
    end if;
  
    if pi_request_id is not null then
      begin
        l_request_id := to_number(pi_request_id);
        select s.id bulk collect
          into l_all_req10
          from tr_request_service s
          join tr_request r
            on r.id = s.request_id
         where r.id = l_request_id
            or s.id = l_request_id;
      exception
        when others then
          select s.id bulk collect
            into l_all_req10
            from tr_request_service s
            join tr_request r
              on r.id = s.request_id
           where r.request_id = pi_request_id;
      end;
    end if;
  
    if (pi_contractNum is not null or pi_mrf_order_num is not null or
       pi_ELK_ACCOUNT is not null or pi_PERSONAL_ACCOUNT is not null) then
      select t.id bulk collect
        into l_all_req3
        from (select p.id,
                     r.request_id,
                     r.dogovor_number,
                     p.mrf_order_num,
                     r.elk_account,
                     r.personal_account
                from tr_request_service p
                join tr_request r
                  on r.id = p.request_id
               where r.dogovor_number = pi_contractNum
                  or p.mrf_order_num = pi_mrf_order_num
                  or r.elk_account = pi_ELK_ACCOUNT
                  or r.personal_account = pi_PERSONAL_ACCOUNT) t
       where (t.dogovor_number = pi_contractNum or pi_contractNum is null)
         and (t.mrf_order_num = pi_mrf_order_num or
             pi_mrf_order_num is null)
         and (t.elk_account = pi_ELK_ACCOUNT or pi_ELK_ACCOUNT is null)
         and (t.personal_account = pi_PERSONAL_ACCOUNT or
             pi_PERSONAL_ACCOUNT is null);
    end if;
  
    if (pi_IDCardSeria is not null or pi_IDCardNumber is not null) then
      select p.id bulk collect
        into l_all_req5
        from tr_request r
        join tr_request_service p
          on r.id = p.request_id
        join t_system s
          on r.system_id = s.id
        join t_clients cl
          on cl.client_id = r.client_id
         and cl.client_type = 'P'
        join t_person pp
          on cl.fullinfo_id = pp.person_id
        join t_documents d
          on d.doc_id = pp.doc_id
       where d.doc_series = upper(trim(pi_IDCardSeria))
         and d.doc_number = upper(trim(pi_IDCardNumber));
    end if;
  
    if (pi_addr_id is not null and
       (pi_house_id is not null or pi_house_num is not null)) then
      select p.id bulk collect
        into l_all_req4
        from (select r.id
                from tr_request r
                join t_address a
                  on r.address_id = a.addr_id
               where a.addr_obj_id = pi_addr_id
                 and (pi_house_id is null or
                     pi_house_id = a.addr_house_obj_id)
                 and (pi_house_num is null or
                     trim(pi_house_num) = a.addr_building)
                 and (pi_flat_id is null or trim(pi_flat_id) = a.addr_office)) t
        join tr_request_service p
          on p.request_id = t.id;
    end if;
  
    -- фильтр по меткам    
    if nvl(pi_type_request, '0') != '0' then
      select distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req1) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_params par
          on par.request_id = p.request_id
         and par.key = pi_type_request
         and nvl(par.value, '0') = '1'
       where l_callback_view = 1
          or l_sms_view = 1
          or p.state_id not in (17, 18, 20, 21, 22, 23);
    elsif pi_type_request is null and l_all_req1 is not null then
      --нам без разницы какие метки, главное убрать колбак, если он не нужен
      if l_callback_view = 1 and l_sms_view = 1 then
        l_all_req8 := l_all_req1;
      elsif l_sms_view = 1 and l_callback_view = 0 then
        --исключим колбек в статусах колбек
        select distinct p.id bulk collect
          into l_all_req8
          from table(l_all_req1) req
          join tr_request_service p
            on p.id = req.column_value
          left join tr_request_params par
            on par.request_id = p.request_id
           and par.key = 'callback_request'
           and p.state_id in (17, 18, 20, 21, 22, 23)
         where nvl(par.value, 0) = 0;
      elsif l_sms_view = 0 and l_callback_view = 1 then
        --исключим смс в статусах колбек
        select distinct p.id bulk collect
          into l_all_req8
          from table(l_all_req1) req
          join tr_request_service p
            on p.id = req.column_value
          left join tr_request_params par
            on par.request_id = p.request_id
           and par.key = 'sms_request'
           and p.state_id in (17, 18, 20, 21, 22, 23)
         where nvl(par.value, 0) = 0;
      else
        select /*+ use_nl(rc req) */
        distinct p.id bulk collect
          into l_all_req8
          from table(l_all_req1) req
          join tr_request_service p
            on p.id = req.column_value
         where p.state_id not in (17, 18, 20, 21, 22, 23);
      end if;
    elsif pi_type_request = '0' then
      select distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req1) req
        join tr_request_service p
          on p.id = req.column_value
        left join tr_request_params par
          on par.request_id = p.request_id
         and nvl(par.value, '0') = '1'
         and par.key in ('connect_friend',
                         'anketa_mpz',
                         'telemarketing',
                         'callback_request',
                         'sms_request',
                         'is_quick_request',
                         'elk_request',
                         'address_is_not_directory')
       where (l_callback_view = 1 or
             l_sms_view = 1 and p.state_id not in (17, 18, 20, 21, 22, 23))
         and nvl(par.value, '0') = '0';
    end if;
  
    --Фильтр по уже подключенным услугам
    if nvl(pi_tech_posib, 0) = 3 then
      select distinct s.id bulk collect
        into l_all_req7
        from table(l_all_req8) req
        join tr_request_service s
          on req.column_value = s.id
        join TR_REQUEST_SERV_EXIST e
          on e.request_id = s.request_id
         and e.service_type = s.product_category
       where nvl(e.is_old_address,0)=0  ;
    end if;
  
    --Фильтр по проверке ТВ
    if nvl(pi_tech_posib, 0) in (1, 2, 4, 5) then
      select distinct aa.id bulk collect
        into l_all_req7
        from (select /* ordered use_nl(req rc) use_nl(rc th) */
               p.id, max(th.is_success) tech_posib
                from table(l_all_req8) req
                join tr_request_service p
                  on req.column_value = p.id
                join tr_request r
                  on r.id = p.request_id
                left join tr_request_tech_poss th
                  on th.request_id = p.request_id
                 and th.service_type = p.product_category
                 and th.address_id = r.address_id
                 and nvl(th.tech_id, 0) = nvl(p.tech_id, nvl(th.tech_id, 0))
               group by p.id) aa
       where nvl(aa.tech_posib, -1) =
             decode(pi_tech_posib, 1, 1, 2, 0, 4, -1, 5, 5);
    end if;
  
    if pi_uslNumber is not null or pi_mainEquipment is not null then
      select distinct s.service_id bulk collect
        into l_all_req6
        from tr_request_service_detail s
       where (s.main_equipment = pi_mainEquipment or
             pi_mainEquipment is null)
         and (s.usl_number = pi_uslNumber or pi_uslNumber is null);
    end if;
  
    if l_all_req7 is null then
      l_all_req := intersects(l_all_req, l_all_req8);
    else
      l_all_req := intersects(l_all_req, l_all_req7);
    end if;
    l_all_req := intersects(l_all_req, l_all_req2);
    l_all_req := intersects(l_all_req, l_all_req3);
    l_all_req := intersects(l_all_req, l_all_req10);
    l_all_req := intersects(l_all_req, l_all_req4);
    l_all_req := intersects(l_all_req, l_all_req5);
    l_all_req := intersects(l_all_req, l_all_req6);
  
    if pi_send is not null and pi_send.key is not null then
      --Обработка заявок сторонними системами
      l_all_req8 := null;
      select distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_params par
          on par.request_id = p.request_id
         and par.key = pi_send.key
       where par.value = pi_send.value;
    
      l_all_req  := l_all_req8;
      l_all_req8 := null;
    end if;
  
    --фильтр по тегам опций
    if pi_opt_tag is not null then
      l_all_req8 := null;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_product_option t
          on t.service_id = req.column_value
        join t_opt_tag opt
          on opt.option_id = t.option_id
       where opt.tag_id = pi_opt_tag;
    
      l_all_req  := l_all_req8;
      l_all_req8 := null;
    end if;
  
    --фильтр по оборудованию
/*    if pi_device_id is not null then
      l_all_req8 := null;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_device t
          on t.request_id = p.request_id
       where t.device_type = pi_device_id;
    
      l_all_req  := l_all_req8;
      l_all_req8 := null;
    end if;*/
    
    -- фильтр по оборудованию из ЕИССД
    if pi_device_id is not null then
      l_all_req8 := null;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_device t
          on t.request_id = p.request_id
       where t.device_type = pi_device_id
         and not exists (select ord.orders_id
                from tr_request_orders ord
                join t_orders o
                  on ord.orders_id = o.id
               where o.order_type = 5
                 and ord.request_id = p.request_id);               
      l_all_req := l_all_req8;
      l_all_req8 := null;
    end if;  
    -- по типам оборудование из Лиры
    if pi_device_card_type is not null then      
      l_all_req8 := null;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_orders ro
          on ro.request_id = p.request_id
        join t_orders o
          on o.id = ro.orders_id
         and o.order_type = 5
        join tr_request_device_card rdc
          on rdc.request_id = p.request_id
       where rdc.type_equipment = pi_device_card_type
         and rdc.is_exists = 0;
               
      l_all_req := l_all_req8;
      l_all_req8 := null;
    end if;
    
    if pi_type_request is null and l_callback_view = 0 and l_sms_view = 0 and
       l_all_req1 is null then
      --метки не были переданы, значит  по параметрам клиента. 
      --Нужно убрать колбак статусы, раз они не нужен           
    
      select /*+ use_nl(rc req) */
      distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
       where p.state_id not in (17, 18, 20, 21, 22, 23);
    
      l_all_req := l_all_req8;
    end if;
  
    --подберем тарифы по тегам
    if pi_tag_id is not null then
      select t.id bulk collect
        into l_tag_tab
        from t_dic_tag t
      connect by t.parent_id = prior t.id
       start with t.id = pi_tag_id;
    
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_service_product ps
          on ps.service_id = req.column_value
        join tr_request_product p
          on p.id = ps.product_id
        join t_version_tag vt
          on vt.object_id = p.tar_id
       where vt.tag_id in (select column_value from table(l_tag_tab));
    
      l_all_req := l_all_req8;
    end if;
    --
    if pi_req_type is not null then
      l_all_req8 := null;
      if pi_req_type = 3 then
        l_filt_callback_hours := constant_pkg.c_filt_callback_hours;
      end if;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service rs
          on rs.id = req.column_value
        join tr_request r
          on r.id = rs.request_id
       where (pi_req_type = 1 and r.instwishtimecall2 < sysdate)
          or (pi_req_type = 2 and r.instwishtimecall1 < sysdate and
             r.instwishtimecall2 > sysdate)
          or (pi_req_type = 3 and r.instwishtimecall1 between sysdate and
             sysdate + l_filt_callback_hours / 24)
          or (pi_req_type = 4 and r.instwishtimecall1 > sysdate)
          or (pi_req_type = 5 and r.instwishtimecall1 is null);
      l_all_req := l_all_req8;
    end if;
  
    --/*+ ordered */
    select request_Order_Type(id, rownum) bulk collect
      into l_col_request
      from (select /*+ ordered */
             p.id
              from (select /*+ use_nl(p xxx) */
                     p.*
                      from table(l_all_req) xxx
                      join tr_request_service p
                        on p.id = xxx.column_value) p
              join tr_request r
                on r.id = p.request_id
              join table(User_reg_org) reg_org
                on reg_org.number_1 = r.org_id
               and nvl(r.region_id, '-1') =
                   nvl(reg_org.number_2, nvl(r.region_id, -1))
              join t_system s
                on r.system_id = s.id
              join table(pi_systems) ss
                on ss.column_value = r.system_id
              join Table(pi_Channels) ch
                on ch.column_value = r.channel_id
              join table(l_service_tab) serv
                on serv.column_value = p.product_category
              join t_clients cl
                on cl.client_id = r.client_id
               and cl.client_type = 'P'
              join t_clients p_cl
                on p_cl.client_id = r.contact_person_id
               and p_cl.client_type = 'P'
              join t_person pp
                on p_cl.fullinfo_id = pp.person_id
              left join t_address a
                on r.address_id = a.addr_id
              left join t_dic_region dr
                on dr.reg_id = r.region_id
              left join t_dic_mrf dm
                on dm.id = dr.mrf_id
              left join t_Organizations orgM
                on dm.org_id = orgM.Org_id
              left join t_Organizations orgF
                on dr.org_id = orgf.Org_id
              join t_dic_request_state drs
                on p.state_id = drs.state_id
             where p.state_id in
                   (select t.column_value from table(pi_States) t)
               and (pi_type_system = 2 or (s.is_federal = pi_type_system))
               and ((nvl(pi_only_mine, 0) = 1 and
                   p.worker_create = pi_worker_id) or
                   nvl(pi_only_mine, 0) <> 1)
               and p.state_id not in (24)
               and p.type_request in
                   (select * from table(pi_type_operation))
             order by decode(l_order_asc,
                             9,
                             (case
                               when r.instwishtimecall1 > sysdate then
                                4
                               when sysdate between r.instwishtimecall1 and
                                    r.instwishtimecall2 then
                                2
                               when r.instwishtimecall1 is null then
                                3
                               else
                                1
                             end),
                             null) asc,
                      decode(l_order_asc,
                             null,
                             null,
                             1,
                             lpad(r.request_id, 13, '0'),
                             2,
                             to_char(p.date_create_fact,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             3,
                             lower(pp.person_lastname || ' ' ||
                                   pp.person_firstname || ' ' ||
                                   pp.person_middlename),
                             4,
                             nvl2(a.addr_oth,
                                  a.addr_oth || ' ' || a.addr_building || '-' ||
                                  a.addr_office,
                                  substr(a.addr_city,
                                         0,
                                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                                  a.addr_street || ' ' || a.addr_building || '-' ||
                                  a.addr_office),
                             5,
                             drs.state_name,
                             6,
                             to_char(p.date_change_state,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             8,
                             upper(orgM.Org_Name || orgf.org_name),
                             9,
                             case
                               when r.instwishtimecall1 > sysdate then
                                to_char(nvl(r.instwishtimecall1, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                               else
                                to_char(nvl(r.instwishtimecall2, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                             end,
                             null) asc,
                      decode(l_order_desc,
                             9,
                             (case
                               when r.instwishtimecall1 > sysdate then
                                4
                               when sysdate between r.instwishtimecall1 and
                                    r.instwishtimecall2 then
                                2
                               when r.instwishtimecall1 is null then
                                3
                               else
                                1
                             end),
                             null) desc,
                      decode(l_order_desc,
                             null,
                             null,
                             1,
                             lpad(r.request_id, 13, '0'),
                             2,
                             to_char(p.date_create_fact,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             3,
                             lower(pp.person_lastname || ' ' ||
                                   pp.person_firstname || ' ' ||
                                   pp.person_middlename),
                             4,
                             nvl2(a.addr_oth,
                                  a.addr_oth || ' ' || a.addr_building || '-' ||
                                  a.addr_office,
                                  substr(a.addr_city,
                                         0,
                                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                                  a.addr_street || ' ' || a.addr_building || '-' ||
                                  a.addr_office),
                             5,
                             drs.state_name,
                             6,
                             to_char(p.date_change_state,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             8,
                             upper(orgM.Org_Name || orgf.org_name),
                             9,
                             case
                               when r.instwishtimecall1 > sysdate then
                                to_char(nvl(r.instwishtimecall1, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                               else
                                to_char(nvl(r.instwishtimecall2, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                             end,
                             null) desc);
  
    po_all_count := l_col_request.count;
  
    l_max_num_page := round(po_all_count / nvl(pi_count_req, 1));
  
    if (pi_num_page > l_max_num_page and l_max_num_page <> 0) then
      l_num_page := l_max_num_page + 1;
    else
      l_num_page := nvl(pi_num_page, 1);
    end if;
  
    open res for
      select /*+ ordered */
       p.id service_id,
       r.id,
       r.request_id,
       p.date_create_fact date_create,
       pp.person_lastname || ' ' || pp.person_firstname || ' ' ||
       pp.person_middlename as fio,
       nvl2(aa.addr_oth,
            aa.addr_oth || nvl2(aa.addr_building,
                                ', д. ' || aa.addr_building,
                                aa.addr_building) ||
            nvl2(aa.addr_office,
                 ', кв. ' || aa.addr_office,
                 aa.addr_office),
            substr(aa.addr_city, 0, Constant_pkg.c_adress_city_name_length) || ' ' ||
            aa.addr_street || nvl2(aa.addr_building,
                                   ', д. ' || aa.addr_building,
                                   aa.addr_building) ||
            nvl2(aa.addr_office,
                 ', кв. ' || aa.addr_office,
                 aa.addr_office)) adr,
       drs.state_name State,
       p.date_change_state,
       
       p.product_category,
       nvl(orgM.Org_Name, o_rtk.org_name) mrf,
       nvl(orgf.org_name, o_rtk.org_name) filial,
       u.usr_login as create_user,
       r.channel_id,
       oo.org_name,
       pp.person_phone,
       pp.person_home_phone,
       r.instwishtimecall1,
       r.instwishtimecall2,
       u.employee_number
        from (Select request_id, rn
                from table(l_col_request)
               where rn between (l_num_page - 1) * pi_count_req + 1 and
                     (l_num_page) * pi_count_req) col_rc
        join tr_request_service p
          on p.id = col_rc.request_id
        join tr_request r
          on p.request_id = r.id
        join t_users u
          on u.usr_id = p.worker_create
        join t_clients cl
          on cl.client_id = r.client_id
         and cl.client_type = 'P'
        join t_clients p_cl
          on p_cl.client_id = r.contact_person_id
         and p_cl.client_type = 'P'
        join t_person pp
          on pp.person_id = p_cl.fullinfo_id
        left join t_dic_request_state drs
          on p.state_id = drs.state_id
        join t_address aa
          on r.address_id = aa.addr_id
        left join t_dic_region dr
          on dr.reg_id = r.region_id
        left join t_dic_mrf dm
          on dm.id = dr.mrf_id
        left join t_Organizations orgM
          on dm.org_id = orgM.Org_id
        left join t_Organizations orgF
          on dr.org_id = orgf.Org_id
        left join t_organizations o_rtk
          on o_rtk.org_id = 0
        join t_organizations oo
          on oo.org_id = r.org_id
       order by col_rc.rn;
  
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;   

  ----------------------------------------------------------------------------------
  -- Список Заявок на подключение услуг ФЛ
  -- %param pi_worker_id Пользователь
  -- %param pi_org_tab Организация
  -- %param pi_org_child_include учитывать подчиненные
  -- %param pi_DateTimeBeg Начало периода
  -- %param pi_DateTimeEnd конец периода
  -- %param pi_States Статусы заявки
  --        {*}1 - новая
  --        {*}2 - выполняется
  --        {*}3 - выполнена
  --        {*}4 - удалена  
  -- %param pi_type_system Тип системы - число. принимается значения 0 - региональна, 1 -федеральная, 2 - все
  -- %param pi_only_mine Признак показывать просроченные заявки - Число. Принимает значения 0/1
  -- %param pi_overdue Признак показывать только просроченные
  -- %param pi_services
  -- %param pi_Channels
  -- %param pi_systems

  -- %param pi_addr_label метка адресного объекта - число
  -- %param pi_type_request признак "Тип заявки"
  --        {*} null - все
  --        {*} '0' - без меток
  --        {*} 'connect_friend' - Подключи друга
  --        {*} 'anketa_mpz' - Анкетирование МПЗ
  --        {*} 'telemarketing' - Телемаркетинг
  --        {*} 'callback_request' - Обратный звонок
  --        {*} 'sms_request' - Заявка по sms
  --        {*} 'is_quick_request' - Быстрая заявка  
  --        {*} 'elk_request' - елк заявка
  --%param pi_send Обработка заявок сторонними системами
  --        {*} null - все
  --        {*} 'export_to_mrf'  Не отправлена в ИС МРФ
  --%param pi_req_type Тип фильтра «По дате и времени для обратного звонка:» 
  --        {*} null - все
  --        {*} 1 «просроченные заявки» - на момент времени форм отчета дата и время окончания периода звонка уже наступили
  --        {*} 2 «текущие заявки» - на момент времени форм отчета дата и время начала периода звонка уже наступили, а время окончания периода звонка еще не наступило
  --        {*} 3 «время для звонка наступит в течение 8 час.» - на момент времени форм отчета дата и время начала периода для звонка должны наступить в течение 8 часов
  --        {*} 4 «будущие заявки» - на момент времени форм отчета дата начала периода для звонка уже наступила, а время начала периода звонка еще не наступило, или дата и время начала периода звонка еще не наступили  
  --        {*} 5 «заявки без даты обратного звонка» - заявки, у которых дата и время звонка не указаны
  -- %param pi_num_page Номер страницы
  -- %param pi_count_req Количество записей на странице
  -- %param pi_column Номер колонки для сортировки - число
  -- %param pi_sorting Принцип сортировки. Число. Принимает значения:
  --        {*} 0-по возрастанию,
  --        {*} 1-по убыванию
  -- %param po_all_count Возвращается общее кол-во заявок, подходящих под условия данных
  -- %param po_err_num возвращается код ошибки
  -- %param po_err_msg возвращается сообщение об ошибке
  --@return возвращается курсор с информацией о заявках ФЛ
  ----------------------------------------------------------------------------------
  function Get_List_Phys_Request(pi_worker_id         in number,
                                 pi_org_tab           in num_tab,
                                 pi_org_child_include in number,
                                 
                                 pi_DateTimeBeg in date,
                                 pi_DateTimeEnd in date,
                                 pi_States      in num_tab, --
                                 pi_type_system in number,
                                 pi_only_mine   in number,
                                 pi_overdue     in number,
                                 
                                 pi_services in num_tab,
                                 pi_Channels in num_tab,
                                 pi_systems  in num_tab,
                                 
                                 pi_addr_label   in number,
                                 pi_type_request in varchar2,
                                 pi_send         in request_param_type,
                                 
                                 pi_req_type in number,
                                 
                                 pi_num_page  in number,
                                 pi_count_req in number,
                                 pi_column    in number,
                                 pi_sorting   in number,
                                 
                                 po_all_count out number,
                                 po_err_num   out number,
                                 po_err_msg   out varchar2)
    return sys_refcursor is
    l_func_name varchar2(100) := 'request_list.Get_List_Phys_Request';
  
    res            sys_refcursor;
    l_order_asc    number; -- по возрастанияю
    l_order_desc   number; -- по убыванию
    l_max_num_page number;
    l_num_page     number;
  
    l_org_num_2  array_num_2 := array_num_2();
    l_org_tab    num_tab;
    User_Orgs    Num_Tab;
    User_Orgs2   Num_Tab;
    l_reg_org    ARRAY_NUM_2;
    User_reg_org ARRAY_NUM_2;
  
    l_service_tab num_tab := pi_services;
  
    l_all_req             num_tab;
    l_all_req1            num_tab;
    l_all_req2            num_tab;
    l_col_request         request_Order_Tab;
    l_filt_callback_hours number;
  begin
    if pi_org_tab is not null then
      select rec_num_2(number_1 => column_value,
                       number_2 => pi_org_child_include) bulk collect
        into l_org_num_2
        from table(pi_org_tab);
    else
      l_org_num_2 := array_num_2(rec_num_2(1, nvl(pi_org_child_include, 1)));
    end if;
  
    l_org_tab := get_orgs_tab_for_multiset(pi_orgs            => l_org_num_2,
                                           Pi_worker_id       => pi_worker_id,
                                           pi_block           => 1,
                                           pi_org_relation    => null,
                                           pi_is_rtmob        => 0,
                                           pi_tm_1009_include => 1);
  
    User_Orgs := SECURITY_PKG.Get_User_Orgs_Tab_By_Right_str(pi_worker_id,
                                                             'SD.REQUEST.PHYS.VIEW_LIST',
                                                             null);
  
    User_Orgs2 := intersects(l_org_tab, User_Orgs);
  
    l_reg_org := SECURITY_PKG.get_region_by_worker_right2(pi_worker_id => pi_worker_id,
                                                          pi_right_str => string_tab('SD.REQUEST.PHYS.VIEW_LIST'),
                                                          pi_org_id    => l_org_tab);
  
    User_reg_org := intersect_num2(User_Orgs2, l_reg_org);
  
    If pi_sorting = 0 then
      l_order_asc := NVL(pi_column, 2);
    else
      l_order_desc := NVL(pi_column, 2);
    end If;
  
    if nvl(pi_type_request, '0') = 'sms_request' then
      --l_sms_view := 1;
      l_service_tab.extend;
      l_service_tab(l_service_tab.count) := 13;
    end if;
  
    select /*+ index(r DATE_CHANGE_FACT) */p.id bulk collect
      into l_all_req1
      from tr_request r
      join tr_request_service p
        on p.request_id = r.id
     where r.date_create_fact >= pi_DateTimeBeg
       and r.date_create_fact < pi_DateTimeEnd
       and p.state_id != 24;

    select s.id bulk collect
      into l_all_req
      from table(l_all_req1) a
      join tr_request_service s
        on s.id = a.column_value
      join tr_request r
        on r.id = s.request_id
      join table(User_reg_org) reg_org
        on reg_org.number_1 = r.org_id
       and nvl(r.region_id, '-1') =
           nvl(reg_org.number_2, nvl(r.region_id, -1));
  
    --Топ городов
    if pi_addr_label is not null then
      l_all_req2 := null;
      select distinct p.id bulk collect
        into l_all_req2
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request r
          on r.id = p.request_id
        join t_address a
          on a.addr_id = r.address_id
        join (select ao.id
                from t_address_object ao
              connect by prior ao.id = ao.parent_id
               start with ao.id in
                          (select al.local_id
                             from t_addr_label al
                            where al.lable_id = pi_addr_label)) aa
          on aa.id = a.addr_obj_id;
      l_all_req  := l_all_req2;
      l_all_req2 := null;
    end if;
  
    -- фильтр по меткам    
    if nvl(pi_type_request, '0') != '0' then
      l_all_req2 := null;
      select distinct s.id bulk collect
        into l_all_req2
        from table(l_all_req) req
        join tr_request_service s
          on s.id = req.column_value
        join tr_request_params p
          on p.request_id = s.request_id
         and p.key = pi_type_request
         and nvl(p.value, '0') = '1';
      l_all_req  := l_all_req2;
      l_all_req2 := null;
    elsif pi_type_request = '0' then
      l_all_req2 := null;
      select distinct s.id bulk collect
        into l_all_req2
        from table(l_all_req) req
        join tr_request_service s
          on s.id = req.column_value
        left join tr_request_params p
          on p.request_id = s.request_id
         and p.key in ('connect_friend',
                       'anketa_mpz',
                       'telemarketing',
                       'callback_request',
                       'sms_request',
                       'is_quick_request',
                       'elk_request',
                       'address_is_not_directory')
       where nvl(p.value, '0') = '0';
      l_all_req  := l_all_req2;
      l_all_req2 := null;
    end if;
  
    --Обработка заявок сторонними системами
    if pi_send is not null and pi_send.key is not null then
      l_all_req2 := null;
      select distinct s.id bulk collect
        into l_all_req2
        from table(l_all_req) req
        join tr_request_service s
          on s.id = req.column_value
        join tr_request_params p
          on p.request_id = s.request_id
         and p.key = pi_send.key
       where p.value = pi_send.value;
    
      l_all_req  := l_all_req2;
      l_all_req2 := null;
    end if;
  
    if pi_overdue = 1 then
      --показывать только просроченные
      l_all_req2 := null;
      select t.id bulk collect
        into l_all_req2
        from (select s.id,
                     max(case
                           when sysdate - s.date_change_state > 5 / 24 / 60 and
                                s.state_id = 1 then
                            1
                           when r.instwishtimecall2 is null and
                                sysdate - s.date_change_state > 1 / 24 and
                                s.state_id = 17 then
                            1
                           when r.instwishtimecall2 is null and
                                sysdate - s.date_change_state > 3 and
                                s.state_id in (3, 4) then
                            1
                           when r.instwishtimecall2 is not null and
                                sysdate > r.instwishtimecall2 and
                                s.state_id in (17, 3, 4) then
                            1
                           when sysdate - s.date_change_state > 3 and
                                s.state_id in (2, 5, 6) then
                            1
                           else
                            0
                         end) overdue
                from table(l_all_req) req
                join tr_request_service s
                  on s.id = req.column_value
                join tr_request r
                  on r.id = s.request_id
               group by s.id) t
       where pi_overdue = t.overdue;
    
      l_all_req := l_all_req2;
    end if;
    --
    if pi_req_type is not null then
      l_all_req2 := null;
      if pi_req_type = 3 then
        l_filt_callback_hours := constant_pkg.c_filt_callback_hours;
      end if;
      select distinct req.column_value bulk collect
        into l_all_req2
        from table(l_all_req) req
        join tr_request_service rs
          on rs.id = req.column_value
        join tr_request r
          on r.id = rs.request_id
       where (pi_req_type = 1 and r.instwishtimecall2 < sysdate)
          or (pi_req_type = 2 and r.instwishtimecall1 < sysdate and
             r.instwishtimecall2 > sysdate)
          or (pi_req_type = 3 and r.instwishtimecall1 between sysdate and
             sysdate + l_filt_callback_hours / 24)
          or (pi_req_type = 4 and r.instwishtimecall1 > sysdate)
          or (pi_req_type = 5 and r.instwishtimecall1 is null);
      l_all_req := l_all_req2;
    end if;
    select request_Order_Type(id, rownum) bulk collect
      into l_col_request
      from (select t.id
              from (select min(case
                                 when s.state_id in (1, 2, 17) then
                                  1
                                 when s.state_id in (3, 4, 5, 6) then
                                  2
                                 when s.state_id in
                                      (7, 8, 9, 10, 11, 12, 18, 20, 21, 23) then
                                  3
                                 when s.state_id in (13, 14, 15, 16, 22) then
                                  4
                               end) req_state,
                           r.address_id,
                           r.client_id,
                           r.contact_person_id,
                           r.region_id,
                           r.date_create_fact date_create,
                           r.id,
                           r.request_id,
                           r.instwishtimecall1,
                           r.instwishtimecall2
                      from table(l_all_req) req
                      join tr_request_service s
                        on req.column_value = s.id
                      join tr_request r
                        on r.id = s.request_id
                      join t_system ss
                        on ss.id = r.system_id
                     where (pi_type_system = 2 or
                           (ss.is_federal = pi_type_system))
                       and s.state_id != 24
                       and ss.id in (select * from table(pi_systems))
                       and r.channel_id in (select * from table(pi_Channels))
                       and s.product_category in
                           (select * from table(l_service_tab))
                       and ((nvl(pi_only_mine, 0) = 1 and
                           s.worker_create = pi_worker_id) or
                           nvl(pi_only_mine, 0) <> 1)
                     group by r.address_id,
                              r.client_id,
                              r.contact_person_id,
                              r.region_id,
                              r.date_create_fact,
                              r.id,
                              r.request_id,
                              r.instwishtimecall1,
                              r.instwishtimecall2) t
              join t_clients cl
                on cl.client_id = t.client_id
               and cl.client_type = 'P'
              join t_clients p_cl
                on p_cl.client_id = t.contact_person_id
               and p_cl.client_type = 'P'
              join t_person p
                on p_cl.fullinfo_id = p.person_id
              left join t_address a
                on t.address_id = a.addr_id
              left join t_dic_region dr
                on dr.reg_id = t.region_id
              left join t_dic_mrf dm
                on dm.id = dr.mrf_id
              left join t_Organizations orgM
                on dm.org_id = orgM.Org_id
              left join t_Organizations orgF
                on dr.org_id = orgf.Org_id
             where t.req_state in (select * from table(pi_States))
             order by decode(l_order_asc,
                             9,
                             (case
                               when t.instwishtimecall1 > sysdate then
                                4
                               when sysdate between t.instwishtimecall1 and
                                    t.instwishtimecall2 then
                                2
                               when t.instwishtimecall1 is null then
                                3
                               else
                                1
                             end),
                             null) asc,
                      decode(l_order_asc,
                             null,
                             null,
                             1,
                             lpad(t.request_id, 13, '0'),
                             2,
                             to_char(t.date_create, 'yyyy.mm.dd hh24:mi:ss'),
                             3,
                             p.person_lastname || ' ' || p.person_firstname || ' ' ||
                             p.person_middlename,
                             4,
                             nvl2(a.addr_oth,
                                  a.addr_oth || ' ' || a.addr_building || '-' ||
                                  a.addr_office,
                                  substr(a.addr_city,
                                         0,
                                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                                  a.addr_street || ' ' || a.addr_building || '-' ||
                                  a.addr_office),
                             5,
                             t.req_state,
                             6,
                             upper(orgM.Org_Name || orgf.org_name),
                             /*7,
                             to_char(tt.instwishtimecall2) ||
                             to_char(tt.date_create, 'yyyy.mm.dd hh24:mi:ss'),*/
                             9,
                             case
                               when nvl(t.instwishtimecall1, sysdate) > sysdate then
                                to_char(nvl(t.instwishtimecall1, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                               else
                                to_char(nvl(t.instwishtimecall2, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                             end,
                             null) asc,
                      decode(l_order_desc,
                             9,
                             (case
                               when t.instwishtimecall1 > sysdate then
                                4
                               when sysdate between t.instwishtimecall1 and
                                    t.instwishtimecall2 then
                                2
                               when t.instwishtimecall1 is null then
                                3
                               else
                                1
                             end),
                             null) desc,
                      decode(l_order_desc,
                             null,
                             null,
                             1,
                             lpad(t.request_id, 13, '0'),
                             2,
                             to_char(t.date_create, 'yyyy.mm.dd hh24:mi:ss'),
                             3,
                             p.person_lastname || ' ' || p.person_firstname || ' ' ||
                             p.person_middlename,
                             4,
                             nvl2(a.addr_oth,
                                  a.addr_oth || ' ' || a.addr_building || '-' ||
                                  a.addr_office,
                                  substr(a.addr_city,
                                         0,
                                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                                  a.addr_street || ' ' || a.addr_building || '-' ||
                                  a.addr_office),
                             5,
                             t.req_state,
                             6,
                             upper(orgM.Org_Name || orgf.org_name),
                             /* 7,
                             to_char(tt.instwishtimecall2) ||
                             to_char(tt.date_create, 'yyyy.mm.dd hh24:mi:ss'),*/
                             9,
                             case
                               when nvl(t.instwishtimecall1, sysdate) > sysdate then
                                to_char(nvl(t.instwishtimecall1, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                               else
                                to_char(nvl(t.instwishtimecall2, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                             end,
                             null) desc);
  
    po_all_count := l_col_request.count;
  
    l_max_num_page := round(po_all_count / nvl(pi_count_req, 1));
  
    if (pi_num_page > l_max_num_page and l_max_num_page <> 0) then
      l_num_page := l_max_num_page + 1;
    else
      l_num_page := nvl(pi_num_page, 1);
    end if;
  
    open res for
      select t.id,
             t.request_id,
             t.date_create,
             t.service_tab,
             t.req_state,
             t.channel_id,
             p.person_lastname || ' ' || p.person_firstname || ' ' ||
             p.person_middlename as fio,
             nvl2(aa.addr_oth,
                  aa.addr_oth || nvl2(aa.addr_building,
                                      ', д. ' || aa.addr_building,
                                      aa.addr_building) ||
                  nvl2(aa.addr_office,
                       ', кв. ' || aa.addr_office,
                       aa.addr_office),
                  substr(aa.addr_city,
                         0,
                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                  aa.addr_street || nvl2(aa.addr_building,
                                         ', д. ' || aa.addr_building,
                                         aa.addr_building) ||
                  nvl2(aa.addr_office,
                       ', кв. ' || aa.addr_office,
                       aa.addr_office)) adr,
             nvl(orgM.Org_Name, o_rtk.org_name) mrf,
             nvl(orgf.org_name, o_rtk.org_name) filial,
             u.usr_login as create_user,
             p.person_phone,
             p.person_home_phone,
             case
               when t.date_next - sysdate < 0 then
                0
               when t.date_next - sysdate < 1 then
                1
               else
                2
             end overdue_marker,
             oo.org_name,
             p.person_phone,
             p.person_home_phone,
             t.instwishtimecall1,
             t.instwishtimecall2,
             rtr.base_request_id,
             rtr.base_org_id,
             o_rtr.org_name base_org_name,
             (select cast(collect(rtr2.request_id) as num_tab)
                from tr_request_to_request rtr2
               where rtr2.BASE_REQUEST_ID = t.id) child_request,
             u.employee_number
        from (select cast(collect(rec_num_2(number_1 => s.id,
                                            number_2 => s.product_category)) as
                          array_num_2) service_tab,
                     min(case
                           when s.state_id in (1, 2, 17) then
                            1
                           when s.state_id in (3, 4, 5, 6) then
                            2
                           when s.state_id in
                                (7, 8, 9, 10, 11, 12, 18, 20, 21, 23) then
                            3
                           when s.state_id in (13, 14, 15, 16, 22) then
                            4
                         end) req_state,
                     min(nvl(case
                               when s.state_id in (17, 3, 4) then
                                r.instwishtimecall2
                               when s.state_id in (7,
                                                   8,
                                                   9,
                                                   10,
                                                   11,
                                                   12,
                                                   13,
                                                   14,
                                                   15,
                                                   16,
                                                   18,
                                                   20,
                                                   21,
                                                   22,
                                                   23) then
                                sysdate + 3
                               else
                                null
                             end,
                             s.date_change_state +
                             decode(s.state_id, 1, 5 / 24 / 60, 17, 1, 3))) date_next,
                     
                     col_rc.rn,
                     r.id,
                     r.request_id,
                     r.date_create_fact date_create,
                     max(r.instwishtimecall1) instwishtimecall1,
                     max(r.instwishtimecall2) instwishtimecall2,
                     r.address_id,
                     r.contact_person_id,
                     r.client_id,
                     r.worker_create,
                     r.region_id,
                     r.channel_id,
                     r.org_id
                from (Select request_id, rn
                        from table(l_col_request)
                       where rn between (l_num_page - 1) * pi_count_req + 1 and
                             (l_num_page) * pi_count_req) col_rc
                join tr_request_service s
                  on s.request_id = col_rc.request_id
                 and s.state_id != 24
                join tr_request r
                  on s.request_id = r.id
               group by col_rc.rn,
                        r.id,
                        r.request_id,
                        r.date_create_fact,
                        r.address_id,
                        r.contact_person_id,
                        r.client_id,
                        r.worker_create,
                        r.region_id,
                        r.channel_id,
                        r.org_id) t
      
        join t_clients cl
          on cl.client_id = t.client_id
         and cl.client_type = 'P'
        join t_clients p_cl
          on p_cl.client_id = t.contact_person_id
         and p_cl.client_type = 'P'
        join t_person p
          on p.person_id = p_cl.fullinfo_id
        join t_users u
          on u.usr_id = t.worker_create
        join t_address aa
          on t.address_id = aa.addr_id
        left join t_dic_region dr
          on dr.reg_id = t.region_id
        left join t_dic_mrf dm
          on dm.id = dr.mrf_id
        left join t_Organizations orgM
          on dm.org_id = orgM.Org_id
        left join t_Organizations orgF
          on dr.org_id = orgf.Org_id
        left join t_organizations o_rtk
          on o_rtk.org_id = 0
        join t_organizations oo
          on oo.org_id = t.org_id
        left join tr_request_to_request rtr
          on rtr.request_id = t.id
        left join t_organizations o_rtr
          on o_rtr.org_id = rtr.base_org_id
       order by rn;
  
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;
  --------------------------------------------------------------
  --Расширенная выгрузка услуг в заявках ФЛ
  --%param pi_worker_id  Идентификатор пользователя                                              
  --%param pi_contractNum Номер договора
  --%param pi_request_id Номер заявки
  --%param pi_mrf_order_num Номер заявки в ИС МРФ
  --%param pi_only_mine Показывать только мои заявки (признак)
  --%param pi_FirstName Имя клиента
  --%param pi_LastName Фамилия клиента
  --%param pi_MiddleName Отчество клиента
  --%param pi_IDCardSeria Серия документа
  --%param pi_IDCardNumber Номер документа
  --%param pi_ELK_ACCOUNT Логин ЕЛК
  --%param pi_PERSONAL_ACCOUNT Лицевой счет
  --%param pi_phone Номер телефона (контактного)
  --%param pi_uslNumber Номер устройства/телефона
  --%param pi_mainEquipment Номер основного устройства/порта
  --%param pi_addr_id Локальный идентификатор улицы
  --%param pi_house_id Локальный идентификатор дома
  --%param pi_house_num Номер дома
  --%param pi_flat_id Номер квартиры
  --%param pi_services Категория услуги
  --%param pi_org_tab Подразделение
  --%param pi_org_child_include учитывать подчиненные
  --%param pi_type_request Тип заявки(метка)
  --        {*} null - все
  --        {*} '0' - без меток
  --        {*} 'connect_friend' - Подключи друга
  --        {*} 'anketa_mpz' - Анкетирование МПЗ
  --        {*} 'telemarketing' - Телемаркетинг
  --        {*} 'callback_request' - Обратный звонок
  --        {*} 'sms_request' - Заявка по sms
  --        {*} 'is_quick_request' - Быстрая заявка    
  --        {*} 'address_is_not_directory' - адрес не из справочника      
  --        {*} 'elk_request' - елк заявка
  --%param pi_addr_label Топ городов
  --%param pi_type_operation Тип операции: установка/смена технологии
  -- {*} 0 - установка
  -- {*} 2 - смена технологияя
  --%param pi_DateTimeBeg Дата начала периода
  --%param pi_DateTimeEnd Дата окончания периода
  --%param pi_sorting_date Тип периода
  -- {*} 0 - Фактическая дата приема заявки в ИС
  -- {*} 1 - Период последнего изменения
  -- {*} 2 - Период поступления в текущую фазу
  -- {*} 3 - Период приема заявки в ЕИССД/МПЗ
  --%param pi_device_id Оборудование в заявке
  --%param pi_Channels Список каналов поступления заявки
  --%param pi_States Список состояний заявки
  --%param pi_systems Список систем поступления заявки
  --%param pi_type_system Тип системы поступления заявки
  -- {*} 0 - региональна 
  -- {*} 1 -федеральная 
  -- {*} 2 - все    
  --%param pi_tech_posib Результат проверки ТхВ
  --        {*} null - любая
  --        {*} 1 Техническая возможность есть
  --        {*} 2 Технической возможности нет
  --        {*} 3 по адресу/телефону есть услуги того же типа
  --        {*} 4 проверка не проводилась  
  --        {*} 5 Техническая возможность неизвестна (по = U)
  --%param pi_send Обработка заявок сторонними системами
  --        {*} null - все
  --        {*} 'export_to_mrf'  Не отправлена в ИС МРФ
  --%param pi_tag_id Тип тарифного плана
  --%param pi_opt_tag Тип опции
  --%param pi_req_type Тип фильтра «По дате и времени для обратного звонка:» 
  --        {*} null - все
  --        {*} 1 «просроченные заявки» - на момент времени форм отчета дата и время окончания периода звонка уже наступили
  --        {*} 2 «текущие заявки» - на момент времени форм отчета дата и время начала периода звонка уже наступили, а время окончания периода звонка еще не наступило
  --        {*} 3 «время для звонка наступит в течение 8 час.» - на момент времени форм отчета дата и время начала периода для звонка должны наступить в течение 8 часов
  --        {*} 4 «будущие заявки» - на момент времени форм отчета дата начала периода для звонка уже наступила, а время начала периода звонка еще не наступило, или дата и время начала периода звонка еще не наступили  
  --        {*} 5 «заявки без даты обратного звонка» - заявки, у которых дата и время звонка не указаны
  --%param pi_num_page номер страницы                                                                                  
  --%param pi_count_req кол-во записей на странице
  --%param pi_column Номер колонки для сортировки
  --%param pi_sorting Тип сортировки: 0-по возрастанияю, 1-по убыванию
  --%param po_all_count Общее кол-во заявок
  --%param po_err_num Код ошибки
  --%param po_err_msg сообщение об ошибке
  --------------------------------------------------------------
  function get_list_phys_service_more(pi_worker_id        in number,
                                      pi_contractNum      in varchar2,
                                      pi_request_id       in tr_request.request_id%type,
                                      pi_mrf_order_num    in varchar2,
                                      pi_only_mine        in number,
                                      pi_FirstName        in t_person.person_firstname%type,
                                      pi_LastName         in t_person.person_lastname%type,
                                      pi_MiddleName       in t_person.person_middlename%type,
                                      pi_IDCardSeria      in t_documents.doc_series%type,
                                      pi_IDCardNumber     in t_documents.doc_number%type,
                                      pi_ELK_ACCOUNT      in varchar2,
                                      pi_PERSONAL_ACCOUNT in varchar2,
                                      pi_phone            in t_person.person_phone%type,
                                      pi_uslNumber        in tr_request_service_detail.usl_number%type,
                                      pi_mainEquipment    in tr_request_service_detail.main_equipment%type,
                                      ------------
                                      pi_addr_id   in number,
                                      pi_house_id  in number,
                                      pi_house_num in varchar2,
                                      pi_flat_id   in varchar2,
                                      ------------
                                      pi_services          in num_tab,
                                      pi_org_tab           in num_tab,
                                      pi_org_child_include in number,
                                      pi_type_request      in varchar2,
                                      pi_addr_label        in number,
                                      pi_type_operation    in num_tab,
                                      pi_DateTimeBeg       in date,
                                      pi_DateTimeEnd       in date,
                                      pi_sorting_date      in number,
                                      pi_device_id         in number,
                                      pi_device_card_type  in number, -- тип оборудования из Лиры
                                      pi_Channels          in num_tab,
                                      pi_States            in num_tab,
                                      pi_systems           in num_tab,
                                      pi_type_system       in number,
                                      pi_tech_posib        in number,
                                      pi_send              in request_param_type,
                                      pi_tag_id            in number,
                                      pi_opt_tag           in number,
                                      pi_req_type          in number,
                                      ---------
                                      pi_num_page  in number,
                                      pi_count_req in number,
                                      pi_column    in number,
                                      pi_sorting   in number,
                                      po_all_count out number,
                                      po_err_num   out number,
                                      po_err_msg   out varchar2)
    return sys_refcursor is
    l_func_name varchar2(100) := 'request_list.get_list_phys_service_more';
  
    res            sys_refcursor;
    l_order_asc    number; -- по возрастанияю
    l_order_desc   number; -- по убыванию
    l_max_num_page number;
    l_num_page     number;
  
    l_org_num_2  array_num_2 := array_num_2();
    l_org_tab    num_tab;
    User_Orgs    Num_Tab;
    User_Orgs2   Num_Tab;
    l_reg_org    ARRAY_NUM_2;
    User_reg_org ARRAY_NUM_2;
  
    l_callback_view number := 0;
    l_sms_view      number := 0;
    l_date_cr_beg   date;
    l_date_cr_end   date;
    l_date_ch_beg   date;
    l_date_ch_end   date;
    l_tag_tab       num_tab;
    l_service_tab   num_tab := pi_services;
  
    l_cnt number;
  
    l_all_req1  num_tab; -- по датам
    l_all_req2  num_tab;
    l_all_req3  num_tab;
    l_all_req4  num_tab;
    l_all_req5  num_tab;
    l_all_req6  num_tab;
    l_all_req7  num_tab;
    l_all_req8  num_tab;
    l_all_req9  num_tab;
    l_all_req10 num_tab;
  
    l_all_req             num_tab;
    l_col_request         request_Order_Tab;
    l_request_id          number;
    l_filt_callback_hours number;
    l_request_devices array_num2_str;
  begin
    if pi_org_tab is not null then
      select rec_num_2(number_1 => column_value,
                       number_2 => pi_org_child_include) bulk collect
        into l_org_num_2
        from table(pi_org_tab);
    else
      l_org_num_2 := array_num_2(rec_num_2(1, nvl(pi_org_child_include, 1)));
    end if;
  
    l_org_tab := get_orgs_tab_for_multiset(pi_orgs            => l_org_num_2,
                                           Pi_worker_id       => pi_worker_id,
                                           pi_block           => 1,
                                           pi_org_relation    => null,
                                           pi_is_rtmob        => 0,
                                           pi_tm_1009_include => 1);
  
    User_Orgs := SECURITY_PKG.Get_User_Orgs_Tab_By_Right_str(pi_worker_id,
                                                             'SD.REQUEST.PHYS.VIEW_LIST',
                                                             null);
  
    User_Orgs2 := intersects(l_org_tab, User_Orgs);
  
    l_reg_org := SECURITY_PKG.get_region_by_worker_right2(pi_worker_id => pi_worker_id,
                                                          pi_right_str => string_tab('SD.REQUEST.PHYS.VIEW_LIST'),
                                                          pi_org_id    => l_org_tab);
  
    User_reg_org := intersect_num2(User_Orgs2, l_reg_org);
  
    If pi_sorting = 0 then
      l_order_asc := NVL(pi_column, 2);
    else
      l_order_desc := NVL(pi_column, 2);
    end If;
  
    if pi_DateTimeBeg is not null and nvl(pi_sorting_date, 0) in (0, 3) then
      l_date_cr_beg := pi_DateTimeBeg - Constant_pkg.c_GMT;
    else
      l_date_cr_beg := to_date('01.01.1900', 'dd.mm.yyyy');
    end if;
  
    if pi_DateTimeEnd is not null and nvl(pi_sorting_date, 0) in (0, 3) then
      l_date_cr_end := pi_DateTimeEnd - Constant_pkg.c_GMT;
    else
      l_date_cr_end := to_date('31.12.2999', 'dd.mm.yyyy');
    end if;
  
    if pi_DateTimeBeg is not null and nvl(pi_sorting_date, 0) in (1, 2) then
      l_date_ch_beg := pi_DateTimeBeg - Constant_pkg.c_GMT;
    else
      l_date_ch_beg := to_date('01.01.1900', 'dd.mm.yyyy');
    end if;
  
    if pi_DateTimeEnd is not null and nvl(pi_sorting_date, 0) in (1, 2) then
      l_date_ch_end := pi_DateTimeEnd - Constant_pkg.c_GMT;
    else
      l_date_ch_end := to_date('31.12.2999', 'dd.mm.yyyy');
    end if;
  
    if (pi_DateTimeBeg is not null or pi_DateTimeEnd is not null) then
      if pi_sorting_date = 0 then
        select /*+ index(p IDX_REQUEST_SERVICE_DATCR_F) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_create_fact >= l_date_cr_beg
           and p.date_create_fact < l_date_cr_end;
      
      elsif pi_sorting_date = 1 then
        select /*+ index(p IDX_REQUEST_SERVICE_DATCH_F) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_change_fact >= l_date_ch_beg
           and p.date_change_fact < l_date_ch_end;
      
      elsif pi_sorting_date = 2 then
        --по дате перехода в текущее состояние
        select /*+ index(p IDX_TR_PRODUCT_DCHST) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_change_state >= l_date_ch_beg
           and p.date_change_state < l_date_ch_end;
      elsif pi_sorting_date = 3 then
        select /*+ index(p IDX_TR_PRODUCT_DCR) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_create >= l_date_cr_beg
           and p.date_create < l_date_cr_end;
      end if;
    end if;
  
    select count(*)
      into l_cnt
      from table(pi_States) s
     where s.column_value in (17, 18, 20, 21, 22, 23);
  
    --определим показывать коллбак или нет
    -- состояние колбек или метка обратный звонок
    if l_cnt > 0 or nvl(pi_type_request, '0') = 'callback_request' then
      l_callback_view := 1;
    end if;
  
    if l_cnt > 0 or nvl(pi_type_request, '0') = 'sms_request' then
      l_sms_view := 1;
      l_service_tab.extend;
      l_service_tab(l_service_tab.count) := 13;
    end if;
  
    --Топ городов
    if pi_addr_label is not null then
      select distinct p.id bulk collect
        into l_all_req9
        from table(l_all_req1) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request r
          on r.id = p.request_id
        join t_address a
          on a.addr_id = r.address_id
        join (select ao.id
                from t_address_object ao
              connect by prior ao.id = ao.parent_id
               start with ao.id in
                          (select al.local_id
                             from t_addr_label al
                            where al.lable_id = pi_addr_label)) aa
          on aa.id = a.addr_obj_id;
      l_all_req1 := l_all_req9;
    end if;
  
    if (pi_FirstName is not null or pi_LastName is not null or
       pi_MiddleName is not null or pi_phone is not null) then
      select id bulk collect
        into l_all_req2
        from (select t.id
                from (select p.id,
                             pp.person_firstname,
                             pp.person_lastname,
                             pp.person_middlename,
                             pp.person_phone,
                             pp.person_home_phone
                        from tr_request r
                        join tr_request_service p
                          on r.id = p.request_id
                        join t_clients cl
                          on cl.client_id = r.client_id
                         and cl.client_type = 'P'
                        join t_clients p_cl
                          on p_cl.client_id = r.contact_person_id
                         and p_cl.client_type = 'P'
                        join t_person pp
                          on p_cl.fullinfo_id = pp.person_id
                       where lower(pp.person_firstname) =
                             lower(trim(pi_FirstName))
                          or lower(pp.person_lastname) =
                             lower(trim(pi_LastName))
                          or lower(pp.person_middlename) =
                             lower(trim(pi_MiddleName))
                          or pp.person_phone = pi_phone
                          or pp.person_home_phone = pi_phone) t
               where (pi_FirstName is null or
                     lower(t.person_firstname) = lower(trim(pi_FirstName)))
                 and (pi_LastName is null or
                     lower(t.person_lastname) = lower(trim(pi_LastName)))
                 and (pi_MiddleName is null or lower(t.person_middlename) =
                     lower(trim(pi_MiddleName)))
                 and (pi_phone is null or t.person_phone = pi_phone or
                     t.person_home_phone = pi_phone)
              union
              select t.id
                from (select p.id,
                             pp.person_firstname,
                             pp.person_lastname,
                             pp.person_middlename,
                             pp.person_phone,
                             pp.person_home_phone
                        from tr_request r
                        join tr_request_service p
                          on r.id = p.request_id
                        join t_clients cl
                          on cl.client_id = r.client_id
                         and cl.client_type = 'P'
                        join t_person pp
                          on cl.fullinfo_id = pp.person_id
                       where lower(pp.person_firstname) =
                             lower(trim(pi_FirstName))
                          or lower(pp.person_lastname) =
                             lower(trim(pi_LastName))
                          or lower(pp.person_middlename) =
                             lower(trim(pi_MiddleName))) t
               where (pi_FirstName is null or
                     lower(t.person_firstname) = lower(trim(pi_FirstName)))
                 and (pi_LastName is null or
                     lower(t.person_lastname) = lower(trim(pi_LastName)))
                 and (pi_MiddleName is null or lower(t.person_middlename) =
                     lower(trim(pi_MiddleName))));
    end if;
  
    if pi_request_id is not null then
      begin
        l_request_id := to_number(pi_request_id);
        select s.id bulk collect
          into l_all_req10
          from tr_request_service s
          join tr_request r
            on r.id = s.request_id
         where r.id = l_request_id
            or s.id = l_request_id;
      exception
        when others then
          select s.id bulk collect
            into l_all_req10
            from tr_request_service s
            join tr_request r
              on r.id = s.request_id
           where r.request_id = pi_request_id;
      end;
    end if;
  
    if (pi_contractNum is not null or pi_mrf_order_num is not null or
       pi_ELK_ACCOUNT is not null or pi_PERSONAL_ACCOUNT is not null) then
      select t.id bulk collect
        into l_all_req3
        from (select p.id,
                     r.request_id,
                     r.dogovor_number,
                     p.mrf_order_num,
                     r.elk_account,
                     r.personal_account
                from tr_request_service p
                join tr_request r
                  on r.id = p.request_id
               where r.dogovor_number = pi_contractNum
                  or p.mrf_order_num = pi_mrf_order_num
                  or r.elk_account = pi_ELK_ACCOUNT
                  or r.personal_account = pi_PERSONAL_ACCOUNT) t
       where (t.dogovor_number = pi_contractNum or pi_contractNum is null)
         and (t.mrf_order_num = pi_mrf_order_num or
             pi_mrf_order_num is null)
         and (t.elk_account = pi_ELK_ACCOUNT or pi_ELK_ACCOUNT is null)
         and (t.personal_account = pi_PERSONAL_ACCOUNT or
             pi_PERSONAL_ACCOUNT is null);
    end if;
  
    if (pi_IDCardSeria is not null or pi_IDCardNumber is not null) then
      select p.id bulk collect
        into l_all_req5
        from tr_request r
        join tr_request_service p
          on r.id = p.request_id
        join t_system s
          on r.system_id = s.id
        join t_clients cl
          on cl.client_id = r.client_id
         and cl.client_type = 'P'
        join t_person pp
          on cl.fullinfo_id = pp.person_id
        join t_documents d
          on d.doc_id = pp.doc_id
       where d.doc_series = upper(trim(pi_IDCardSeria))
         and d.doc_number = upper(trim(pi_IDCardNumber));
    end if;
  
    if (pi_addr_id is not null and
       (pi_house_id is not null or pi_house_num is not null)) then
      select p.id bulk collect
        into l_all_req4
        from (select r.id
                from tr_request r
                join t_address a
                  on r.address_id = a.addr_id
               where a.addr_obj_id = pi_addr_id
                 and (pi_house_id is null or
                     pi_house_id = a.addr_house_obj_id)
                 and (pi_house_num is null or
                     trim(pi_house_num) = a.addr_building)
                 and (pi_flat_id is null or trim(pi_flat_id) = a.addr_office)) t
        join tr_request_service p
          on p.request_id = t.id;
    end if;
  
    -- фильтр по меткам    
    if nvl(pi_type_request, '0') != '0' then
      select distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req1) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_params par
          on par.request_id = p.request_id
         and par.key = pi_type_request
         and nvl(par.value, '0') = '1'
       where l_callback_view = 1
          or l_sms_view = 1
          or p.state_id not in (17, 18, 20, 21, 22, 23);
    elsif pi_type_request is null and l_all_req1 is not null then
      --нам без разницы какие метки, главное убрать колбак, если он не нужен
      if l_callback_view = 1 and l_sms_view = 1 then
        l_all_req8 := l_all_req1;
      elsif l_sms_view = 1 and l_callback_view = 0 then
        --исключим колбек в статусах колбек
        select distinct p.id bulk collect
          into l_all_req8
          from table(l_all_req1) req
          join tr_request_service p
            on p.id = req.column_value
          left join tr_request_params par
            on par.request_id = p.request_id
           and par.key = 'callback_request'
           and p.state_id in (17, 18, 20, 21, 22, 23)
         where nvl(par.value, 0) = 0;
      elsif l_sms_view = 0 and l_callback_view = 1 then
        --исключим смс в статусах колбек
        select distinct p.id bulk collect
          into l_all_req8
          from table(l_all_req1) req
          join tr_request_service p
            on p.id = req.column_value
          left join tr_request_params par
            on par.request_id = p.request_id
           and par.key = 'sms_request'
           and p.state_id in (17, 18, 20, 21, 22, 23)
         where nvl(par.value, 0) = 0;
      else
        select /*+ use_nl(rc req) */
        distinct p.id bulk collect
          into l_all_req8
          from table(l_all_req1) req
          join tr_request_service p
            on p.id = req.column_value
         where p.state_id not in (17, 18, 20, 21, 22, 23);
      end if;
    elsif pi_type_request = '0' then
      select distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req1) req
        join tr_request_service p
          on p.id = req.column_value
        left join tr_request_params par
          on par.request_id = p.request_id
         and nvl(par.value, '0') = '1'
         and par.key in ('connect_friend',
                         'anketa_mpz',
                         'telemarketing',
                         'callback_request',
                         'sms_request',
                         'is_quick_request',
                         'elk_request',
                         'address_is_not_directory')
       where (l_callback_view = 1 or
             l_sms_view = 1 and p.state_id not in (17, 18, 20, 21, 22, 23))
         and nvl(par.value, '0') = '0';
    end if;
  
    --Фильтр по уже подключенным услугам
    if nvl(pi_tech_posib, 0) = 3 then
      select distinct s.id bulk collect
        into l_all_req7
        from table(l_all_req8) req
        join tr_request_service s
          on req.column_value = s.id
        join TR_REQUEST_SERV_EXIST e
          on e.request_id = s.request_id
         and e.service_type = s.product_category
       where nvl(e.is_old_address,0)=0  ;
    end if;
  
    --Фильтр по проверке ТВ
    if nvl(pi_tech_posib, 0) in (1, 2, 4, 5) then
      select distinct aa.id bulk collect
        into l_all_req7
        from (select /* ordered use_nl(req rc) use_nl(rc th) */
               p.id, max(th.is_success) tech_posib
                from table(l_all_req8) req
                join tr_request_service p
                  on req.column_value = p.id
                join tr_request r
                  on r.id = p.request_id
                left join tr_request_tech_poss th
                  on th.request_id = p.request_id
                 and th.service_type = p.product_category
                 and th.address_id = r.address_id
                 and nvl(th.tech_id, 0) = nvl(p.tech_id, nvl(th.tech_id, 0))
               group by p.id) aa
       where nvl(aa.tech_posib, -1) =
             decode(pi_tech_posib, 1, 1, 2, 0, 4, -1, 5, 5);
    end if;
  
    if pi_uslNumber is not null or pi_mainEquipment is not null then
      select distinct s.service_id bulk collect
        into l_all_req6
        from tr_request_service_detail s
       where (s.main_equipment = pi_mainEquipment or
             pi_mainEquipment is null)
         and (s.usl_number = pi_uslNumber or pi_uslNumber is null);
    end if;
  
    if l_all_req7 is null then
      l_all_req := intersects(l_all_req, l_all_req8);
    else
      l_all_req := intersects(l_all_req, l_all_req7);
    end if;
    l_all_req := intersects(l_all_req, l_all_req2);
    l_all_req := intersects(l_all_req, l_all_req3);
    l_all_req := intersects(l_all_req, l_all_req10);
    l_all_req := intersects(l_all_req, l_all_req4);
    l_all_req := intersects(l_all_req, l_all_req5);
    l_all_req := intersects(l_all_req, l_all_req6);
  
    if pi_send is not null and pi_send.key is not null then
      --Обработка заявок сторонними системами
      l_all_req8 := null;
      select distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_params par
          on par.request_id = p.request_id
         and par.key = pi_send.key
       where par.value = pi_send.value;
    
      l_all_req  := l_all_req8;
      l_all_req8 := null;
    end if;
  
    --фильтр по тегам опций
    if pi_opt_tag is not null then
      l_all_req8 := null;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_product_option t
          on t.service_id = req.column_value
        join t_opt_tag opt
          on opt.option_id = t.option_id
       where opt.tag_id = pi_opt_tag;
    
      l_all_req  := l_all_req8;
      l_all_req8 := null;
    end if;
  
    -- фильтр по оборудованию из ЕИССД
    if pi_device_id is not null then
      l_all_req8 := null;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_device t
          on t.request_id = p.request_id
       where t.device_type = pi_device_id
         and not exists (select ord.orders_id
                from tr_request_orders ord
                join t_orders o
                  on ord.orders_id = o.id
               where o.order_type = 5
                 and ord.request_id = p.request_id);               
      l_all_req := l_all_req8;
      l_all_req8 := null;
    end if;  
    -- по типам оборудование из Лиры
    if pi_device_card_type is not null then      
      l_all_req8 := null;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_orders ro
          on ro.request_id = p.request_id
        join t_orders o
          on o.id = ro.orders_id
         and o.order_type = 5
        join tr_request_device_card rdc
          on rdc.request_id = p.request_id
       where rdc.type_equipment = pi_device_card_type
         and rdc.is_exists = 0;
               
      l_all_req := l_all_req8;
      l_all_req8 := null;
    end if;
  
    if pi_type_request is null and l_callback_view = 0 and l_sms_view = 0 and
       l_all_req1 is null then
      --метки не были переданы, значит  по параметрам клиента. 
      --Нужно убрать колбак статусы, раз они не нужен           
    
      select /*+ use_nl(rc req) */
      distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
       where p.state_id not in (17, 18, 20, 21, 22, 23);
    
      l_all_req := l_all_req8;
    end if;
  
    --подберем тарифы по тегам
    if pi_tag_id is not null then
      select t.id bulk collect
        into l_tag_tab
        from t_dic_tag t
      connect by t.parent_id = prior t.id
       start with t.id = pi_tag_id;
    
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_service_product ps
          on ps.service_id = req.column_value
        join tr_request_product p
          on p.id = ps.product_id
        join t_version_tag vt
          on vt.object_id = p.tar_id
       where vt.tag_id in (select column_value from table(l_tag_tab));
    
      l_all_req := l_all_req8;
    end if;
    --
    if pi_req_type is not null then
      l_all_req8 := null;
      if pi_req_type = 3 then
        l_filt_callback_hours := constant_pkg.c_filt_callback_hours;
      end if;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service rs
          on rs.id = req.column_value
        join tr_request r
          on r.id = rs.request_id
       where (pi_req_type = 1 and r.instwishtimecall2 < sysdate)
          or (pi_req_type = 2 and r.instwishtimecall1 < sysdate and
             r.instwishtimecall2 > sysdate)
          or (pi_req_type = 3 and r.instwishtimecall1 between sysdate and
             sysdate + l_filt_callback_hours / 24)
          or (pi_req_type = 4 and r.instwishtimecall1 > sysdate)
          or (pi_req_type = 5 and r.instwishtimecall1 is null);
      l_all_req := l_all_req8;
    end if;
  
    --/*+ ordered */
    select request_Order_Type(id, rownum) bulk collect
      into l_col_request
      from (select /*+ ordered */
             p.id
              from (select /*+ use_nl(p xxx) */
                     p.*
                      from table(l_all_req) xxx
                      join tr_request_service p
                        on p.id = xxx.column_value) p
              join tr_request r
                on r.id = p.request_id
              join table(User_reg_org) reg_org
                on reg_org.number_1 = r.org_id
               and nvl(r.region_id, '-1') =
                   nvl(reg_org.number_2, nvl(r.region_id, -1))
              join t_system s
                on r.system_id = s.id
              join table(pi_systems) ss
                on ss.column_value = r.system_id
              join Table(pi_Channels) ch
                on ch.column_value = r.channel_id
              join table(l_service_tab) serv
                on serv.column_value = p.product_category
              join t_clients cl
                on cl.client_id = r.client_id
               and cl.client_type = 'P'
              join t_clients p_cl
                on p_cl.client_id = r.contact_person_id
               and p_cl.client_type = 'P'
              join t_person pp
                on p_cl.fullinfo_id = pp.person_id
              left join t_address a
                on r.address_id = a.addr_id
              left join t_dic_region dr
                on dr.reg_id = r.region_id
              left join t_dic_mrf dm
                on dm.id = dr.mrf_id
              left join t_Organizations orgM
                on dm.org_id = orgM.Org_id
              left join t_Organizations orgF
                on dr.org_id = orgf.Org_id
              join t_dic_request_state drs
                on p.state_id = drs.state_id
              join table(pi_States) stt
                on stt.column_value = drs.state_id
             where (pi_type_system = 2 or (s.is_federal = pi_type_system))
               and ((nvl(pi_only_mine, 0) = 1 and
                   p.worker_create = pi_worker_id) or
                   nvl(pi_only_mine, 0) <> 1)
                  --and (r.system_id in (select * from table(pi_systems)))
               and p.state_id not in (24)
               and p.type_request in
                   (select * from table(pi_type_operation))
             order by decode(l_order_asc,
                             9,
                             (case
                               when r.instwishtimecall1 > sysdate then
                                4
                               when sysdate between r.instwishtimecall1 and
                                    r.instwishtimecall2 then
                                2
                               when r.instwishtimecall1 is null then
                                3
                               else
                                1
                             end),
                             null) asc,
                      decode(l_order_asc,
                             null,
                             null,
                             1,
                             lpad(r.request_id, 13, '0'),
                             2,
                             to_char(p.date_create_fact,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             3,
                             lower(pp.person_lastname || ' ' ||
                                   pp.person_firstname || ' ' ||
                                   pp.person_middlename),
                             4,
                             nvl2(a.addr_oth,
                                  a.addr_oth || ' ' || a.addr_building || '-' ||
                                  a.addr_office,
                                  substr(a.addr_city,
                                         0,
                                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                                  a.addr_street || ' ' || a.addr_building || '-' ||
                                  a.addr_office),
                             5,
                             drs.state_name,
                             6,
                             to_char(p.date_change_state,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             8,
                             upper(orgM.Org_Name || orgf.org_name),
                             9,
                             case
                               when nvl(r.instwishtimecall1, sysdate) > sysdate then
                                to_char(nvl(r.instwishtimecall1, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                               else
                                to_char(nvl(r.instwishtimecall2, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                             end,
                             null) asc,
                      decode(l_order_desc,
                             9,
                             (case
                               when r.instwishtimecall1 > sysdate then
                                4
                               when sysdate between r.instwishtimecall1 and
                                    r.instwishtimecall2 then
                                2
                               when r.instwishtimecall1 is null then
                                3
                               else
                                1
                             end),
                             null) desc,
                      decode(l_order_desc,
                             null,
                             null,
                             1,
                             lpad(r.request_id, 13, '0'),
                             2,
                             to_char(p.date_create_fact,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             3,
                             lower(pp.person_lastname || ' ' ||
                                   pp.person_firstname || ' ' ||
                                   pp.person_middlename),
                             4,
                             nvl2(a.addr_oth,
                                  a.addr_oth || ' ' || a.addr_building || '-' ||
                                  a.addr_office,
                                  substr(a.addr_city,
                                         0,
                                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                                  a.addr_street || ' ' || a.addr_building || '-' ||
                                  a.addr_office),
                             5,
                             drs.state_name,
                             6,
                             to_char(p.date_change_state,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             8,
                             upper(orgM.Org_Name || orgf.org_name),
                             9,
                             case
                               when nvl(r.instwishtimecall1, sysdate) > sysdate then
                                to_char(nvl(r.instwishtimecall1, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                               else
                                to_char(nvl(r.instwishtimecall2, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                             end,
                             null) desc);
  
    po_all_count := l_col_request.count;
  
    l_max_num_page := round(po_all_count / nvl(pi_count_req, 1));
  
    if (pi_num_page > l_max_num_page and l_max_num_page <> 0) then
      l_num_page := l_max_num_page + 1;
    else
      l_num_page := nvl(pi_num_page, 1);
    end if;
    
    -- оборудование по заявке
    select rec_num2_str(tab_dev.request_id,
                        tab_dev.service_id,
                        tab_dev.device) bulk collect
      into l_request_devices
      from (select tab.request_id,
                   tab.service_id,
                   listagg(nvl2(tab.device_name,
                                tab.device_name || ' ' || device_type_op ||
                                ' - ' || device_cnt || 'шт.',
                                null),
                           ', ') WITHIN GROUP(order by 1) device
              from (select s.request_id,
                           s.id service_id,
                           nvl2(is_ord.cnt_orders, rdc.type_equipment, d.id) as device_id,
                           nvl2(is_ord.cnt_orders, l_eq.name, d.name) as device_name,
                           nvl2(is_ord.cnt_orders, l_top.name, ss.name) as device_type_op,
                           --nvl2(is_ord.cnt_orders, nvl(rdc.quant, 1), rd.device_count)
                           sum(nvl2(is_ord.cnt_orders,
                                    nvl(rdc.quant, 1),
                                    rd.device_count)) as device_cnt
                      from (Select request_id, rn
                              from table(l_col_request)
                             where rn between
                                   (l_num_page - 1) * pi_count_req + 1 and
                                   (l_num_page) * pi_count_req) tt
                      join tr_request_service s
                        on s.id = tt.request_id
                    -- для карточки оборудования  
                    -- наличие наряда на подключение по заявке
                      left join (select ord.request_id,
                                       count(ord.orders_id) cnt_orders
                                  from tr_request_orders ord
                                  join t_orders o
                                    on ord.orders_id = o.id
                                 where o.order_type = 5
                                 group by ord.request_id) is_ord
                        on s.request_id = is_ord.request_id
                    -- для группировки карточек оборудования по услугам  
                      left join (select ord.request_id,
                                       ord.service_id,
                                       ord.orders_id
                                  from tr_request_orders ord
                                  join t_orders o
                                    on ord.orders_id = o.id
                                 where o.order_type = 5) group_serv
                        on s.request_id = group_serv.request_id
                       and s.id = group_serv.service_id
                      left join tr_request_device_card rdc
                        on rdc.request_id = s.request_id
                       and group_serv.orders_id = rdc.order_id
                       and rdc.is_exists = 0
                      left join t_lira_type_operations l_top
                        on l_top.id = rdc.op_type_id
                      left join t_lira_type_equipment l_eq
                        on l_eq.id = rdc.type_equipment
                    -- end для карточки оборудования
                      left join tr_request_device rd
                        on rd.request_id = s.request_id
                       and is_ord.cnt_orders is null
                      left join t_device d
                        on d.id = rd.device_type
                      left join t_eqipment_sale_schema ss
                        on ss.id = rd.device_use_scheme
                     group by s.request_id,
                              s.id,
                              nvl2(is_ord.cnt_orders, rdc.type_equipment, d.id),
                              nvl2(is_ord.cnt_orders, l_eq.name, d.name),
                              nvl2(is_ord.cnt_orders, l_top.name, ss.name)) tab
             group by tab.request_id, tab.service_id) tab_dev;
               
    open res for
      select rr.id,
             rr.request_id,
             bb.date_create,
             bb.service_id,
             bb.tech_id,
             bb.rn,
             bb.comments,
             bb.date_change_state,
             bb.worker_create,
             bb.tar_name,
             bb.tag_name,
             bb.product_category,
             bb.packet_channel,
             bb.opt,
             bb.device,
             bb.state_id,
             drs.state_name state,
             pp.person_lastname || ' ' || pp.person_firstname || ' ' ||
             pp.person_middlename as fio,
             pp.person_phone,
             pp.person_home_phone,
             pp.person_email,
             nvl2(aa.addr_oth,
                  aa.addr_oth || nvl2(aa.addr_building,
                                      ', д. ' || aa.addr_building,
                                      aa.addr_building) ||
                  nvl2(aa.addr_office,
                       ', кв. ' || aa.addr_office,
                       aa.addr_office),
                  substr(aa.addr_city,
                         0,
                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                  aa.addr_street || nvl2(aa.addr_building,
                                         ', д. ' || aa.addr_building,
                                         aa.addr_building) ||
                  nvl2(aa.addr_office,
                       ', кв. ' || aa.addr_office,
                       aa.addr_office)) adr,
             u.usr_login as create_user,
             rr.channel_id,
             nvl(orgM.Org_Name, o_rtk.org_name) mrf,
             nvl(orgf.org_name, o_rtk.org_name) filial,
             bb.mrf_order_num,
             oo.org_name,
             rr.instwishtimecall1,
             rr.instwishtimecall2,
             rtr.base_request_id,
             rtr.base_org_id,
             o_rtr.org_name base_org_name,
             (select cast(collect(rtr2.request_id) as num_tab)
                from tr_request_to_request rtr2
               where rtr2.BASE_REQUEST_ID = rr.id) child_request,
             u.employee_number
        from (select aa.service_id,
                     aa.tech_id,
                     aa.request_id,
                     aa.rn,
                     aa.comments,
                     aa.date_change_state,
                     aa.worker_create,
                     aa.tar_name,
                     aa.tag_name,
                     aa.product_category,
                     aa.packet_channel,
                     aa.opt,
                     aa.state_id,
                     aa.date_create,
                     aa.mrf_order_num,
                     rd.str device

                from (select tar.service_id,
                             tar.tech_id,
                             tar.request_id,
                             tar.rn,
                             tar.comments,
                             tar.date_change_state,
                             tar.worker_create,
                             tar.tar_name,
                             tar.tag_name,
                             tar.product_category,
                             tar.state_id,
                             tar.date_create,
                             tar.mrf_order_num,
                             case
                               when tar.product_category = 5 then
                                listagg(case
                                          when tar_opt.id in
                                               (select opt.option_id
                                                  from t_dic_tag t
                                                  join t_opt_tag opt
                                                    on opt.tag_id = t.id
                                                 start with t.parent_id = contract_iptv.
                                                 c_tag_opt_packs
                                                connect by prior t.id = t.parent_id) then
                                           tar_c.title
                                          else
                                           null
                                        end,
                                        ', ') WITHIN
                                GROUP(order by tar_c.title)
                             end packet_channel,
                             case
                               when tar.product_category = 5 then
                                listagg(case
                                          when tar_opt.id in
                                               (select opt.option_id
                                                  from t_dic_tag t
                                                  join t_opt_tag opt
                                                    on opt.tag_id = t.id
                                                 start with t.parent_id = contract_iptv.
                                                 c_tag_opt_packs
                                                         or t.id = 708
                                                connect by prior t.id = t.parent_id) then
                                           null
                                          else
                                           nvl(tar_c.title, mrf_opt.name_option)
                                        end,
                                        ', ') WITHIN
                                GROUP(order by
                                      nvl(tar_c.title, mrf_opt.name_option))
                               else
                                listagg(nvl(tar_c.title, mrf_opt.name_option),
                                        ', ') WITHIN
                                GROUP(order by
                                      nvl(tar_c.title, mrf_opt.name_option))
                             end opt
                        from (select s.id service_id,
                                     s.product_category,
                                     s.tech_id,
                                     s.request_id,
                                     tt.rn,
                                     s.comment_status comments,
                                     s.date_change_state,
                                     s.worker_create,
                                     s.state_id,
                                     s.date_create_fact date_create,
                                     s.mrf_order_num,
                                     nvl(max(nvl(tar_v.title, tar_mrf.title)),listagg(tar_m.title,', ') within group (order by tar_m.id)) tar_name,
                                     nvl(nvl(max(tag_pack.tag_name),
                                         max(tag_mon.tag_name)), listagg(dic_m.dv_name,', ') within group (order by tar_m.id)) tag_name,
                                     max(pr_mon.id) mon_prod_id
                                from (Select request_id, rn
                                        from table(l_col_request)
                                       where rn between
                                             (l_num_page - 1) * pi_count_req + 1 and
                                             (l_num_page) * pi_count_req) tt
                                join tr_request_service s
                                  on s.id = tt.request_id
                                left join tr_service_product sp
                                  on sp.service_id = s.id
                                left join tr_request_product pr_mon
                                  on sp.product_id = pr_mon.id
                                 and pr_mon.type_product = 1
                                left join t_tariff_version tar_v
                                  on tar_v.id = pr_mon.tar_id
                                 and pr_mon.type_tariff = 1
                                left join t_version_tag vt
                                  on vt.object_id = tar_v.id
                                 and pr_mon.type_tariff = 1
                                left join (select t.id, t.tag_name
                                            from t_dic_tag t
                                          connect by t.parent_id = prior t.id
                                           start with t.id in (4, 24, 144)) tag_mon
                                  on tag_mon.id = vt.tag_id
                                 and pr_mon.type_tariff = 1
                                left join t_mrf_tariff tar_mrf
                                  on tar_mrf.id = pr_mon.tar_id
                                 and pr_mon.type_tariff = 3
                                left join tr_request_product pr_pack
                                  on sp.product_id = pr_pack.id
                                 and pr_pack.type_product = 2
                                left join t_version_tag vt_pack
                                  on vt_pack.object_id = pr_pack.tar_id
                                 and pr_pack.type_tariff = 1
                                left join (select t.id, t.tag_name
                                            from t_dic_tag t
                                          connect by t.parent_id = prior t.id
                                           start with t.id in (10, 72)) tag_pack
                                  on tag_pack.id = vt_pack.tag_id                                                               
                                -- для тарифов мобильной связи
                                left join TR_REQUEST_SERVICE_MOBILE rsm
                                  on rsm.service_id = s.id
                                left join t_tarif_by_at_id tar_m
                                  on tar_m.at_id = rsm.tar_id
                                left join t_dic_values dic_m
                                  on tar_m.tariff_type = dic_m.dv_id
                               group by s.id,
                                        s.tech_id,
                                        s.request_id,
                                        tt.rn,
                                        s.comment_status,
                                        s.date_change_state,
                                        s.worker_create,
                                        s.product_category,
                                        s.tech_id,
                                        s.state_id,
                                        s.date_create_fact,
                                        s.mrf_order_num) tar
                        left join tr_request_product pr
                          on pr.id = tar.mon_prod_id
                        left join tr_product_option o
                          on o.product_id = pr.id
                         and o.service_id = tar.service_id
                        left join t_tariff_option tar_opt
                          on tar_opt.id = o.option_id
                         and pr.type_tariff = 1
                         and tar_opt.alt_name not in
                             ('discount_doubleplay', 'discount_tripleplay')
                        left join t_tariff_option_cost tar_c
                          on tar_c.option_id = tar_opt.id
                         and tar_c.tar_ver_id = pr.tar_id
                         and pr.type_tariff = 1
                         and nvl(tar_c.tech_id, nvl(tar.tech_id, -1)) =
                             nvl(tar.tech_id, -1)
                        left join t_mrf_tariff_option mrf_opt
                          on mrf_opt.id = o.option_id
                         and pr.type_tariff = 3
                       group by tar.service_id,
                                tar.tech_id,
                                tar.request_id,
                                tar.rn,
                                tar.comments,
                                tar.date_change_state,
                                tar.worker_create,
                                tar.tar_name,
                                tar.tag_name,
                                tar.product_category,
                                tar.state_id,
                                tar.date_create,
                                tar.mrf_order_num) aa
                left join table(l_request_devices) rd
                 on rd.num1 = aa.request_id
                and rd.num2 = aa.service_id
                ) bb
        join tr_request rr
          on rr.id = bb. request_id
        join t_clients p_cl
          on p_cl.client_id = rr.contact_person_id
         and p_cl.client_type = 'P'
        join t_person pp
          on pp.person_id = p_cl.fullinfo_id
        join t_address aa
          on rr.address_id = aa.addr_id
        left join t_dic_region dr
          on dr.reg_id = rr.region_id
        left join t_dic_mrf dm
          on dm.id = dr.mrf_id
        left join t_Organizations orgM
          on dm.org_id = orgM.Org_id
        left join t_Organizations orgF
          on dr.org_id = orgf.Org_id
        left join t_organizations o_rtk
          on o_rtk.org_id = 0
        join t_users u
          on u.usr_id = bb.worker_create
        left join t_dic_request_state drs
          on bb.state_id = drs.state_id
        join t_organizations oo
          on oo.org_id = rr.org_id
        left join tr_request_to_request rtr
          on rtr.request_id=rr.id
        left join t_organizations o_rtr
          on o_rtr.org_id=rtr.base_org_id;
          
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;
    
  -----------------------------------------
  --Получение списка заявок на подключение ФЛ по инициативе ИС МРФ
  --%param pi_worker_id пользователь
  --%param pi_date_start Дата начала выборки (дата создания заявки)
  --%param pi_date_end Дата окончания выборки (дата создания заявки)
  --%param pi_state_id Статусы услуги
  --%param pi_system_id Система поступления заявки
  --%param pi_channel_id Канал поступления заявки
  --%param pi_org_id Код организации, за которой закреплена заявка 
  --%param pi_kl_region кладровский код региона
  --%param pi_local_id локальный ид города или улицы
  --%param pi_house_local_id локальный ид дома
  --%param pi_house_num номер дома
  --%param pi_office номер квартиры
  --%param po_err_num код ошибки
  --%param po_err_msg сообщение об ошибке
  -----------------------------------------
  function get_request_for_search_order(pi_worker_id      in number,
                                        pi_date_start     in date,
                                        pi_date_end       in date,
                                        pi_state_id       in num_tab,
                                        pi_system_id      in number,
                                        pi_channel_id     in number,
                                        pi_org_id         in number,
                                        pi_kl_region      in varchar2,
                                        pi_local_id       in number,
                                        pi_house_local_id in number,
                                        pi_house_num      in varchar2,
                                        pi_office         in varchar2,
                                        po_err_num        out number,
                                        po_err_msg        out varchar2)
    return sys_refcursor is
    res         sys_refcursor;
    l_func_name varchar2(150) := 'request_list.get_request_for_search_order';
  
    User_Orgs    Num_Tab;
    l_reg_org    ARRAY_NUM_2;
    User_reg_org ARRAY_NUM_2;
  
    L_rc_by_date num_tab;
    l_res        num_tab;
  begin
    user_orgs := SECURITY_PKG.Get_User_Orgs_Tab_By_Right_str(pi_worker_id,
                                                             'MPZ.ORDER.PHYS.VIEW',
                                                             pi_org_id);
  
    l_reg_org := SECURITY_PKG.get_region_by_worker_right2(pi_worker_id => pi_worker_id,
                                                          pi_right_str => string_tab('MPZ.ORDER.PHYS.VIEW'),
                                                          pi_org_id    => num_tab(NVL(pi_org_id,
                                                                                      0)));
  
    User_reg_org := intersect_num2(User_Orgs, l_reg_org);
  
    select s.id bulk collect
      into L_rc_by_date
      from tr_request c
      join table(User_reg_org) reg_org
        on reg_org.number_1 = c.org_id
       and nvl(c.region_id, '-1') =
           nvl(reg_org.number_2, nvl(c.region_id, -1))
      join tr_request_service s
        on c.id = s.request_id
      join t_dic_region r
        on r.reg_id = c.region_id
      join t_clients cl
        on cl.client_id = c.client_id
       and cl.client_type = 'P'
     where c.date_create >=
           nvl(pi_date_start, to_date('01.01.1900', 'dd.mm.yyyy'))
       and c.date_create <
           nvl(pi_date_end, to_date('01.01.3000', 'dd.mm.yyyy'))
       and (pi_state_id is null or pi_state_id is empty or
           s.state_id in (select * from table(pi_state_id)))
       and (pi_system_id is null or c.system_id = pi_system_id)
       and (pi_channel_id is null or c.channel_id = pi_channel_id)
       and (pi_org_id is null or c.org_id = pi_org_id)
       and (pi_kl_region is null or r.kl_region = pi_kl_region);
  
    if pi_local_id is not null then
      select s.id bulk collect
        into l_res
        from table(L_rc_by_date) cc
        join tr_request_service s
          on cc.column_value = s.id
        join tr_request r
          on r.id = s.request_id
        join t_address a
          on a.addr_id = r.address_id
       where a.addr_obj_id in
             (select ao.id
                from t_address_object ao
              connect by prior ao.id = ao.parent_id
                     and ao.is_deleted = 0
               start with ao.id = pi_local_id)
         and (pi_house_local_id is null or
             a.addr_house_obj_id = pi_house_local_id)
         and (pi_house_num is null or a.addr_building = pi_house_num)
         and (pi_office is null or a.addr_office = pi_office);
    else
      l_res := L_rc_by_date;
    end if;
  
    open res for
      select distinct s.request_id
        from table(l_res) tt
        join tr_request_service s
          on tt.column_value = s.id;
  
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;

  ----------------------------------
  -- Просмотр данных заявки на подключение ЕШПД ф-ция для м2м
  -- %param pi_orderId идентификатор заявки
  -- %param pi_productId идентификатор услуги
  -- %param pi_orderUserId 10-значный идентификатор заявки - стркоа 10 символов
  -- %param pi_date_type Тип пепедаваемого диапазона дат
  --          {*} 1 – дата приема заявки в ЕИССД/МПЗ
  --          {*} 2 – дата последней операции над заявкой
  --          {*} 3-дата фактического приема услуги
  --          {*} 4-дата последней операции над услугой
  --          {*} 5 – дата фактического приема заявки  
  -- %param pi_DateTimeBeg Дата начала периода
  -- %param pi_DateTimeEnd Дата окончания периода
  -- %param pi_orderstateId Статус
  -- %param pi_orderChannelId канал поступления заявки
  -- %param pi_orderInstAdrCode ссылка на адрес
  -- %param pi_worker_id пользователь
  -- %param pi_org_id код организации
  -- %param pi_region кладровский код региона - строка 2 символа
  -- %param pi_product_category категория продукта
  -- %param pi_type_get тип запроса:
  -- {*} 0 - по услуга
  -- {*} 1 -  по заявкам
  -- %param po_all_count Возвращается общее кол-во заявок, подходящих под условия данных
  -- %param res2 Возвращается список опций по заявке
  -- %param res3 Возвращаются источники информации о ЕШПД
  -- %param res4 Возвращается информация о тех. возможности
  -- %param cur_Device Возвращается список устройств
  -- %param po_existing_service возвращается курсор с уже подключенными услугами
  -- %param po_params возвращается курсор с метками
  -- %param po_mobile информация о моб связи
  -- %param po_response возвращается код ошибки
  -- %param po_message возвращается сообщение об ошибке
  ----------------------------------
  function Get_requests_M2M(pi_orderId          in number,
                            pi_productId        in number,
                            pi_orderUserId      in varchar2,
                            pi_date_type        in number,
                            pi_DateTimeBeg      in date,
                            pi_DateTimeEnd      in date,
                            pi_orderstateId     in number,
                            pi_orderChannelId   in number,
                            pi_orderInstAdrCode in varchar2,
                            pi_worker_id        in number,
                            pi_org_id           in number,
                            pi_region           in varchar2,
                            pi_product_category in number, -- категория продукта (1 -- ШПД, 5 -- ИП ТВ)
                            pi_tech_posib       in number,
                            pi_mrfOrderId       in varchar2,
                            pi_type_get         in number,
                            po_all_count        out number, -- выводим общее кол-во заявок, подходящих под условия данных
                            po_tariff           out sys_refcursor,
                            po_options          out sys_refcursor,
                            po_inf_source       out sys_refcursor,
                            po_tech_pos         out sys_refcursor,
                            po_Device           out sys_refcursor, -- Список устройств.
                            po_existing_service out sys_refcursor,
                            po_params           out sys_refcursor,
                            po_mobile           out sys_refcursor,
                            po_err_num          out number,
                            po_err_msg          out varchar2)
    return sys_refcursor is
  
    cur_order        sys_refcursor;
    User_Orgs        Num_Tab;
    l_col_request_id request_Order_Tab;
    l_rc_by_date     num_tab;
    l_rc_by_ids      num_tab;
    l_rc_by_adr      num_tab;
    l_rc_fin         num_tab;
    l_rc_fin2        num_tab;
    l_rc_by_tech     num_tab;
  
    l_reg_org    ARRAY_NUM_2;
    l_kl_reg     varchar2(2);
    User_reg_org ARRAY_NUM_2;
    l_func_name  varchar2(100) := 'request_list.Get_requests_M2M';
  begin
  
    if pi_region is not null and length(pi_region) = 1 then
      l_kl_reg := '0' || pi_region;
    else
      l_kl_reg := pi_region;
    end if;
  
    user_orgs := SECURITY_PKG.Get_User_Orgs_Tab_By_Right_str(pi_worker_id,
                                                             'SD.REQUEST.PHYS.VIEW_LIST',
                                                             pi_org_id);
  
    l_reg_org := SECURITY_PKG.get_region_by_worker_right2(pi_worker_id => pi_worker_id,
                                                          pi_right_str => string_tab('SD.REQUEST.PHYS.VIEW_LIST'),
                                                          pi_org_id    => num_tab(NVL(pi_org_id,
                                                                                      0)));
  
    User_reg_org := intersect_num2(User_Orgs, l_reg_org);
  
    l_rc_by_date := null;
    l_rc_by_ids  := null;
    l_rc_by_adr  := null;
    l_rc_by_tech := null;
  
    if nvl(pi_date_type, 1) = 1 and
       (pi_DateTimeBeg is not null or pi_DateTimeEnd is not null) then
      select s.id bulk collect
        into l_rc_by_date
        from tr_request r
        join tr_request_service s
          on s.request_id = r.id
         and s.product_category != 13 --смс
         and s.TYPE_REQUEST = 0
       where r.date_create >=
             nvl(pi_DateTimeBeg, to_date('01.01.1900', 'dd.mm.yyyy'))
         and r.date_create <
             nvl(pi_DateTimeEnd, to_date('01.01.3000', 'dd.mm.yyyy'));
    
    elsif nvl(pi_date_type, 1) = 2 and
          (pi_DateTimeBeg is not null or pi_DateTimeEnd is not null) then
      select s.id bulk collect
        into l_rc_by_date
        from tr_request r
        join tr_request_service s
          on s.request_id = r.id
         and s.product_category != 13 --смс
         and s.TYPE_REQUEST = 0
       where r.date_change_fact >=
             nvl(pi_DateTimeBeg, to_date('01.01.1900', 'dd.mm.yyyy'))
         and r.date_change_fact <
             nvl(pi_DateTimeEnd, to_date('01.01.3000', 'dd.mm.yyyy'));
    
    elsif nvl(pi_date_type, 1) = 3 and
          (pi_DateTimeBeg is not null or pi_DateTimeEnd is not null) then
    
      select s.id bulk collect
        into l_rc_by_date
        from tr_request_service s
       where s.date_create_fact >=
             nvl(pi_DateTimeBeg, to_date('01.01.1900', 'dd.mm.yyyy'))
         and s.date_create_fact <
             nvl(pi_DateTimeEnd, to_date('01.01.3000', 'dd.mm.yyyy'))
         and s.product_category != 13 --смс
         and s.TYPE_REQUEST = 0;
    
    elsif nvl(pi_date_type, 1) = 4 and
          (pi_DateTimeBeg is not null or pi_DateTimeEnd is not null) then
    
      select s.id bulk collect
        into l_rc_by_date
        from tr_request_service s
       where s.date_change_fact >=
             nvl(pi_DateTimeBeg, to_date('01.01.1900', 'dd.mm.yyyy'))
         and s.date_change_fact <
             nvl(pi_DateTimeEnd, to_date('01.01.3000', 'dd.mm.yyyy'))
         and s.product_category != 13 --смс
         and s.TYPE_REQUEST = 0;
         
    elsif nvl(pi_date_type, 1) = 5 and
       (pi_DateTimeBeg is not null or pi_DateTimeEnd is not null) then
      select s.id bulk collect
        into l_rc_by_date
        from tr_request r
        join tr_request_service s
          on s.request_id = r.id
         and s.product_category != 13 --смс
         and s.TYPE_REQUEST = 0
       where r.date_create_fact >=
             nvl(pi_DateTimeBeg, to_date('01.01.1900', 'dd.mm.yyyy'))
         and r.date_create_fact <
             nvl(pi_DateTimeEnd, to_date('01.01.3000', 'dd.mm.yyyy'));
    end if;
  
    if pi_orderId is not null then
      select s.id bulk collect
        into l_rc_by_ids
        from tr_request_service s
       where s.request_id = pi_orderId
         and s.product_category != 13 --смс
         and s.TYPE_REQUEST = 0;
    
    elsif pi_productId is not null then
      select s.id bulk collect
        into l_rc_by_ids
        from tr_request_service s
       where s.id = pi_productId
         and s.product_category != 13 --смс
         and s.TYPE_REQUEST = 0;
    
    elsif pi_orderUserId is not null then
      select id bulk collect
        into l_rc_by_ids
        from (select rc.id
                from t_request_connect_mov rc
                join tr_request_service s
                  on rc.id = s.id
               where rc.order_num = pi_orderUserId
                 and rc.product_category != 11
              union
              select s.id
                from tr_request_service s
               where s.id = pi_orderUserId -- для новых заявок
                 and s.product_category != 13 --смс
                 and s.TYPE_REQUEST = 0);
    
    elsif pi_mrfOrderId is not null then
      select s.id bulk collect
        into l_rc_by_ids
        from tr_request_service s
       where s.mrf_order_num = pi_mrfOrderId
         and s.product_category != 13 --смс
         and s.TYPE_REQUEST = 0;
    
    end if;
  
    if pi_orderInstAdrCode is not null then
      select s.id bulk collect
        into l_rc_by_adr
        from tr_request r
        join tr_request_service s
          on s.request_id = r.id
         and s.product_category != 13 --смс
         and s.TYPE_REQUEST = 0
        join t_address a
          on a.addr_id = r.address_id
       where a.addr_code_street = pi_orderInstAdrCode;
    
    end if;
  
    l_rc_fin2 := intersects(l_rc_fin2, l_rc_by_date);
    l_rc_fin2 := intersects(l_rc_fin2, l_rc_by_ids);
    l_rc_fin2 := intersects(l_rc_fin2, l_rc_by_adr);
  
    --исключим онлайн заявки
    select t.column_value bulk collect
      into l_rc_fin
      from table(l_rc_fin2) t
      join tr_service_product sp
        on sp.service_id = t.column_value
      join tr_request_product p
        on p.id = sp.product_id
       and p.type_product = 1
       and p.type_tariff != 3;
  
    if nvl(pi_tech_posib, 0) = 3 then
      select s.id bulk collect
        into l_rc_by_tech
        from table(l_rc_fin) t
        join tr_request_service s
          on t.column_value = s.id
        join tr_request_serv_exist e
          on e.request_id = s.request_id
         and e.service_type = s.product_category
       where nvl(e.is_old_address,0)=0;
    
    end if;
  
    if nvl(pi_tech_posib, 0) in (1, 2, 4) then
      select distinct aa.id bulk collect
        into l_rc_by_tech
        from (select s.id, max(nvl(th.is_success, -1)) tech_posib --max(decode(th.info, 'Y', 1, 'R', 1, null, -1, 0))
                from table(l_rc_fin) t
                join tr_request_service s
                  on t.column_value = s.id
                left join tr_request_tech_poss th
                  on th.request_id = s.request_id
                 and th.service_type = s.product_category
               group by s.id) aa
       where nvl(aa.tech_posib, -1) =
             decode(pi_tech_posib, 1, 1, 2, 0, 4, -1);
    end if;
  
    l_rc_fin := intersects(l_rc_fin, l_rc_by_tech);
  
    select request_Order_Type(id, rn) bulk collect
      into l_col_request_id
      from (select id,
                   case
                     when pi_type_get = 1 then
                      DENSE_RANK() OVER(ORDER BY request_id)
                     else
                      rownum
                   end rn
              from (select /*+ Index(s PK_REQUEST_SERVICE) */
                     s.id, r.id request_id
                      from table(l_rc_fin) t
                      join tr_request_service s
                        on s.id = t.column_value
                      join tr_request r
                        on r.id = s.request_id
                      join table(User_reg_org) reg_org
                        on reg_org.number_1 = r.org_id
                       and nvl(r.region_id, '-1') =
                           nvl(reg_org.number_2, nvl(r.region_id, -1))
                      join t_clients cc
                        on cc.client_id = r.client_id
                       and cc.client_type = 'P'
                      join t_dic_region dr
                        on dr.reg_id = r.region_id
                     where (pi_product_category is Null or
                           s.product_category = pi_product_category)
                       and (pi_orderStateId is null or
                           s.state_id = pi_orderStateId)
                       and (pi_orderChannelId is null or
                           r.channel_id = pi_orderChannelId)
                       and (l_kl_reg is null or dr.kl_region = l_kl_reg)
                       and s.state_id not in (24)
                     order by r.date_create_fact));
  
    select nvl(max(rn), 0) into po_all_count from table(l_col_request_id);
  
    open cur_order for
      select s.id,
             r.id contract_client_id,
             r.date_create_fact date_create,
             null OrderDate,
             r.dogovor_number dogovor_number,
             nvl(rc.order_num, s.id) order_num,
             r.org_id,
             s.state_id as state_id,
             r.date_change_fact date_change,
             s.operation_id LastOpId,
             r.org_id LastOpOrgId,
             r.worker_change LastOpUserId,
             s.comment_status LastOpComment,
             r.request_content,
             r.channel_id,
             0 is_wish_iptv,
             r.notified_by_sms smsnoticeflag,
             r.notified_by_email emailnoticeflag,
             r.client_id,
             -- rc.tar_id,
             --rp.tar_id as pack_id,
             s.tech_id TechId,
             s.product_category product_category,
             r.instwishtime1 instwishtime1,
             r.instwishtime2,
             r.instagreedatetime1,
             r.instagreedatetime2,
             null instfactdate,
             null internet_login,
             r.inst_adr_phone telephone_number,
             r.address_id,
             rownum as index_number,
             p.person_firstname FirstName,
             p.person_lastname LastName,
             p.person_middlename MiddleName,
             d.doc_type IDCardType,
             d.doc_series IDCardSeria,
             d.doc_number IDCardNumber,
             d.doc_regdate IDCardDate,
             d.doc_extrainfo IDCardAuthority,
             c_p.person_phone contactCellPhone,
             c_p.person_home_phone contactHomePhone,
             substr(c_p.person_email, 0, Constant_pkg.c_person_email_length) contactEmail,
             ---       
             ar.kl_code       Code,
             ar.kl_lvladdrobj lvl,
             ao.a_full_name,
             aa.addr_building House,
             hr.kl_code       HouseCode,
             aa.addr_office   Flat,
             aa.addr_oth      Oth,
             aadr.kl_region   addr_kl_region,
             ---
             dr.kl_region region_id,
             null date_confirm_email,
             s.mrf_order_num,
             u1.usr_login manager_login,
             p1.person_lastname || ' ' || p1.person_firstname || ' ' ||
             p1.person_middlename manager_fio,
             p.person_birthday,
             --t.name tariff_name,
             c_p.person_lastname contact_lastname,
             c_p.person_firstname contact_firstname,
             c_p.person_middlename contact_middlename,
             cast(TO_CHAR(r.instwishtimecall1 - Constant_pkg.c_DB_GMT / 24 +
                          tz.tz_offset / 24,
                          'hh24') as number) wishtimecall1,
             cast(TO_CHAR(r.instwishtimecall2 - Constant_pkg.c_DB_GMT / 24 +
                          tz.tz_offset / 24,
                          'hh24') as number) wishtimecall2,
             trunc(r.instwishtimecall2 - Constant_pkg.c_DB_GMT / 24 +
                   tz.tz_offset / 24) wishdatecall,
             decode(r.fix_system_id, 13, 0, 1) is_blocked,
             r.elk_account,
             r.personal_account,
             sa.sa_emp_num,
             rsd.cost_usl_number cost_usl_number,
             nvl(rsd.usl_number, rsd.dop_usl_number) usl_number,
             rsd.dogovor_serv_num
        from (Select request_id
                from table(l_col_request_id) tt
               where tt.rn <= 1000) tmp
        join tr_request_service s
          on tmp.request_id = s.id
        left join tr_request_service_detail rsd
          on rsd.service_id = s.id  
        left join t_request_connect_mov rc
          on rc.id = s.id
        join tr_request r
          on r.id = s.request_id
        join t_address aa
          on r.address_id = aa.addr_id
        left join t_dic_region aadr
          on aadr.reg_id = aa.region_id
        left join t_address_object ao
          on aadr.kl_region = ao.kl_region
         and ao.id = aa.addr_obj_id
         and ao.is_deleted = 0
        left join t_address_ref ar
          on ar.id = ao.id
         and ar.is_deleted = 0
         and ar.source_type = aadr.source_type
        left join t_address_house_ref hr
          on hr.id = aa.addr_house_obj_id
         and hr.is_deleted = 0
         and hr.source_type = aadr.source_type
        join t_clients cl
          on cl.client_id = r.client_id
         and cl.client_type = 'P'
        join t_person p
          on p.person_id = cl.fullinfo_id
        left join t_documents d
          on d.doc_id = p.doc_id
        join t_dic_region dr
          on r.region_id = dr.reg_id
        left join t_timezone tz
          on tz.tz_id = dr.gmt
        join tr_service_product sp
          on sp.service_id = s.id
        join tr_request_product pr
          on pr.id = sp.product_id
         and pr.type_product = 1 
        left join t_users u1
          on u1.usr_id = s.worker_create
        left join t_person p1
          on u1.usr_person_id = p1.person_id
        left join t_tariff t
          on rc.tar_id = t.id
        join t_clients p_cl
          on p_cl.client_id = r.contact_person_id
         and p_cl.client_type = 'P'
        join t_person c_p
          on c_p.person_id = p_cl.fullinfo_id
        left join t_seller_active sa
          on sa.sa_id = r.seller_id;
  
    open po_tariff for
      select aa.id, aa.tariff_id, aa.title, tt.pack_id
        from (select s.id, v.tariff_id, v.title
                from (Select request_id
                        from table(l_col_request_id) tt
                       where tt.rn <= 1000) tmp
                join tr_request_service s
                  on tmp.request_id = s.id
                 and s.product_category != 4 
                join tr_service_product sp
                  on sp.service_id = s.id
                join tr_request_product p
                  on p.id = sp.product_id
                 and p.type_product = 1
                 and p.type_tariff != 3
                join t_tariff_version v
                  on v.id = p.tar_id) aa
        left join (select s.id, v.tariff_id pack_id
                     from (Select request_id
                             from table(l_col_request_id) tt
                            where tt.rn <= 1000) tmp
                     join tr_request_service s
                       on tmp.request_id = s.id
                      and s.product_category != 4  
                     join tr_service_product sp
                       on sp.service_id = s.id
                     join tr_request_product p
                       on p.id = sp.product_id
                      and p.type_product = 2
                      and p.type_tariff != 3
                     join t_tariff_version v
                       on v.id = p.tar_id) tt
          on aa.id = tt.id;
  
    Open po_options for
      select s.id          request_id,
             s.request_id  contract_client_id,
             rto.option_id Id,
             a.option_name name_option
        from tr_request_service s
        join tr_product_option rto
          on rto.service_id = s.id
        join mv_options a
          on rto.option_id = a.id
        join table(l_col_request_id) tt
          on tt.request_id = s.id
         and tt.rn <= 1000
        and s.product_category != 4  
       Order by s.id, rto.option_id;
  
    Open po_inf_source for
      select distinct s.request_id    contract_client_id,
                      rps.id_source   Id,
                      rps.text_source text
        from tr_request_service s
        join tr_request_inf_source rps
          on rps.request_id = s.request_id
        join table(l_col_request_id) tt
          on tt.request_id = s.id
         and tt.rn <= 1000
       Order by s.request_id, rps.id_source;
  
    Open po_tech_pos for
      select pp.id,
             s.id                     request_id,
             s.request_id             contract_client_id,
             pp.date_create           reg_date,
             pp.tech_id               TechId,
             pp.INFO                  res,
             pp.max_Speed             maxSpeed,
             pp.TECH_DATA             descr,
             pp.plan_date             planDate,
             pp.tech_possibility_type
        from tr_request_service s
        join tr_request_tech_poss pp
          on pp.request_id = s.request_id
         and pp.is_response = 1
         and s.product_category = pp.service_type
        join table(l_col_request_id) tt
          on tt.request_id = s.id
         and tt.rn <= 1000
       Order by s.id, pp.date_create;
  
    Open po_device for
      Select s.id                request_id,
             t.device_type,
             t.device_use_scheme,
             s.request_id        contract_client_id,
             t.device_count
        from tr_request_device t
        join tr_request_service s
          on s.request_id = t.request_id
        join table(l_col_request_id) tt
          on tt.request_id = s.id
         and tt.rn <= 1000;
  
    open po_existing_service for
      select s.request_id    contract_client_id,
             s.id            request_id,
             es.service_type type_product,
             es.account_num  account,
             es.tech_id
        from tr_request_serv_exist es
        join tr_request_service s
          on s.request_id = es.request_id
        join table(l_col_request_id) tt
          on tt.request_id = s.id
         and tt.rn <= 1000
       where nvl(es.is_old_address,0)=0;
  
    open po_params for
      select distinct s.request_id, p.key
        from tr_request_params p
        join tr_request_service s
          on s.request_id = p.request_id
        join table(l_col_request_id) tt
          on tt.request_id = s.id
         and tt.rn <= 1000
       where p.key in
             (select d.id from t_dic_request_params d where d.is_mpz = 1)
         and p.value = '1';
         
    open po_mobile for
     select s.id            service_id,
            m.id            mobile_id,
            at.at_remote_id,
            m.reg_mvno,
            m.cnt_sim,
            m.color_sim,
            at.title as tar_name,
            r.name as reg_name
       from tr_request_service s
       join tr_request_service_mobile m
         on m.service_id = s.id
       join T_TARIF_BY_AT_ID at
         on at.at_id = m.tar_id
       join t_dic_mvno_region r 
         on r.id = m.reg_mvno         
       join table(l_col_request_id) tt
         on tt.request_id = s.id
        and tt.rn <= 1000
      where s.product_category = 4;
  
    po_err_num := 0;
    return cur_order;
  
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      if (cur_order%isopen) then
        close cur_order;
      end if;
      return null;
  end;    

  --------------------------------------------
  --Получение списка услуг для мобильного приложения по пользователю-создателю
  --фильтр по дате создания Услуги и пользователю создателю Услуги
  --------------------------------------------
  function get_orders_mobile_app(pi_date_begin    in date,
                                 pi_date_end      in date,
                                 pi_create_worker in number,
                                 po_err_num       out number,
                                 po_err_msg       out varchar2)
    return sys_refcursor is
    res         sys_refcursor;
    l_func_name varchar2(100) := 'request_list.get_orders_mobile_app';
  begin
  
    open res for
      select aa.request_id contract_id,
             aa.id,
             aa.order_num,
             aa.date_create_fact data_create_req,
             nvl2(trim(p.person_lastname || p.person_firstname ||
                       p.person_middlename),
                  p.person_lastname || ' ' || p.person_firstname || ' ' ||
                  p.person_middlename,
                  c_p.person_lastname || ' ' || c_p.person_firstname || ' ' ||
                  c_p.person_middlename) person_name,
             aa.state_id,
             st.state_name,
             aa.product_category,
             a.addr_street addr_oth,
             a.addr_building,
             a.addr_office,
             service_tab,
             type_req
        from (select r.id request_id,
                     s.id,
                     case
                       when pr.type_tariff = 3 then
                        11
                       else
                        s.product_category
                     end product_category,
                     s.state_id,
                     s.date_create_fact,
                     cl.fullinfo_id,
                     p_cl.fullinfo_id contact_pers_id,
                     nvl(rc.order_num, s.id) order_num,
                     r.address_id,
                     cast(collect(s.product_category) as num_tab) service_tab,
                     1 type_req
                from tr_request r
                join tr_request_service s
                  on s.request_id = r.id
                left join t_request_connect_mov rc
                  on rc.id = s.id
                join t_clients cl
                  on cl.client_id = r.client_id
                 and cl.client_type = 'P'
                join t_clients p_cl
                  on p_cl.client_id = r.contact_person_id
                 and p_cl.client_type = 'P'
                join tr_service_product sp
                  on sp.service_id = s.id
                 and sp.product_type in (0, 1)
                join tr_request_product pr
                  on pr.id = sp.product_id
                 and pr.type_product in (0, 1)
               where s.date_create_fact >= pi_date_begin
                 and s.date_create_fact < pi_date_end
                 and s.worker_create = pi_create_worker
                 and s.product_category in (1, 2, 4, 5)
                 and s.state_id not in (24)
               group by r.id,
                        s.id,
                        case
                          when pr.type_tariff = 3 then
                           11
                          else
                           s.product_category
                        end,
                        s.state_id,
                        s.date_create_fact,
                        cl.fullinfo_id,
                        p_cl.fullinfo_id,
                        nvl(rc.order_num, s.id),
                        r.address_id
              union all
              select d.id           contract_id,
                     null           id,
                     null           product_category,
                     d.state_id     state_id,
                     d.create_date  data_create_req,
                     cl.fullinfo_id,
                     null,
                     null           order_num,
                     d.address_id,
                     null           service_tab,
                     2              type_req
                from t_request_delivery d
                join t_clients cl
                  on cl.client_id = d.client_id
                 and cl.client_type = 'P'
               where d.create_date >= pi_date_begin
                 and d.create_date < pi_date_end
                 and d.create_worker_id = pi_create_worker) aa
        join t_person p
          on p.person_id = aa.fullinfo_id
        left join t_person c_p
          on c_p.person_id = aa.contact_pers_id
        left join t_dic_request_state st
          on st.state_id = aa.state_id
        join t_address a
          on a.addr_id = aa.address_id
       where rownum <= 1000
       order by date_create_fact;
  
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;

  ------------------------------------
  --получение списка смс-заявок в состояниях 17,20,21,22
  -- %param pi_worker_id пользователь
  -- %param pi_pack_id номер заявки
  -- %param pi_org_id организация
  -- %param pi_date_type тип даты
  -- %param pi_date_begin период поступления: начало
  -- %param pi_date_end период поступления: окончание
  -- %param pi_state_id состояние услуги
  -- %param pi_num_page номер страницы для отображения
  -- %param pi_count_req кол-во записей на странице для отображения
  -- %param pi_kl_region кладровский код региона
  -- %param po_all_count выводим общее кол-во заявок, подходящих под условия данных
  -- %param po_err_num код ошибки
  -- %param po_err_msg сообщение об ошибке
  -- @return возвращается информация о заявках (курсор)
  -------------------------------------------------
  function get_list_sms_request(pi_worker_id  in number,
                                pi_pack_id    in number,
                                pi_org_id     in number,
                                pi_date_type  in number,
                                pi_date_begin in date,
                                pi_date_end   in date,
                                pi_state_id   in number,
                                pi_num_page   in number,
                                pi_count_req  in number,
                                pi_kl_region  in varchar2,
                                po_all_count  out number,
                                po_err_num    out number,
                                po_err_msg    out varchar2)
    return sys_refcursor is
    res sys_refcursor;
    --User_Orgs     Num_Tab;
    l_col_request  request_Order_Tab;
    l_max_num_page number;
    l_num_page     number;
    l_date_cr_beg  date;
    l_date_cr_end  date;
    l_date_ch_beg  date;
    l_date_ch_end  date;
    l_sel_orgs     num_tab;
    l_func_name    varchar2(100) := 'request_list.get_list_sms_request';
  begin
  
    po_err_num := 0;
  
    if pi_worker_id > 1000 then
      /*Для инфогорода костыль*/
      if not SECURITY_PKG.Check_User_Right_str('SD.REQUEST.PHYS.VIEW_LIST',
                                               pi_worker_id,
                                               po_err_num,
                                               po_err_msg) then
      
        po_err_num := 1;
        po_err_msg := 'Операция не выполнена. Недостаточно прав доступа';
      
        return null;
      end if;
    
      if po_err_num != 0 then
        return null;
      end if;
    
      select m.org_id bulk collect
        into l_sel_orgs
        from MV_ORG_TREE m
      Connect by prior m.org_id = m.org_pid
       Start with m.org_id = nvl(pi_org_id, 0);
    else
      select m.org_id bulk collect
        into l_sel_orgs
        from MV_ORG_TREE m
      Connect by prior m.org_id = m.org_pid
       Start with m.org_id = nvl(pi_org_id, 0)
      intersect
      select column_value
        from table(SECURITY_PKG.Get_User_Orgs_Tab_By_Right_str(pi_worker_id,
                                                               'MPZ.ORDER.PHYS.VIEW',
                                                               pi_org_id));
    end if;
  
    if pi_date_begin is not null and nvl(pi_date_type, 1) = 1 then
      l_date_cr_beg := pi_date_begin - Constant_pkg.c_GMT;
    else
      l_date_cr_beg := to_date('01.01.1900', 'dd.mm.yyyy');
    end if;
  
    if pi_date_end is not null and nvl(pi_date_type, 1) = 1 then
      l_date_cr_end := pi_date_end - Constant_pkg.c_GMT;
    else
      l_date_cr_end := to_date('31.12.2999', 'dd.mm.yyyy');
    end if;
  
    if pi_date_begin is not null and nvl(pi_date_type, 1) = 2 then
      l_date_ch_beg := pi_date_begin - Constant_pkg.c_GMT;
    else
      l_date_ch_beg := to_date('01.01.1900', 'dd.mm.yyyy');
    end if;
  
    if pi_date_end is not null and nvl(pi_date_type, 1) = 2 then
      l_date_ch_end := pi_date_end - Constant_pkg.c_GMT;
    else
      l_date_ch_end := to_date('31.12.2999', 'dd.mm.yyyy');
    end if;
  
    select request_Order_Type(id, rownum) bulk collect
      into l_col_request
      from (select r.id
              from tr_request r
              join tr_request_service s
                on s.request_id = r.id
               and s.product_category = 13
               and s.state_id in (17, 20, 21, 22)
              join t_clients cl
                on r.client_id = cl.client_id
               and cl.client_type = 'P'
            /* join t_person p
            on p.person_id = cl.fullinfo_id*/
              join tr_request_params par
                on par.request_id = r.id
               and par.key = 'sms_request'
               and par.value = '1'
              left join t_dic_region r
                on r.reg_id = r.region_id
             where not exists
             (select null
                      from tr_request_service ss
                     where ss.request_id = r.id
                       and ss.product_category != 13)
                  -- and cc.channel_id = 21
               and (pi_state_id is null and s.state_id in (17, 20, 21, 22) or
                   s.state_id = pi_state_id)
               and (r.date_create_fact >= l_date_cr_beg)
               and (r.date_create_fact < l_date_cr_end)
               and (r.date_change_fact >= l_date_ch_beg)
               and (r.date_change_fact < l_date_ch_end)
               and (pi_pack_id is null or r.id = pi_pack_id)
               and (r.kl_region = pi_kl_region or pi_kl_region is null)
               and r.org_id in (select column_value from table(l_sel_orgs))
             order by r.date_create_fact desc);
  
    po_all_count := l_col_request.count;
  
    l_max_num_page := round(po_all_count / nvl(pi_count_req, 1));
  
    if (pi_num_page > l_max_num_page and l_max_num_page <> 0) then
      l_num_page := l_max_num_page + 1;
    else
      l_num_page := nvl(pi_num_page, 1);
    end if;
  
    open res for
      select contract_id,
             person_phone,
             date_create,
             state_name,
             mrf,
             filial,
             is_blocked,
             org_id,
             channel_id,
             kl_region,
             sms_state_id,
             wishtimecall1,
             wishtimecall2,
             wishdatecall,
             person_lastname,
             person_firstname,
             person_middlename
        from (select r.id contract_id,
                     p.person_phone,
                     r.date_create_fact date_create,
                     drs.state_name,
                     orgM.Org_Name mrf,
                     orgf.org_name filial,
                     r.org_id,
                     r.channel_id,
                     s.state_id sms_state_id,
                     cast(TO_CHAR(r.instwishtimecall1 -
                                  Constant_pkg.c_DB_GMT / 24 +
                                  tz.tz_offset / 24,
                                  'hh24') as number) wishtimecall1,
                     cast(TO_CHAR(r.instwishtimecall2 -
                                  Constant_pkg.c_DB_GMT / 24 +
                                  tz.tz_offset / 24,
                                  'hh24') as number) wishtimecall2,
                     trunc(r.instwishtimecall2 - Constant_pkg.c_DB_GMT / 24 +
                           tz.tz_offset / 24) wishdatecall,
                     r.kl_region,
                     p.person_lastname,
                     p.person_firstname,
                     p.person_middlename,
                     decode(r.fix_system_id, 13, 0, 1) is_blocked,
                     row_number() over(order by r.date_create_fact, r.id) as rn
                from (Select request_id, rn
                        from table(l_col_request)
                       where rn between (l_num_page - 1) * pi_count_req + 1 and
                             (l_num_page) * pi_count_req) col_cc
                join tr_request r
                  on col_cc.request_id = r.id
                join tr_request_service s
                  on s.request_id = r.id
                 and s.product_category = 13
                 and s.state_id in (17, 20, 21, 22)
                join t_clients cl
                  on r.contact_person_id = cl.client_id
                join t_person p
                  on p.person_id = cl.fullinfo_id
                join t_dic_request_state drs
                  on drs.state_id = s.state_id
                left join t_dic_region r
                  on r.reg_id = r.region_id
                left join t_timezone tz
                  on tz.tz_id = r.gmt
                left join t_dic_mrf dm
                  on r.mrf_id = dm.Id
                left join t_organizations orgM
                  on orgm.org_id = nvl(dm.org_id, 0)
                left join t_Organizations orgF
                  on r.org_id = orgf.Org_id
              
              ) tmp
       order by date_create desc;
  
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      if res%isopen then
        close res;
      end if;
  end;
  
  --фильтр поиска заявок "для web-заявок"(выгрузка в эксель)
 
  function get_list_phys_serv_erkc_call(pi_worker_id        in number,
                                        pi_contractNum      in varchar2,
                                        pi_request_id       in tr_request.request_id%type,
                                        pi_mrf_order_num    in varchar2,
                                        pi_only_mine        in number,
                                        pi_FirstName        in t_person.person_firstname%type,
                                        pi_LastName         in t_person.person_lastname%type,
                                        pi_MiddleName       in t_person.person_middlename%type,
                                        pi_IDCardSeria      in t_documents.doc_series%type,
                                        pi_IDCardNumber     in t_documents.doc_number%type,
                                        pi_ELK_ACCOUNT      in varchar2,
                                        pi_PERSONAL_ACCOUNT in varchar2,
                                        pi_phone            in t_person.person_phone%type,
                                        pi_uslNumber        in tr_request_service_detail.usl_number%type,
                                        pi_mainEquipment    in tr_request_service_detail.main_equipment%type,
                                        ------------
                                        pi_addr_id   in number,
                                        pi_house_id  in number,
                                        pi_house_num in varchar2,
                                        pi_flat_id   in varchar2,
                                        ------------
                                        pi_services          in num_tab,
                                        pi_org_tab           in num_tab,
                                        pi_org_child_include in number,
                                        pi_type_request      in varchar2,
                                        pi_addr_label        in number,
                                        pi_type_operation    in num_tab,
                                        pi_DateTimeBeg       in date,
                                        pi_DateTimeEnd       in date,
                                        pi_sorting_date      in number,
                                        pi_device_id         in number,
                                        pi_device_card_type  in number, -- тип оборудования из Лиры
                                        pi_Channels          in num_tab,
                                        pi_States            in num_tab,
                                        pi_systems           in num_tab,
                                        pi_type_system       in number,
                                        pi_tech_posib        in number,
                                        pi_send              in request_param_type,
                                        pi_tag_id            in number,
                                        pi_opt_tag           in number,
                                        pi_req_type          in number,
                                        ---------
                                        pi_num_page  in number,
                                        pi_count_req in number,
                                        pi_column    in number,
                                        pi_sorting   in number,
                                        po_all_count out number,
                                        po_err_num   out number,
                                        po_err_msg   out varchar2)
    return sys_refcursor is
    l_func_name varchar2(100) := 'request_list.get_list_phys_service_erkc_call';

    res            sys_refcursor;
    l_order_asc    number; -- по возрастанияю
    l_order_desc   number; -- по убыванию
    l_max_num_page number;
    l_num_page     number;

    l_org_num_2  array_num_2 := array_num_2();
    l_org_tab    num_tab;
    User_Orgs    Num_Tab;
    User_Orgs2   Num_Tab;
    l_reg_org    ARRAY_NUM_2;
    User_reg_org ARRAY_NUM_2;

    l_callback_view number := 0;
    l_sms_view      number := 0;
    l_date_cr_beg   date;
    l_date_cr_end   date;
    l_date_ch_beg   date;
    l_date_ch_end   date;
    l_tag_tab       num_tab;
    l_service_tab   num_tab := pi_services;

    l_cnt number;

    l_all_req1  num_tab; -- по датам
    l_all_req2  num_tab;
    l_all_req3  num_tab;
    l_all_req4  num_tab;
    l_all_req5  num_tab;
    l_all_req6  num_tab;
    l_all_req7  num_tab;
    l_all_req8  num_tab;
    l_all_req9  num_tab;
    l_all_req10 num_tab;

    l_all_req             num_tab;
    l_col_request         request_Order_Tab;
    l_request_id          number;
    l_filt_callback_hours number;
    
    l_request_devices array_num2_str;
  begin
    if pi_org_tab is not null then
      select rec_num_2(number_1 => column_value,
                       number_2 => pi_org_child_include) bulk collect
        into l_org_num_2
        from table(pi_org_tab);
    else
      l_org_num_2 := array_num_2(rec_num_2(1, nvl(pi_org_child_include, 1)));
    end if;

    l_org_tab := get_orgs_tab_for_multiset(pi_orgs            => l_org_num_2,
                                           Pi_worker_id       => pi_worker_id,
                                           pi_block           => 1,
                                           pi_org_relation    => null,
                                           pi_is_rtmob        => 0,
                                           pi_tm_1009_include => 1);

    User_Orgs := SECURITY_PKG.Get_User_Orgs_Tab_By_Right_str(pi_worker_id,
                                                             'SD.REQUEST.PHYS.VIEW_LIST',
                                                             null);

    User_Orgs2 := intersects(l_org_tab, User_Orgs);

    l_reg_org := SECURITY_PKG.get_region_by_worker_right2(pi_worker_id => pi_worker_id,
                                                          pi_right_str => string_tab('SD.REQUEST.PHYS.VIEW_LIST'),
                                                          pi_org_id    => l_org_tab);

    User_reg_org := intersect_num2(User_Orgs2, l_reg_org);

    If pi_sorting = 0 then
      l_order_asc := NVL(pi_column, 2);
    else
      l_order_desc := NVL(pi_column, 2);
    end If;

    if pi_DateTimeBeg is not null and nvl(pi_sorting_date, 0) in (0, 3) then
      l_date_cr_beg := pi_DateTimeBeg - Constant_pkg.c_GMT;
    else
      l_date_cr_beg := to_date('01.01.1900', 'dd.mm.yyyy');
    end if;

    if pi_DateTimeEnd is not null and nvl(pi_sorting_date, 0) in (0, 3) then
      l_date_cr_end := pi_DateTimeEnd - Constant_pkg.c_GMT;
    else
      l_date_cr_end := to_date('31.12.2999', 'dd.mm.yyyy');
    end if;

    if pi_DateTimeBeg is not null and nvl(pi_sorting_date, 0) in (1, 2) then
      l_date_ch_beg := pi_DateTimeBeg - Constant_pkg.c_GMT;
    else
      l_date_ch_beg := to_date('01.01.1900', 'dd.mm.yyyy');
    end if;

    if pi_DateTimeEnd is not null and nvl(pi_sorting_date, 0) in (1, 2) then
      l_date_ch_end := pi_DateTimeEnd - Constant_pkg.c_GMT;
    else
      l_date_ch_end := to_date('31.12.2999', 'dd.mm.yyyy');
    end if;

    if (pi_DateTimeBeg is not null or pi_DateTimeEnd is not null) then
      if pi_sorting_date = 0 then
        select /*+ index(p IDX_REQUEST_SERVICE_DATCR_F) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_create_fact >= l_date_cr_beg
           and p.date_create_fact < l_date_cr_end;
        
      elsif pi_sorting_date = 1 then
        select /*+ index(p IDX_REQUEST_SERVICE_DATCH_F) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_change_fact >= l_date_ch_beg
           and p.date_change_fact < l_date_ch_end;
        
      elsif pi_sorting_date = 2 then
        --по дате перехода в текущее состояние
        select /*+ index(p IDX_TR_PRODUCT_DCHST) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_change_state >= l_date_ch_beg
           and p.date_change_state < l_date_ch_end;
      elsif pi_sorting_date = 3 then
        select /*+ index(p IDX_TR_PRODUCT_DCR) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_create >= l_date_cr_beg
           and p.date_create < l_date_cr_end;
      end if;
    end if;

    select count(*)
      into l_cnt
      from table(pi_States) s
     where s.column_value in (17, 18, 20, 21, 22, 23);

    --определим показывать коллбак или нет
    -- состояние колбек или метка обратный звонок
    if l_cnt > 0 or nvl(pi_type_request, '0') = 'callback_request' then
      l_callback_view := 1;
    end if;

    if l_cnt > 0 or nvl(pi_type_request, '0') = 'sms_request' then
      l_sms_view := 1;
      l_service_tab.extend;
      l_service_tab(l_service_tab.count) := 13;
    end if;

    --Топ городов
    if pi_addr_label is not null then
      select distinct p.id bulk collect
        into l_all_req9
        from table(l_all_req1) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request r
          on r.id = p.request_id
        join t_address a
          on a.addr_id = r.address_id
        join (select ao.id
                from t_address_object ao
              connect by prior ao.id = ao.parent_id
               start with ao.id in
                          (select al.local_id
                             from t_addr_label al
                            where al.lable_id = pi_addr_label)) aa
          on aa.id = a.addr_obj_id;
      l_all_req1 := l_all_req9;
    end if;

    if (pi_FirstName is not null or pi_LastName is not null or
       pi_MiddleName is not null or pi_phone is not null) then
      select id bulk collect
        into l_all_req2
        from (select t.id
                from (select p.id,
                             pp.person_firstname,
                             pp.person_lastname,
                             pp.person_middlename,
                             pp.person_phone,
                             pp.person_home_phone
                        from tr_request r
                        join tr_request_service p
                          on r.id = p.request_id
                        join t_clients cl
                          on cl.client_id = r.client_id
                         and cl.client_type = 'P'
                        join t_clients p_cl
                          on p_cl.client_id = r.contact_person_id
                         and p_cl.client_type = 'P'
                        join t_person pp
                          on p_cl.fullinfo_id = pp.person_id
                       where lower(pp.person_firstname) =
                             lower(trim(pi_FirstName))
                          or lower(pp.person_lastname) =
                             lower(trim(pi_LastName))
                          or lower(pp.person_middlename) =
                             lower(trim(pi_MiddleName))
                          or pp.person_phone = pi_phone
                          or pp.person_home_phone = pi_phone) t
               where (pi_FirstName is null or
                     lower(t.person_firstname) = lower(trim(pi_FirstName)))
                 and (pi_LastName is null or
                     lower(t.person_lastname) = lower(trim(pi_LastName)))
                 and (pi_MiddleName is null or
                     lower(t.person_middlename) = lower(trim(pi_MiddleName)))
                 and (pi_phone is null or t.person_phone = pi_phone or
                     t.person_home_phone = pi_phone)
              union
              select t.id
                from (select p.id,
                             pp.person_firstname,
                             pp.person_lastname,
                             pp.person_middlename,
                             pp.person_phone,
                             pp.person_home_phone
                        from tr_request r
                        join tr_request_service p
                          on r.id = p.request_id
                        join t_clients cl
                          on cl.client_id = r.client_id
                         and cl.client_type = 'P'
                        join t_person pp
                          on cl.fullinfo_id = pp.person_id
                       where lower(pp.person_firstname) =
                             lower(trim(pi_FirstName))
                          or lower(pp.person_lastname) =
                             lower(trim(pi_LastName))
                          or lower(pp.person_middlename) =
                             lower(trim(pi_MiddleName))) t
               where (pi_FirstName is null or
                     lower(t.person_firstname) = lower(trim(pi_FirstName)))
                 and (pi_LastName is null or
                     lower(t.person_lastname) = lower(trim(pi_LastName)))
                 and (pi_MiddleName is null or
                     lower(t.person_middlename) = lower(trim(pi_MiddleName))));
    end if;

    if pi_request_id is not null then
      begin
        l_request_id := to_number(pi_request_id);
        select s.id bulk collect
          into l_all_req10
          from tr_request_service s
          join tr_request r
            on r.id = s.request_id
         where r.id = l_request_id
            or s.id = l_request_id;
      exception
        when others then
          select s.id bulk collect
            into l_all_req10
            from tr_request_service s
            join tr_request r
              on r.id = s.request_id
           where r.request_id = pi_request_id;
      end;
    end if;

    if (pi_contractNum is not null or pi_mrf_order_num is not null or
       pi_ELK_ACCOUNT is not null or pi_PERSONAL_ACCOUNT is not null) then
      select t.id bulk collect
        into l_all_req3
        from (select p.id,
                     r.request_id,
                     r.dogovor_number,
                     p.mrf_order_num,
                     r.elk_account,
                     r.personal_account
                from tr_request_service p
                join tr_request r
                  on r.id = p.request_id
               where r.dogovor_number = pi_contractNum
                  or p.mrf_order_num = pi_mrf_order_num
                  or r.elk_account = pi_ELK_ACCOUNT
                  or r.personal_account = pi_PERSONAL_ACCOUNT) t
       where (t.dogovor_number = pi_contractNum or pi_contractNum is null)
         and (t.mrf_order_num = pi_mrf_order_num or pi_mrf_order_num is null)
         and (t.elk_account = pi_ELK_ACCOUNT or pi_ELK_ACCOUNT is null)
         and (t.personal_account = pi_PERSONAL_ACCOUNT or
             pi_PERSONAL_ACCOUNT is null);
    end if;

    if (pi_IDCardSeria is not null or pi_IDCardNumber is not null) then
      select p.id bulk collect
        into l_all_req5
        from tr_request r
        join tr_request_service p
          on r.id = p.request_id
        join t_system s
          on r.system_id = s.id
        join t_clients cl
          on cl.client_id = r.client_id
         and cl.client_type = 'P'
        join t_person pp
          on cl.fullinfo_id = pp.person_id
        join t_documents d
          on d.doc_id = pp.doc_id
       where d.doc_series = upper(trim(pi_IDCardSeria))
         and d.doc_number = upper(trim(pi_IDCardNumber));
    end if;

    if (pi_addr_id is not null and
       (pi_house_id is not null or pi_house_num is not null)) then
      select p.id bulk collect
        into l_all_req4
        from (select r.id
                from tr_request r
                join t_address a
                  on r.address_id = a.addr_id
               where a.addr_obj_id = pi_addr_id
                 and (pi_house_id is null or pi_house_id = a.addr_house_obj_id)
                 and (pi_house_num is null or
                     trim(pi_house_num) = a.addr_building)
                 and (pi_flat_id is null or trim(pi_flat_id) = a.addr_office)) t
        join tr_request_service p
          on p.request_id = t.id;
    end if;

    -- фильтр по меткам
    if nvl(pi_type_request, '0') != '0' then
      select distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req1) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_params par
          on par.request_id = p.request_id
         and par.key = pi_type_request
         and nvl(par.value, '0') = '1'
       where l_callback_view = 1
          or l_sms_view = 1
          or p.state_id not in (17, 18, 20, 21, 22, 23);
    elsif pi_type_request is null and l_all_req1 is not null then
      --нам без разницы какие метки, главное убрать колбак, если он не нужен
      if l_callback_view = 1 and l_sms_view = 1 then
        l_all_req8 := l_all_req1;
      elsif l_sms_view = 1 and l_callback_view = 0 then
        --исключим колбек в статусах колбек
        select distinct p.id bulk collect
          into l_all_req8
          from table(l_all_req1) req
          join tr_request_service p
            on p.id = req.column_value
          left join tr_request_params par
            on par.request_id = p.request_id
           and par.key = 'callback_request'
           and p.state_id in (17, 18, 20, 21, 22, 23)
         where nvl(par.value, 0) = 0;
      elsif l_sms_view = 0 and l_callback_view = 1 then
        --исключим смс в статусах колбек
        select distinct p.id bulk collect
          into l_all_req8
          from table(l_all_req1) req
          join tr_request_service p
            on p.id = req.column_value
          left join tr_request_params par
            on par.request_id = p.request_id
           and par.key = 'sms_request'
           and p.state_id in (17, 18, 20, 21, 22, 23)
         where nvl(par.value, 0) = 0;
      else
        select /*+ use_nl(rc req) */
        distinct p.id bulk collect
          into l_all_req8
          from table(l_all_req1) req
          join tr_request_service p
            on p.id = req.column_value
         where p.state_id not in (17, 18, 20, 21, 22, 23);
      end if;
    elsif pi_type_request = '0' then
      select distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req1) req
        join tr_request_service p
          on p.id = req.column_value
        left join tr_request_params par
          on par.request_id = p.request_id
         and nvl(par.value, '0') = '1'
         and par.key in ('connect_friend',
                         'anketa_mpz',
                         'telemarketing',
                         'callback_request',
                         'sms_request',
                         'is_quick_request',
                         'elk_request',
                         'address_is_not_directory')
       where (l_callback_view = 1 or
             l_sms_view = 1 and p.state_id not in (17, 18, 20, 21, 22, 23))
         and nvl(par.value, '0') = '0';
    end if;

    --Фильтр по уже подключенным услугам
    if nvl(pi_tech_posib, 0) = 3 then
      select distinct s.id bulk collect
        into l_all_req7
        from table(l_all_req8) req
        join tr_request_service s
          on req.column_value = s.id
        join TR_REQUEST_SERV_EXIST e
          on e.request_id = s.request_id
         and e.service_type = s.product_category
       where nvl(e.is_old_address,0)=0;
    end if;

    --Фильтр по проверке ТВ
    if nvl(pi_tech_posib, 0) in (1, 2, 4, 5) then
      select distinct aa.id bulk collect
        into l_all_req7
        from (select /* ordered use_nl(req rc) use_nl(rc th) */
               p.id, max(th.is_success) tech_posib
                from table(l_all_req8) req
                join tr_request_service p
                  on req.column_value = p.id
                join tr_request r
                  on r.id = p.request_id
                left join tr_request_tech_poss th
                  on th.request_id = p.request_id
                 and th.service_type = p.product_category
                 and th.address_id = r.address_id
                 and nvl(th.tech_id, 0) = nvl(p.tech_id, nvl(th.tech_id, 0))
               group by p.id) aa
       where nvl(aa.tech_posib, -1) =
             decode(pi_tech_posib, 1, 1, 2, 0, 4, -1, 5, 5);
    end if;

    if pi_uslNumber is not null or pi_mainEquipment is not null then
      select distinct s.service_id bulk collect
        into l_all_req6
        from tr_request_service_detail s
       where (s.main_equipment = pi_mainEquipment or pi_mainEquipment is null)
         and (s.usl_number = pi_uslNumber or pi_uslNumber is null);
    end if;

    if l_all_req7 is null then
      l_all_req := intersects(l_all_req, l_all_req8);
    else
      l_all_req := intersects(l_all_req, l_all_req7);
    end if;
    l_all_req := intersects(l_all_req, l_all_req2);
    l_all_req := intersects(l_all_req, l_all_req3);
    l_all_req := intersects(l_all_req, l_all_req10);
    l_all_req := intersects(l_all_req, l_all_req4);
    l_all_req := intersects(l_all_req, l_all_req5);
    l_all_req := intersects(l_all_req, l_all_req6);

    if pi_send is not null and pi_send.key is not null then
      --Обработка заявок сторонними системами
      l_all_req8 := null;
      select distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_params par
          on par.request_id = p.request_id
         and par.key = pi_send.key
       where par.value = pi_send.value;
      
      l_all_req  := l_all_req8;
      l_all_req8 := null;
    end if;

    --фильтр по тегам опций
    if pi_opt_tag is not null then
      l_all_req8 := null;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_product_option t
          on t.service_id = req.column_value
        join t_opt_tag opt
          on opt.option_id = t.option_id
       where opt.tag_id = pi_opt_tag;
      
      l_all_req  := l_all_req8;
      l_all_req8 := null;
    end if;

    -- фильтр по оборудованию из ЕИССД
    if pi_device_id is not null then
      l_all_req8 := null;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_device t
          on t.request_id = p.request_id
       where t.device_type = pi_device_id
         and not exists (select ord.orders_id
                from tr_request_orders ord
                join t_orders o
                  on ord.orders_id = o.id
               where o.order_type = 5
                 and ord.request_id = p.request_id);               
      l_all_req := l_all_req8;
      l_all_req8 := null;
    end if;  
    -- по типам оборудование из Лиры
    if pi_device_card_type is not null then      
      l_all_req8 := null;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_orders ro
          on ro.request_id = p.request_id
        join t_orders o
          on o.id = ro.orders_id
         and o.order_type = 5
        join tr_request_device_card rdc
          on rdc.request_id = p.request_id
       where rdc.type_equipment = pi_device_card_type
         and rdc.is_exists = 0;
               
      l_all_req := l_all_req8;
      l_all_req8 := null;
    end if;

    if pi_type_request is null and l_callback_view = 0 and l_sms_view = 0 and
       l_all_req1 is null then
      --метки не были переданы, значит  по параметрам клиента.
      --Нужно убрать колбак статусы, раз они не нужен
      
      select /*+ use_nl(rc req) */
      distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
       where p.state_id not in (17, 18, 20, 21, 22, 23);
      
      l_all_req := l_all_req8;
    end if;

    --подберем тарифы по тегам
    if pi_tag_id is not null then
      select t.id bulk collect
        into l_tag_tab
        from t_dic_tag t
      connect by t.parent_id = prior t.id
       start with t.id = pi_tag_id;
      
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_service_product ps
          on ps.service_id = req.column_value
        join tr_request_product p
          on p.id = ps.product_id
        join t_version_tag vt
          on vt.object_id = p.tar_id
       where vt.tag_id in (select column_value from table(l_tag_tab));
      
      l_all_req := l_all_req8;
    end if;
    --
    if pi_req_type is not null then
      l_all_req8 := null;
      if pi_req_type = 3 then
        l_filt_callback_hours := constant_pkg.c_filt_callback_hours;
      end if;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service rs
          on rs.id = req.column_value
        join tr_request r
          on r.id = rs.request_id
       where (pi_req_type = 1 and r.instwishtimecall2 < sysdate)
          or (pi_req_type = 2 and r.instwishtimecall1 < sysdate and
             r.instwishtimecall2 > sysdate)
          or (pi_req_type = 3 and r.instwishtimecall1 between sysdate and
             sysdate + l_filt_callback_hours / 24)
          or (pi_req_type = 4 and r.instwishtimecall1 > sysdate)
          or (pi_req_type = 5 and r.instwishtimecall1 is null);
      l_all_req := l_all_req8;
    end if;

    --/*+ ordered */
    select request_Order_Type(id, rownum) bulk collect
      into l_col_request
      from (select /*+ ordered */
             p.id
              from (select /*+ use_nl(p xxx) */
                     p.*
                      from table(l_all_req) xxx
                      join tr_request_service p
                        on p.id = xxx.column_value) p
              join tr_request r
                on r.id = p.request_id
              join table(User_reg_org) reg_org
                on reg_org.number_1 = r.org_id
               and nvl(r.region_id, '-1') =
                   nvl(reg_org.number_2, nvl(r.region_id, -1))
              join t_system s
                on r.system_id = s.id
              join table(pi_systems) ss
                on ss.column_value = r.system_id
              join Table(pi_Channels) ch
                on ch.column_value = r.channel_id
              join table(l_service_tab) serv
                on serv.column_value = p.product_category
              join t_clients cl
                on cl.client_id = r.client_id
               and cl.client_type = 'P'
              join t_clients p_cl
                on p_cl.client_id = r.contact_person_id
               and p_cl.client_type = 'P'
              join t_person pp
                on p_cl.fullinfo_id = pp.person_id
              left join t_address a
                on r.address_id = a.addr_id
              left join t_dic_region dr
                on dr.reg_id = r.region_id
              left join t_dic_mrf dm
                on dm.id = dr.mrf_id
              left join t_Organizations orgM
                on dm.org_id = orgM.Org_id
              left join t_Organizations orgF
                on dr.org_id = orgf.Org_id
              join t_dic_request_state drs
                on p.state_id = drs.state_id
              join table(pi_States) stt
                on stt.column_value = drs.state_id
             where (pi_type_system = 2 or (s.is_federal = pi_type_system))
               and ((nvl(pi_only_mine, 0) = 1 and
                   p.worker_create = pi_worker_id) or
                   nvl(pi_only_mine, 0) <> 1)
                  --and (r.system_id in (select * from table(pi_systems)))
               and p.state_id not in (24)
               and p.type_request in (select * from table(pi_type_operation))
             order by decode(l_order_asc,
                             9,
                             (case
                               when r.instwishtimecall1 > sysdate then
                                4
                               when sysdate between r.instwishtimecall1 and
                                    r.instwishtimecall2 then
                                2
                               when r.instwishtimecall1 is null then
                                3
                               else
                                1
                             end),
                             null) asc,
                      decode(l_order_asc,
                             null,
                             null,
                             1,
                             lpad(r.request_id, 13, '0'),
                             2,
                             to_char(p.date_create_fact,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             3,
                             lower(pp.person_lastname || ' ' ||
                                   pp.person_firstname || ' ' ||
                                   pp.person_middlename),
                             4,
                             nvl2(a.addr_oth,
                                  a.addr_oth || ' ' || a.addr_building || '-' ||
                                  a.addr_office,
                                  substr(a.addr_city,
                                         0,
                                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                                  a.addr_street || ' ' || a.addr_building || '-' ||
                                  a.addr_office),
                             5,
                             drs.state_name,
                             6,
                             to_char(p.date_change_state,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             8,
                             upper(orgM.Org_Name || orgf.org_name),
                             9,
                             case
                               when nvl(r.instwishtimecall1, sysdate) > sysdate then
                                to_char(nvl(r.instwishtimecall1, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                               else
                                to_char(nvl(r.instwishtimecall2, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                             end,
                             null) asc,
                      decode(l_order_desc,
                             9,
                             (case
                               when r.instwishtimecall1 > sysdate then
                                4
                               when sysdate between r.instwishtimecall1 and
                                    r.instwishtimecall2 then
                                2
                               when r.instwishtimecall1 is null then
                                3
                               else
                                1
                             end),
                             null) desc,
                      decode(l_order_desc,
                             null,
                             null,
                             1,
                             lpad(r.request_id, 13, '0'),
                             2,
                             to_char(p.date_create_fact,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             3,
                             lower(pp.person_lastname || ' ' ||
                                   pp.person_firstname || ' ' ||
                                   pp.person_middlename),
                             4,
                             nvl2(a.addr_oth,
                                  a.addr_oth || ' ' || a.addr_building || '-' ||
                                  a.addr_office,
                                  substr(a.addr_city,
                                         0,
                                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                                  a.addr_street || ' ' || a.addr_building || '-' ||
                                  a.addr_office),
                             5,
                             drs.state_name,
                             6,
                             to_char(p.date_change_state,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             8,
                             upper(orgM.Org_Name || orgf.org_name),
                             9,
                             case
                               when nvl(r.instwishtimecall1, sysdate) > sysdate then
                                to_char(nvl(r.instwishtimecall1, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                               else
                                to_char(nvl(r.instwishtimecall2, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                             end,
                             null) desc);

    po_all_count := l_col_request.count;

    l_max_num_page := round(po_all_count / nvl(pi_count_req, 1));

    if (pi_num_page > l_max_num_page and l_max_num_page <> 0) then
      l_num_page := l_max_num_page + 1;
    else
      l_num_page := nvl(pi_num_page, 1);
    end if;
    
    -- оборудование по заявке
    select rec_num2_str(tab_dev.request_id,
                        tab_dev.service_id,
                        tab_dev.device) bulk collect
      into l_request_devices
      from (select tab.request_id,
                   tab.service_id,
                   listagg(nvl2(tab.device_name,
                                tab.device_name || ' ' || device_type_op ||
                                ' - ' || device_cnt || 'шт.',
                                null),
                           ', ') WITHIN GROUP(order by 1) device
              from (select s.request_id,
                           s.id service_id,
                           nvl2(is_ord.cnt_orders, rdc.type_equipment, d.id) as device_id,
                           nvl2(is_ord.cnt_orders, l_eq.name, d.name) as device_name,
                           nvl2(is_ord.cnt_orders, l_top.name, ss.name) as device_type_op,
                           --nvl2(is_ord.cnt_orders, nvl(rdc.quant, 1), rd.device_count)
                           sum(nvl2(is_ord.cnt_orders,
                                    nvl(rdc.quant, 1),
                                    rd.device_count)) as device_cnt
                      from (Select request_id, rn
                              from table(l_col_request)
                             where rn between
                                   (l_num_page - 1) * pi_count_req + 1 and
                                   (l_num_page) * pi_count_req) tt
                      join tr_request_service s
                        on s.id = tt.request_id
                    -- для карточки оборудования  
                    -- наличие наряда на подключение по заявке
                      left join (select ord.request_id,
                                       count(ord.orders_id) cnt_orders
                                  from tr_request_orders ord
                                  join t_orders o
                                    on ord.orders_id = o.id
                                 where o.order_type = 5
                                 group by ord.request_id) is_ord
                        on s.request_id = is_ord.request_id
                    -- для группировки карточек оборудования по услугам  
                      left join (select ord.request_id,
                                       ord.service_id,
                                       ord.orders_id
                                  from tr_request_orders ord
                                  join t_orders o
                                    on ord.orders_id = o.id
                                 where o.order_type = 5) group_serv
                        on s.request_id = group_serv.request_id
                       and s.id = group_serv.service_id
                      left join tr_request_device_card rdc
                        on rdc.request_id = s.request_id
                       and group_serv.orders_id = rdc.order_id
                       and rdc.is_exists = 0
                      left join t_lira_type_operations l_top
                        on l_top.id = rdc.op_type_id
                      left join t_lira_type_equipment l_eq
                        on l_eq.id = rdc.type_equipment
                    -- end для карточки оборудования
                      left join tr_request_device rd
                        on rd.request_id = s.request_id
                       and is_ord.cnt_orders is null
                      left join t_device d
                        on d.id = rd.device_type
                      left join t_eqipment_sale_schema ss
                        on ss.id = rd.device_use_scheme
                     group by s.request_id,
                              s.id,
                              nvl2(is_ord.cnt_orders, rdc.type_equipment, d.id),
                              nvl2(is_ord.cnt_orders, l_eq.name, d.name),
                              nvl2(is_ord.cnt_orders, l_top.name, ss.name)) tab
             group by tab.request_id, tab.service_id) tab_dev;

    open res for
      select rr.id,
             rr.request_id,
             bb.date_create,
             bb.service_id,
             bb.tech_id,
             bb.rn,
             bb.comments,
             bb.date_change_state,
             bb.worker_create,
             bb.tar_name,
             bb.tag_name,
             bb.product_category,
             bb.packet_channel,
             bb.opt,
             bb.device,
             bb.state_id,
             drs.state_name state,
             pp.person_lastname || ' ' || pp.person_firstname || ' ' ||
             pp.person_middlename as fio,
             pp.person_phone,
             pp.person_home_phone,
             pp.person_email,
             nvl2(aa.addr_oth,
                  aa.addr_oth || nvl2(aa.addr_building,
                                      ', д. ' || aa.addr_building,
                                      aa.addr_building) ||
                  nvl2(aa.addr_office,
                       ', кв. ' || aa.addr_office,
                       aa.addr_office),
                  substr(aa.addr_city,
                         0,
                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                  aa.addr_street || nvl2(aa.addr_building,
                                         ', д. ' || aa.addr_building,
                                         aa.addr_building) ||
                  nvl2(aa.addr_office,
                       ', кв. ' || aa.addr_office,
                       aa.addr_office)) adr,
             u.usr_login as create_user,
             rr.channel_id,
             nvl(orgM.Org_Name, o_rtk.org_name) mrf,
             nvl(orgf.org_name, o_rtk.org_name) filial,
             bb.mrf_order_num,
             oo.org_name,
             rr.instwishtimecall1,
             rr.instwishtimecall2,
             rr.callback_date,
             cs.date_op eissd_date_create,
             (SELECT CASE
                       WHEN u.usr_id != 777 THEN
                        p.person_lastname || ' ' || p.person_firstname || ' ' ||
                        p.person_middlename
                       ELSE
                        'Система'
                     END
                FROM t_users u
                LEFT JOIN t_person p
                  ON p.person_id = u.usr_person_id
               WHERE u.usr_id = rr.worker_change) worker_change,
             cs.instwishtimecall1 firstwishtimecall1,
             cs.instwishtimecall2 firstwishtimecall2
        
        from (select aa.service_id,
                     aa.tech_id,
                     aa.request_id,
                     aa.rn,
                     aa.comments,
                     aa.date_change_state,
                     aa.worker_create,
                     aa.tar_name,
                     aa.tag_name,
                     aa.product_category,
                     aa.packet_channel,
                     aa.opt,
                     aa.state_id,
                     aa.date_create,
                     aa.mrf_order_num,
                     rd.str device
/*                     listagg(nvl2(ro.cnt_orders,
                                  nvl2(rdc.card_id,
                                       l_eq.name || ' ' || l_top.name
                                       || ' - ' || nvl(rdc.quant, 1) || 'шт.',
                                       null),
                                  nvl2(rd.device_type,
                                       d.name || ' ' || ss.name || ' - ' ||
                                       rd.device_count || 'шт.',
                                       null)),
                             ', ') WITHIN GROUP(order by 1) device*/
                from (select tar.service_id,
                             tar.tech_id,
                             tar.request_id,
                             tar.rn,
                             tar.comments,
                             tar.date_change_state,
                             tar.worker_create,
                             tar.tar_name,
                             tar.tag_name,
                             tar.product_category,
                             tar.state_id,
                             tar.date_create,
                             tar.mrf_order_num,
                             case
                               when tar.product_category = 5 then
                                listagg(case
                                          when tar_opt.id in
                                               (select opt.option_id
                                                  from t_dic_tag t
                                                  join t_opt_tag opt
                                                    on opt.tag_id = t.id
                                                 start with t.parent_id = contract_iptv.
                                                 c_tag_opt_packs
                                                connect by prior t.id = t.parent_id) then
                                           tar_c.title
                                          else
                                           null
                                        end,
                                        ', ') WITHIN GROUP(order by tar_c.title)
                             end packet_channel,
                             case
                               when tar.product_category = 5 then
                                listagg(case
                                          when tar_opt.id in
                                               (select opt.option_id
                                                  from t_dic_tag t
                                                  join t_opt_tag opt
                                                    on opt.tag_id = t.id
                                                 start with t.parent_id = contract_iptv.
                                                 c_tag_opt_packs
                                                         or t.id = 708
                                                connect by prior t.id = t.parent_id) then
                                           null
                                          else
                                           nvl(tar_c.title, mrf_opt.name_option)
                                        end,
                                        ', ') WITHIN
                                GROUP(order by
                                      nvl(tar_c.title, mrf_opt.name_option))
                               else
                                listagg(nvl(tar_c.title, mrf_opt.name_option),
                                        ', ') WITHIN
                                GROUP(order by
                                      nvl(tar_c.title, mrf_opt.name_option))
                             end opt
                        from (select s.id service_id,
                                     s.product_category,
                                     s.tech_id,
                                     s.request_id,
                                     tt.rn,
                                     s.comment_status comments,
                                     s.date_change_state,
                                     s.worker_create,
                                     s.state_id,
                                     s.date_create_fact date_create,
                                     s.mrf_order_num,
                                     max(nvl(tar_v.title, tar_mrf.title)) tar_name,
                                     nvl(max(tag_pack.tag_name),
                                         max(tag_mon.tag_name)) tag_name,
                                     max(pr_mon.id) mon_prod_id
                                
                                from (Select request_id, rn
                                        from table(l_col_request)
                                       where rn between
                                             (l_num_page - 1) * pi_count_req + 1 and
                                             (l_num_page) * pi_count_req) tt
                                join tr_request_service s
                                  on s.id = tt.request_id
                                left join tr_service_product sp
                                  on sp.service_id = s.id
                                left join tr_request_product pr_mon
                                  on sp.product_id = pr_mon.id
                                 and pr_mon.type_product = 1
                                left join t_tariff_version tar_v
                                  on tar_v.id = pr_mon.tar_id
                                 and pr_mon.type_tariff = 1
                                left join t_version_tag vt
                                  on vt.object_id = tar_v.id
                                 and pr_mon.type_tariff = 1
                                left join (select t.id, t.tag_name
                                            from t_dic_tag t
                                          connect by t.parent_id = prior t.id
                                           start with t.id in (4, 24, 144)) tag_mon
                                  on tag_mon.id = vt.tag_id
                                 and pr_mon.type_tariff = 1
                                left join t_mrf_tariff tar_mrf
                                  on tar_mrf.id = pr_mon.tar_id
                                 and pr_mon.type_tariff = 3
                                left join tr_request_product pr_pack
                                  on sp.product_id = pr_pack.id
                                 and pr_pack.type_product = 2
                                left join t_version_tag vt_pack
                                  on vt_pack.object_id = pr_pack.tar_id
                                 and pr_pack.type_tariff = 1
                                left join (select t.id, t.tag_name
                                            from t_dic_tag t
                                          connect by t.parent_id = prior t.id
                                           start with t.id in (10, 72)) tag_pack
                                  on tag_pack.id = vt_pack.tag_id
                                
                               group by s.id,
                                        s.tech_id,
                                        s.request_id,
                                        tt.rn,
                                        s.comment_status,
                                        s.date_change_state,
                                        s.worker_create,
                                        s.product_category,
                                        s.tech_id,
                                        s.state_id,
                                        s.date_create_fact,
                                        s.mrf_order_num) tar
                        left join tr_request_product pr
                          on pr.id = tar.mon_prod_id
                        left join tr_product_option o
                          on o.product_id = pr.id
                         and o.service_id = tar.service_id
                        left join t_tariff_option tar_opt
                          on tar_opt.id = o.option_id
                         and pr.type_tariff = 1
                         and tar_opt.alt_name not in
                             ('discount_doubleplay', 'discount_tripleplay')
                        left join t_tariff_option_cost tar_c
                          on tar_c.option_id = tar_opt.id
                         and tar_c.tar_ver_id = pr.tar_id
                         and pr.type_tariff = 1
                         and nvl(tar_c.tech_id, nvl(tar.tech_id, -1)) =
                             nvl(tar.tech_id, -1)
                        left join t_mrf_tariff_option mrf_opt
                          on mrf_opt.id = o.option_id
                         and pr.type_tariff = 3
                       group by tar.service_id,
                                tar.tech_id,
                                tar.request_id,
                                tar.rn,
                                tar.comments,
                                tar.date_change_state,
                                tar.worker_create,
                                tar.tar_name,
                                tar.tag_name,
                                tar.product_category,
                                tar.state_id,
                                tar.date_create,
                                tar.mrf_order_num) aa  
               left join table(l_request_devices) rd
                 on rd.num1 = aa.request_id
                and rd.num2 = aa.service_id          	
               -- для карточки оборудования                
/*                left join (select ord.request_id, 
                                  ord.service_id,
                                  o.id order_id,                                  
                                  count(ord.orders_id) cnt_orders 
                             from tr_request_orders ord 
                             join t_orders o
                               on ord.orders_id = o.id
                            where o.order_type = 5
                            group by ord.request_id, 
                                  ord.service_id, 
                                  o.id) ro
                  on ro.request_id = aa.request_id
                 and ro.service_id = aa.service_id
                left join tr_request_device_card rdc
                  on rdc.request_id = aa.request_id   
                 and ro.order_id = rdc.order_id              
                 and rdc.is_exists = 0                   
                left join t_lira_type_operations l_top
                  on l_top.id = rdc.op_type_id
                left join t_lira_type_equipment l_eq
                  on l_eq.id = rdc.type_equipment
                -- end для карточки оборудования                         
                left join tr_request_device rd
                  on rd.request_id = aa.request_id
                 and ro.cnt_orders is null
                left join t_device d
                  on d.id = rd.device_type
                left join t_eqipment_sale_schema ss
                  on ss.id = rd.device_use_scheme*/
               /*group by aa.service_id,
                        aa.tech_id,
                        aa.request_id,
                        aa.rn,
                        aa.comments,
                        aa.date_change_state,
                        aa.worker_create,
                        aa.tar_name,
                        aa.tag_name,
                        aa.product_category,
                        aa.packet_channel,
                        aa.opt,
                        aa.state_id,
                        aa.date_create,
                        aa.mrf_order_num*/) bb
        join tr_request rr
          on rr.id = bb. request_id
        join t_clients p_cl
          on p_cl.client_id = rr.contact_person_id
         and p_cl.client_type = 'P'
        join t_person pp
          on pp.person_id = p_cl.fullinfo_id
        join t_address aa
          on rr.address_id = aa.addr_id
        left join t_dic_region dr
          on dr.reg_id = rr.region_id
        left join t_dic_mrf dm
          on dm.id = dr.mrf_id
        left join t_Organizations orgM
          on dm.org_id = orgM.Org_id
        left join t_Organizations orgF
          on dr.org_id = orgf.Org_id
        left join t_organizations o_rtk
          on o_rtk.org_id = 0
        join t_users u
          on u.usr_id = bb.worker_create
        left join t_dic_request_state drs
          on bb.state_id = drs.state_id
        join t_organizations oo
          on oo.org_id = rr.org_id
        LEFT JOIN TABLE(get_first_service_state_info(bb.service_id)) cs
          ON 1 = 1
       ORDER BY rr.id, bb.service_id;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;
  
  FUNCTION get_first_service_state_info(p_service_id NUMBER) RETURN service_state_info_tab
    PIPELINED AS
    v_service_state_info service_state_info_type;
  BEGIN
    FOR r IN (SELECT *
              FROM tr_service_change_state
              WHERE service_id = p_service_id
                    AND state_id != 24
              ORDER BY id)
    LOOP
      v_service_state_info.id := r.id;
      v_service_state_info.service_id := r.service_id;
      v_service_state_info.date_op := r.date_op;
      v_service_state_info.state_id := r.state_id;
      v_service_state_info.operation_id := r.operation_id;
      v_service_state_info.worker_id := r.worker_id;
      v_service_state_info.comments := r.comments;
      v_service_state_info.instwishtimecall1 := r.instwishtimecall1;
      v_service_state_info.instwishtimecall2 := r.instwishtimecall2;
      PIPE ROW(v_service_state_info);
      RETURN;
    END LOOP;
  END;
  
  -------------------------------------------------
  --Поиск заявок на фиксированные услуги связи для мвно
  --------------------------------------------------
  function get_request_for_mvno(pi_kl_region  in varchar2,
                                pi_request_id in number,
                                pi_worker_id  in number,
                                po_req_state  out number,
                                po_err_num    out number,
                                po_err_msg    out varchar2)
    return sys_refcursor is
    res         sys_refcursor;
    l_func_name varchar2(100) := 'request_list.get_request_for_mvno';
    l_id        number;
    L_req_state number;
    l_addr_id   number;
    l_addr      address_type;
  begin
    begin
      select r.id,
             min(case
                   when s.state_id in (1, 2, 17) then
                    1
                   when s.state_id in (3, 4, 5, 6) then
                    2
                   when s.state_id in (7, 8, 9, 10, 11, 12, 18, 20, 21, 23) then
                    3
                   when s.state_id in (13, 14, 15, 16, 22) then
                    4
                 end) req_state,
             r.ADDRESS_ID
        into l_id, L_req_state, l_addr_id
        from tr_request r
        join t_dic_region reg
          on reg.reg_id = r.REGION_ID
        join tr_request_service s
          on s.request_id = r.id
         and s.product_category!=4 
       where r.id = pi_request_id
         and reg.kl_region=pi_kl_region
       group by r.id, r.ADDRESS_ID;
    exception
      when no_data_found then
        po_err_num := 1;
        po_err_msg := 'Не найдена заявка на подключение фиксированных услуг связи';
        return null;
    end;
    if L_req_state = 4 then
      po_err_num := 2;
      po_err_msg := 'Заявка на подключение фиксированных услуг связи удалена';
      return null;
    end if;

    po_req_state := L_req_state;

    if l_addr_id is not null then
      l_addr := addresse_pkg.get_address_type(pi_addr_id => l_addr_id,
                                              po_err_num => po_err_num,
                                              po_err_msg => po_err_msg);
    end if;
  
    open res for
      select r.id,
             reg.kl_region region_id,
             l_addr addr_info,
             p.person_lastname,
             p.person_firstname,
             p.person_middlename,
             p.person_birthday,
             p.birthplace,
             doc.doc_regdate,
             doc.doc_extrainfo,
             doc.doc_type,
             doc.doc_series,
             doc.doc_number,
             s.id serv_id,
             s.product_category,
             s.state_id,
             r.PERSONAL_ACCOUNT,
             s.ACC_NUMBER,
             d.usl_number
        from tr_request r
        join t_dic_region reg 
          on reg.reg_id=r.region_id
        join t_clients cl
          on cl.client_id = r.CLIENT_ID
        join t_person p
          on p.person_id = cl.fullinfo_id
         and cl.client_type = 'P'
        left join t_documents doc
          on doc.doc_id = p.doc_id
        join tr_request_service s 
          on s.request_id =r.id
         and s.state_id not in (13, 14, 15, 16, 22) 
        left join tr_request_service_detail d 
          on d.service_id = s.id
          where r.id=l_id;
      
       return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
  end;
    
  ----------------------------------------------------------------------
  --Получение оборудования по заявкам на подключение
  ----------------------------------------------------------------------
  function get_list_phys_service_device(pi_worker_id        in number,
                                        pi_contractNum      in varchar2,
                                        pi_request_id       in tr_request.request_id%type,
                                        pi_mrf_order_num    in varchar2,
                                        pi_only_mine        in number,
                                        pi_FirstName        in t_person.person_firstname%type,
                                        pi_LastName         in t_person.person_lastname%type,
                                        pi_MiddleName       in t_person.person_middlename%type,
                                        pi_IDCardSeria      in t_documents.doc_series%type,
                                        pi_IDCardNumber     in t_documents.doc_number%type,
                                        pi_ELK_ACCOUNT      in varchar2,
                                        pi_PERSONAL_ACCOUNT in varchar2,
                                        pi_phone            in t_person.person_phone%type,
                                        pi_uslNumber        in tr_request_service_detail.usl_number%type,
                                        pi_mainEquipment    in tr_request_service_detail.main_equipment%type,
                                        ------------
                                        pi_addr_id   in number,
                                        pi_house_id  in number,
                                        pi_house_num in varchar2,
                                        pi_flat_id   in varchar2,
                                        ------------
                                        pi_services          in num_tab,
                                        pi_org_tab           in num_tab,
                                        pi_org_child_include in number,
                                        pi_type_request      in varchar2,
                                        pi_addr_label        in number,
                                        pi_type_operation    in num_tab,
                                        pi_DateTimeBeg       in date,
                                        pi_DateTimeEnd       in date,
                                        pi_sorting_date      in number,
                                        pi_device_id         in number,
                                        pi_device_card_type  in number, -- тип оборудования из Лиры
                                        pi_Channels          in num_tab,
                                        pi_States            in num_tab,
                                        pi_systems           in num_tab,
                                        pi_type_system       in number,
                                        pi_tech_posib        in number,
                                        pi_send              in request_param_type,
                                        pi_tag_id            in number,
                                        pi_opt_tag           in number,
                                        pi_req_type          in number,
                                        ---------
                                        pi_num_page  in number,
                                        pi_count_req in number,
                                        pi_column    in number,
                                        pi_sorting   in number,
                                        po_all_count out number,
                                        po_err_num   out number,
                                        po_err_msg   out varchar2)
    return sys_refcursor is
    l_func_name varchar2(100) := 'request_list.get_list_phys_service_device';

    res            sys_refcursor;
    l_order_asc    number; -- по возрастанияю
    l_order_desc   number; -- по убыванию
    l_max_num_page number;
    l_num_page     number;

    l_org_num_2  array_num_2 := array_num_2();
    l_org_tab    num_tab;
    User_Orgs    Num_Tab;
    User_Orgs2   Num_Tab;
    l_reg_org    ARRAY_NUM_2;
    User_reg_org ARRAY_NUM_2;

    l_callback_view number := 0;
    l_sms_view      number := 0;
    l_date_cr_beg   date;
    l_date_cr_end   date;
    l_date_ch_beg   date;
    l_date_ch_end   date;
    l_tag_tab       num_tab;
    l_service_tab   num_tab := pi_services;

    l_cnt number;

    l_all_req1  num_tab; -- по датам
    l_all_req2  num_tab;
    l_all_req3  num_tab;
    l_all_req4  num_tab;
    l_all_req5  num_tab;
    l_all_req6  num_tab;
    l_all_req7  num_tab;
    l_all_req8  num_tab;
    l_all_req9  num_tab;
    l_all_req10 num_tab;

    l_all_req             num_tab;
    l_col_request         request_Order_Tab;
    l_request_id          number;
    l_filt_callback_hours number;
    l_request_devices     array_num2_str;
  begin

    if pi_org_tab is not null then
      select rec_num_2(number_1 => column_value,
                       number_2 => pi_org_child_include) bulk collect
        into l_org_num_2
        from table(pi_org_tab);
    else
      l_org_num_2 := array_num_2(rec_num_2(1, nvl(pi_org_child_include, 1)));
    end if;

    l_org_tab := get_orgs_tab_for_multiset(pi_orgs            => l_org_num_2,
                                           Pi_worker_id       => pi_worker_id,
                                           pi_block           => 1,
                                           pi_org_relation    => null,
                                           pi_is_rtmob        => 0,
                                           pi_tm_1009_include => 1);

    User_Orgs := SECURITY_PKG.Get_User_Orgs_Tab_By_Right_str(pi_worker_id,
                                                             'SD.REQUEST.PHYS.VIEW_LIST',
                                                             2001269); --оборудование должно выводится только по уралу

    User_Orgs2 := intersects(l_org_tab, User_Orgs);

    l_reg_org := SECURITY_PKG.get_region_by_worker_right2(pi_worker_id => pi_worker_id,
                                                          pi_right_str => string_tab('SD.REQUEST.PHYS.VIEW_LIST'),
                                                          pi_org_id    => l_org_tab);

    User_reg_org := intersect_num2(User_Orgs2, l_reg_org);

    If pi_sorting = 0 then
      l_order_asc := NVL(pi_column, 2);
    else
      l_order_desc := NVL(pi_column, 2);
    end If;

    if pi_DateTimeBeg is not null and nvl(pi_sorting_date, 0) in (0, 3) then
      l_date_cr_beg := pi_DateTimeBeg - Constant_pkg.c_GMT;
    else
      l_date_cr_beg := to_date('01.01.1900', 'dd.mm.yyyy');
    end if;

    if pi_DateTimeEnd is not null and nvl(pi_sorting_date, 0) in (0, 3) then
      l_date_cr_end := pi_DateTimeEnd - Constant_pkg.c_GMT;
    else
      l_date_cr_end := to_date('31.12.2999', 'dd.mm.yyyy');
    end if;

    if pi_DateTimeBeg is not null and nvl(pi_sorting_date, 0) in (1, 2) then
      l_date_ch_beg := pi_DateTimeBeg - Constant_pkg.c_GMT;
    else
      l_date_ch_beg := to_date('01.01.1900', 'dd.mm.yyyy');
    end if;

    if pi_DateTimeEnd is not null and nvl(pi_sorting_date, 0) in (1, 2) then
      l_date_ch_end := pi_DateTimeEnd - Constant_pkg.c_GMT;
    else
      l_date_ch_end := to_date('31.12.2999', 'dd.mm.yyyy');
    end if;

    if (pi_DateTimeBeg is not null or pi_DateTimeEnd is not null) then
      if pi_sorting_date = 0 then
        select /*+ index(p IDX_REQUEST_SERVICE_DATCR_F) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_create_fact >= l_date_cr_beg
           and p.date_create_fact < l_date_cr_end;

      elsif pi_sorting_date = 1 then
        select /*+ index(p IDX_REQUEST_SERVICE_DATCH_F) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_change_fact >= l_date_ch_beg
           and p.date_change_fact < l_date_ch_end;

      elsif pi_sorting_date = 2 then
        --по дате перехода в текущее состояние
        select /*+ index(p IDX_TR_PRODUCT_DCHST) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_change_state >= l_date_ch_beg
           and p.date_change_state < l_date_ch_end;
      elsif pi_sorting_date = 3 then
        select /*+ index(p IDX_TR_PRODUCT_DCR) */
         p.id bulk collect
          into l_all_req1
          from tr_request_service p
         where p.date_create >= l_date_cr_beg
           and p.date_create < l_date_cr_end;
      end if;
    end if;

    select count(*)
      into l_cnt
      from table(pi_States) s
     where s.column_value in (17, 18, 20, 21, 22, 23);

    --определим показывать коллбак или нет
    -- состояние колбек или метка обратный звонок
    if l_cnt > 0 or nvl(pi_type_request, '0') = 'callback_request' then
      l_callback_view := 1;
    end if;

    if l_cnt > 0 or nvl(pi_type_request, '0') = 'sms_request' then
      l_sms_view := 1;
      l_service_tab.extend;
      l_service_tab(l_service_tab.count) := 13;
    end if;

    --Топ городов
    if pi_addr_label is not null then
      select distinct p.id bulk collect
        into l_all_req9
        from table(l_all_req1) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request r
          on r.id = p.request_id
        join t_address a
          on a.addr_id = r.address_id
        join (select ao.id
                from t_address_object ao
              connect by prior ao.id = ao.parent_id
               start with ao.id in
                          (select al.local_id
                             from t_addr_label al
                            where al.lable_id = pi_addr_label)) aa
          on aa.id = a.addr_obj_id;
      l_all_req1 := l_all_req9;
    end if;

    if (pi_FirstName is not null or pi_LastName is not null or
       pi_MiddleName is not null or pi_phone is not null) then
      select id bulk collect
        into l_all_req2
        from (select t.id
                from (select p.id,
                             pp.person_firstname,
                             pp.person_lastname,
                             pp.person_middlename,
                             pp.person_phone,
                             pp.person_home_phone
                        from tr_request r
                        join tr_request_service p
                          on r.id = p.request_id
                        join t_clients cl
                          on cl.client_id = r.client_id
                         and cl.client_type = 'P'
                        join t_clients p_cl
                          on p_cl.client_id = r.contact_person_id
                         and p_cl.client_type = 'P'
                        join t_person pp
                          on p_cl.fullinfo_id = pp.person_id
                       where lower(pp.person_firstname) =
                             lower(trim(pi_FirstName))
                          or lower(pp.person_lastname) =
                             lower(trim(pi_LastName))
                          or lower(pp.person_middlename) =
                             lower(trim(pi_MiddleName))
                          or pp.person_phone = pi_phone
                          or pp.person_home_phone = pi_phone) t
               where (pi_FirstName is null or
                     lower(t.person_firstname) = lower(trim(pi_FirstName)))
                 and (pi_LastName is null or
                     lower(t.person_lastname) = lower(trim(pi_LastName)))
                 and (pi_MiddleName is null or lower(t.person_middlename) =
                     lower(trim(pi_MiddleName)))
                 and (pi_phone is null or t.person_phone = pi_phone or
                     t.person_home_phone = pi_phone)
              union
              select t.id
                from (select p.id,
                             pp.person_firstname,
                             pp.person_lastname,
                             pp.person_middlename,
                             pp.person_phone,
                             pp.person_home_phone
                        from tr_request r
                        join tr_request_service p
                          on r.id = p.request_id
                        join t_clients cl
                          on cl.client_id = r.client_id
                         and cl.client_type = 'P'
                        join t_person pp
                          on cl.fullinfo_id = pp.person_id
                       where lower(pp.person_firstname) =
                             lower(trim(pi_FirstName))
                          or lower(pp.person_lastname) =
                             lower(trim(pi_LastName))
                          or lower(pp.person_middlename) =
                             lower(trim(pi_MiddleName))) t
               where (pi_FirstName is null or
                     lower(t.person_firstname) = lower(trim(pi_FirstName)))
                 and (pi_LastName is null or
                     lower(t.person_lastname) = lower(trim(pi_LastName)))
                 and (pi_MiddleName is null or lower(t.person_middlename) =
                     lower(trim(pi_MiddleName))));
    end if;

    if pi_request_id is not null then
      begin
        l_request_id := to_number(pi_request_id);
        select s.id bulk collect
          into l_all_req10
          from tr_request_service s
          join tr_request r
            on r.id = s.request_id
         where r.id = l_request_id
            or s.id = l_request_id;
      exception
        when others then
          select s.id bulk collect
            into l_all_req10
            from tr_request_service s
            join tr_request r
              on r.id = s.request_id
           where r.request_id = pi_request_id;
      end;
    end if;

    if (pi_contractNum is not null or pi_mrf_order_num is not null or
       pi_ELK_ACCOUNT is not null or pi_PERSONAL_ACCOUNT is not null) then
      select t.id bulk collect
        into l_all_req3
        from (select p.id,
                     r.request_id,
                     r.dogovor_number,
                     p.mrf_order_num,
                     r.elk_account,
                     r.personal_account
                from tr_request_service p
                join tr_request r
                  on r.id = p.request_id
               where r.dogovor_number = pi_contractNum
                  or p.mrf_order_num = pi_mrf_order_num
                  or r.elk_account = pi_ELK_ACCOUNT
                  or r.personal_account = pi_PERSONAL_ACCOUNT) t
       where (t.dogovor_number = pi_contractNum or pi_contractNum is null)
         and (t.mrf_order_num = pi_mrf_order_num or
             pi_mrf_order_num is null)
         and (t.elk_account = pi_ELK_ACCOUNT or pi_ELK_ACCOUNT is null)
         and (t.personal_account = pi_PERSONAL_ACCOUNT or
             pi_PERSONAL_ACCOUNT is null);
    end if;

    if (pi_IDCardSeria is not null or pi_IDCardNumber is not null) then
      select p.id bulk collect
        into l_all_req5
        from tr_request r
        join tr_request_service p
          on r.id = p.request_id
        join t_system s
          on r.system_id = s.id
        join t_clients cl
          on cl.client_id = r.client_id
         and cl.client_type = 'P'
        join t_person pp
          on cl.fullinfo_id = pp.person_id
        join t_documents d
          on d.doc_id = pp.doc_id
       where d.doc_series = upper(trim(pi_IDCardSeria))
         and d.doc_number = upper(trim(pi_IDCardNumber));
    end if;

    if (pi_addr_id is not null and
       (pi_house_id is not null or pi_house_num is not null)) then
      select p.id bulk collect
        into l_all_req4
        from (select r.id
                from tr_request r
                join t_address a
                  on r.address_id = a.addr_id
               where a.addr_obj_id = pi_addr_id
                 and (pi_house_id is null or
                     pi_house_id = a.addr_house_obj_id)
                 and (pi_house_num is null or
                     trim(pi_house_num) = a.addr_building)
                 and (pi_flat_id is null or trim(pi_flat_id) = a.addr_office)) t
        join tr_request_service p
          on p.request_id = t.id;
    end if;

    -- фильтр по меткам
    if nvl(pi_type_request, '0') != '0' then
      select distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req1) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_params par
          on par.request_id = p.request_id
         and par.key = pi_type_request
         and nvl(par.value, '0') = '1'
       where l_callback_view = 1
          or l_sms_view = 1
          or p.state_id not in (17, 18, 20, 21, 22, 23);
    elsif pi_type_request is null and l_all_req1 is not null then
      --нам без разницы какие метки, главное убрать колбак, если он не нужен
      if l_callback_view = 1 and l_sms_view = 1 then
        l_all_req8 := l_all_req1;
      elsif l_sms_view = 1 and l_callback_view = 0 then
        --исключим колбек в статусах колбек
        select distinct p.id bulk collect
          into l_all_req8
          from table(l_all_req1) req
          join tr_request_service p
            on p.id = req.column_value
          left join tr_request_params par
            on par.request_id = p.request_id
           and par.key = 'callback_request'
           and p.state_id in (17, 18, 20, 21, 22, 23)
         where nvl(par.value, 0) = 0;
      elsif l_sms_view = 0 and l_callback_view = 1 then
        --исключим смс в статусах колбек
        select distinct p.id bulk collect
          into l_all_req8
          from table(l_all_req1) req
          join tr_request_service p
            on p.id = req.column_value
          left join tr_request_params par
            on par.request_id = p.request_id
           and par.key = 'sms_request'
           and p.state_id in (17, 18, 20, 21, 22, 23)
         where nvl(par.value, 0) = 0;
      else
        select /*+ use_nl(rc req) */
        distinct p.id bulk collect
          into l_all_req8
          from table(l_all_req1) req
          join tr_request_service p
            on p.id = req.column_value
         where p.state_id not in (17, 18, 20, 21, 22, 23);
      end if;
    elsif pi_type_request = '0' then
      select distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req1) req
        join tr_request_service p
          on p.id = req.column_value
        left join tr_request_params par
          on par.request_id = p.request_id
         and nvl(par.value, '0') = '1'
         and par.key in ('connect_friend',
                         'anketa_mpz',
                         'telemarketing',
                         'callback_request',
                         'sms_request',
                         'is_quick_request',
                         'elk_request',
                         'address_is_not_directory')
       where (l_callback_view = 1 or
             l_sms_view = 1 and p.state_id not in (17, 18, 20, 21, 22, 23))
         and nvl(par.value, '0') = '0';
    end if;

    --Фильтр по уже подключенным услугам
    if nvl(pi_tech_posib, 0) = 3 then
      select distinct s.id bulk collect
        into l_all_req7
        from table(l_all_req8) req
        join tr_request_service s
          on req.column_value = s.id
        join TR_REQUEST_SERV_EXIST e
          on e.request_id = s.request_id
         and e.service_type = s.product_category
       where nvl(e.is_old_address, 0) = 0;
    end if;

    --Фильтр по проверке ТВ
    if nvl(pi_tech_posib, 0) in (1, 2, 4, 5) then
      select distinct aa.id bulk collect
        into l_all_req7
        from (select /* ordered use_nl(req rc) use_nl(rc th) */
               p.id, max(th.is_success) tech_posib
                from table(l_all_req8) req
                join tr_request_service p
                  on req.column_value = p.id
                join tr_request r
                  on r.id = p.request_id
                left join tr_request_tech_poss th
                  on th.request_id = p.request_id
                 and th.service_type = p.product_category
                 and th.address_id = r.address_id
                 and nvl(th.tech_id, 0) = nvl(p.tech_id, nvl(th.tech_id, 0))
               group by p.id) aa
       where nvl(aa.tech_posib, -1) =
             decode(pi_tech_posib, 1, 1, 2, 0, 4, -1, 5, 5);
    end if;

    if pi_uslNumber is not null or pi_mainEquipment is not null then
      select distinct s.service_id bulk collect
        into l_all_req6
        from tr_request_service_detail s
       where (s.main_equipment = pi_mainEquipment or
             pi_mainEquipment is null)
         and (s.usl_number = pi_uslNumber or pi_uslNumber is null);
    end if;

    if l_all_req7 is null then
      l_all_req := intersects(l_all_req, l_all_req8);
    else
      l_all_req := intersects(l_all_req, l_all_req7);
    end if;
    l_all_req := intersects(l_all_req, l_all_req2);
    l_all_req := intersects(l_all_req, l_all_req3);
    l_all_req := intersects(l_all_req, l_all_req10);
    l_all_req := intersects(l_all_req, l_all_req4);
    l_all_req := intersects(l_all_req, l_all_req5);
    l_all_req := intersects(l_all_req, l_all_req6);

    if pi_send is not null and pi_send.key is not null then
      --Обработка заявок сторонними системами
      l_all_req8 := null;
      select distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_params par
          on par.request_id = p.request_id
         and par.key = pi_send.key
       where par.value = pi_send.value;

      l_all_req  := l_all_req8;
      l_all_req8 := null;
    end if;

    --фильтр по тегам опций
    if pi_opt_tag is not null then
      l_all_req8 := null;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_product_option t
          on t.service_id = req.column_value
        join t_opt_tag opt
          on opt.option_id = t.option_id
       where opt.tag_id = pi_opt_tag;

      l_all_req  := l_all_req8;
      l_all_req8 := null;
    end if;

    -- фильтр по оборудованию из ЕИССД
    if pi_device_id is not null then
      l_all_req8 := null;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_device t
          on t.request_id = p.request_id
       where t.device_type = pi_device_id
         and not exists (select ord.orders_id
                from tr_request_orders ord
                join t_orders o
                  on ord.orders_id = o.id
               where o.order_type = 5
                 and ord.request_id = p.request_id);
      l_all_req  := l_all_req8;
      l_all_req8 := null;
    end if;
    -- по типам оборудование из Лиры
    if pi_device_card_type is not null then
      l_all_req8 := null;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
        join tr_request_orders ro
          on ro.request_id = p.request_id
        join t_orders o
          on o.id = ro.orders_id
         and o.order_type = 5
        join tr_request_device_card rdc
          on rdc.request_id = p.request_id
       where rdc.type_equipment = pi_device_card_type
         and rdc.is_exists = 0;

      l_all_req  := l_all_req8;
      l_all_req8 := null;
    end if;

    if pi_type_request is null and l_callback_view = 0 and l_sms_view = 0 and
       l_all_req1 is null then
      --метки не были переданы, значит  по параметрам клиента.
      --Нужно убрать колбак статусы, раз они не нужен

      select /*+ use_nl(rc req) */
      distinct p.id bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service p
          on p.id = req.column_value
       where p.state_id not in (17, 18, 20, 21, 22, 23);

      l_all_req := l_all_req8;
    end if;

    --подберем тарифы по тегам
    if pi_tag_id is not null then
      select t.id bulk collect
        into l_tag_tab
        from t_dic_tag t
      connect by t.parent_id = prior t.id
       start with t.id = pi_tag_id;

      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_service_product ps
          on ps.service_id = req.column_value
        join tr_request_product p
          on p.id = ps.product_id
        join t_version_tag vt
          on vt.object_id = p.tar_id
       where vt.tag_id in (select column_value from table(l_tag_tab));

      l_all_req := l_all_req8;
    end if;
    --
    if pi_req_type is not null then
      l_all_req8 := null;
      if pi_req_type = 3 then
        l_filt_callback_hours := constant_pkg.c_filt_callback_hours;
      end if;
      select distinct req.column_value bulk collect
        into l_all_req8
        from table(l_all_req) req
        join tr_request_service rs
          on rs.id = req.column_value
        join tr_request r
          on r.id = rs.request_id
       where (pi_req_type = 1 and r.instwishtimecall2 < sysdate)
          or (pi_req_type = 2 and r.instwishtimecall1 < sysdate and
             r.instwishtimecall2 > sysdate)
          or (pi_req_type = 3 and r.instwishtimecall1 between sysdate and
             sysdate + l_filt_callback_hours / 24)
          or (pi_req_type = 4 and r.instwishtimecall1 > sysdate)
          or (pi_req_type = 5 and r.instwishtimecall1 is null);
      l_all_req := l_all_req8;
    end if;

    --/*+ ordered */
    select request_Order_Type(id, rownum) bulk collect
      into l_col_request
      from (select /*+ ordered */
             p.id
              from (select /*+ use_nl(p xxx) */
                     p.*
                      from table(l_all_req) xxx
                      join tr_request_service p
                        on p.id = xxx.column_value) p
              join tr_request r
                on r.id = p.request_id
              join table(User_reg_org) reg_org
                on reg_org.number_1 = r.org_id
               and nvl(r.region_id, '-1') =
                   nvl(reg_org.number_2, nvl(r.region_id, -1))
              join t_system s
                on r.system_id = s.id
              join table(pi_systems) ss
                on ss.column_value = r.system_id
              join Table(pi_Channels) ch
                on ch.column_value = r.channel_id
              join table(l_service_tab) serv
                on serv.column_value = p.product_category
              join t_clients cl
                on cl.client_id = r.client_id
               and cl.client_type = 'P'
              join t_clients p_cl
                on p_cl.client_id = r.contact_person_id
               and p_cl.client_type = 'P'
              join t_person pp
                on p_cl.fullinfo_id = pp.person_id
              left join t_address a
                on r.address_id = a.addr_id
              left join t_dic_region dr
                on dr.reg_id = r.region_id
              left join t_dic_mrf dm
                on dm.id = dr.mrf_id
              left join t_Organizations orgM
                on dm.org_id = orgM.Org_id
              left join t_Organizations orgF
                on dr.org_id = orgf.Org_id
              join t_dic_request_state drs
                on p.state_id = drs.state_id
              join table(pi_States) stt
                on stt.column_value = drs.state_id
             where (pi_type_system = 2 or (s.is_federal = pi_type_system))
               and ((nvl(pi_only_mine, 0) = 1 and
                   p.worker_create = pi_worker_id) or
                   nvl(pi_only_mine, 0) <> 1)
                  --and (r.system_id in (select * from table(pi_systems)))
               and p.state_id not in (24)
               and p.type_request in
                   (select * from table(pi_type_operation))
             order by decode(l_order_asc,
                             9,
                             (case
                               when r.instwishtimecall1 > sysdate then
                                4
                               when sysdate between r.instwishtimecall1 and
                                    r.instwishtimecall2 then
                                2
                               when r.instwishtimecall1 is null then
                                3
                               else
                                1
                             end),
                             null) asc,
                      decode(l_order_asc,
                             null,
                             null,
                             1,
                             lpad(r.request_id, 13, '0'),
                             2,
                             to_char(p.date_create_fact,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             3,
                             lower(pp.person_lastname || ' ' ||
                                   pp.person_firstname || ' ' ||
                                   pp.person_middlename),
                             4,
                             nvl2(a.addr_oth,
                                  a.addr_oth || ' ' || a.addr_building || '-' ||
                                  a.addr_office,
                                  substr(a.addr_city,
                                         0,
                                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                                  a.addr_street || ' ' || a.addr_building || '-' ||
                                  a.addr_office),
                             5,
                             drs.state_name,
                             6,
                             to_char(p.date_change_state,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             8,
                             upper(orgM.Org_Name || orgf.org_name),
                             9,
                             case
                               when nvl(r.instwishtimecall1, sysdate) > sysdate then
                                to_char(nvl(r.instwishtimecall1, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                               else
                                to_char(nvl(r.instwishtimecall2, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                             end,
                             null) asc,
                      decode(l_order_desc,
                             9,
                             (case
                               when r.instwishtimecall1 > sysdate then
                                4
                               when sysdate between r.instwishtimecall1 and
                                    r.instwishtimecall2 then
                                2
                               when r.instwishtimecall1 is null then
                                3
                               else
                                1
                             end),
                             null) desc,
                      decode(l_order_desc,
                             null,
                             null,
                             1,
                             lpad(r.request_id, 13, '0'),
                             2,
                             to_char(p.date_create_fact,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             3,
                             lower(pp.person_lastname || ' ' ||
                                   pp.person_firstname || ' ' ||
                                   pp.person_middlename),
                             4,
                             nvl2(a.addr_oth,
                                  a.addr_oth || ' ' || a.addr_building || '-' ||
                                  a.addr_office,
                                  substr(a.addr_city,
                                         0,
                                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                                  a.addr_street || ' ' || a.addr_building || '-' ||
                                  a.addr_office),
                             5,
                             drs.state_name,
                             6,
                             to_char(p.date_change_state,
                                     'yyyy.mm.dd hh24:mi:ss'),
                             8,
                             upper(orgM.Org_Name || orgf.org_name),
                             9,
                             case
                               when nvl(r.instwishtimecall1, sysdate) > sysdate then
                                to_char(nvl(r.instwishtimecall1, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                               else
                                to_char(nvl(r.instwishtimecall2, sysdate),
                                        'yyyy.mm.dd hh24:mi:ss')
                             end,
                             null) desc);

    po_all_count := l_col_request.count;

    l_max_num_page := round(po_all_count / nvl(pi_count_req, 1));

    if (pi_num_page > l_max_num_page and l_max_num_page <> 0) then
      l_num_page := l_max_num_page + 1;
    else
      l_num_page := nvl(pi_num_page, 1);
    end if;

    open res for
      select null service_id,
             tt.request_id,
             null product_category,
             null tech_id,
             null date_create,
             r.channel_id,
             pp.person_lastname || ' ' || pp.person_firstname || ' ' ||
             pp.person_middlename as fio,
             nvl2(aa.addr_oth,
                  aa.addr_oth || nvl2(aa.addr_building,
                                      ', д. ' || aa.addr_building,
                                      aa.addr_building) ||
                  nvl2(aa.addr_office,
                       ', кв. ' || aa.addr_office,
                       aa.addr_office),
                  substr(aa.addr_city,
                         0,
                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                  aa.addr_street || nvl2(aa.addr_building,
                                         ', д. ' || aa.addr_building,
                                         aa.addr_building) ||
                  nvl2(aa.addr_office,
                       ', кв. ' || aa.addr_office,
                       aa.addr_office)) adr,
             null State,
             null date_change_state,

             nvl(orgM.Org_Name, o_rtk.org_name) mrf,
             nvl(orgf.org_name, o_rtk.org_name) filial,
              u.usr_login as create_user,

             oo.org_name,
             pp.person_phone,
             pp.person_home_phone,
             null mrf_order_num,

             dd.name device_name,
             null model_name,
             null state_device,
             sc.name schema_sale_name,
             d.device_count,
             tag.tag_name action_name,
             nvl(sv.cost, 0) + nvl(sv.fee, 0) cost,
             null SERIAL_NUMBER,
             null NSTB,
             null user_card_req,
             null org_card_req,
             null user_card_dev,
             null org_card_dev
        from (select s.request_id, min(rn) rn
                from (Select request_id, rn
                        from table(l_col_request)
                       where rn between (l_num_page - 1) * pi_count_req + 1 and
                             (l_num_page) * pi_count_req) tt
                join tr_request_service s
                  on s.id = tt.request_id
               group by s.request_id) tt
        join tr_request r
          on tt.request_id = r.id
        join t_address aa
          on aa.addr_id = r.ADDRESS_ID
        join t_clients cl
          on cl.client_id = r.CLIENT_ID
        join t_person pp
          on pp.person_id = cl.fullinfo_id
        join tr_request_device d
          on d.request_id = tt.request_id
        join t_device dd
          on dd.id = d.device_type
        join t_eqipment_sale_schema sc
          on sc.id = d.device_use_scheme
        left join t_dic_tag tag
          on tag.id = d.action_id
        left join t_device_price_vers v
          on v.device_hst = d.price_vers
         and v.device_id = d.device_type
         and nvl(v.action_id, -1) = nvl(d.action_id, -1)
         and v.region_id = r.region_id
         and v.channel_id = r.channel_id
        left join t_device_price_sale_vers sv
          on sv.price_id = v.id
         and sv.type_sale = d.device_use_scheme
        left join t_dic_region dr
          on dr.reg_id = r.region_id
        left join t_dic_mrf dm
          on dm.id = dr.mrf_id
        left join t_Organizations orgM
          on dm.org_id = orgM.Org_id
        left join t_Organizations orgF
          on dr.org_id = orgf.Org_id
        left join t_organizations o_rtk
          on o_rtk.org_id = 0
        join t_organizations oo
          on oo.org_id = r.org_id
        join t_users u
          on u.usr_id = r.worker_create
      union
      select s.id service_id,
             s.request_id request_id,
             s.product_category,
             s.tech_id,
             s.date_create_fact date_create,
             r. channel_id,
             pp.person_lastname || ' ' || pp.person_firstname || ' ' ||
             pp.person_middlename as fio,
             nvl2(aa.addr_oth,
                  aa.addr_oth || nvl2(aa.addr_building,
                                      ', д. ' || aa.addr_building,
                                      aa.addr_building) ||
                  nvl2(aa.addr_office,
                       ', кв. ' || aa.addr_office,
                       aa.addr_office),
                  substr(aa.addr_city,
                         0,
                         Constant_pkg.c_adress_city_name_length) || ' ' ||
                  aa.addr_street || nvl2(aa.addr_building,
                                         ', д. ' || aa.addr_building,
                                         aa.addr_building) ||
                  nvl2(aa.addr_office,
                       ', кв. ' || aa.addr_office,
                       aa.addr_office)) adr,
             drs.state_name State,
             s.date_change_state,

             nvl(orgM.Org_Name, o_rtk.org_name) mrf,
             nvl(orgf.org_name, o_rtk.org_name) filial,
              u.usr_login as create_user,

             oo.org_name,
             pp.person_phone,
             pp.person_home_phone,
             s.mrf_order_num,

             d_e.name         device_name,
             m_e.name         model_name,
             cd.state         state_device, --состояние
             d_o.name         schema_sale_name,
             cd.quant         device_count,
             null             action_name,
             cd.cost,
             cd.SERIAL_NUMBER,
             cd.NSTB,
             u_cr.usr_login   user_card_req,
             o_cr.org_name    org_card_req,
             u_cd.usr_login   user_card_dev,
             o_cd.org_name    org_card_dev

        from (Select request_id, rn
                from table(l_col_request)
               where rn between (l_num_page - 1) * pi_count_req + 1 and
                     (l_num_page) * pi_count_req) tt
        join tr_request_service s
          on s.id = tt.request_id

        join tr_request r
          on s.request_id = r.id
        join t_address aa
          on aa.addr_id = r.ADDRESS_ID
        join t_clients cl
          on cl.client_id = r.CLIENT_ID
        join t_person pp
          on pp.person_id = cl.fullinfo_id
        left join tr_request_device_card cd
          on cd.request_id = s.request_id
         and cd.is_exists = 0
         and cd.service_id = s.id
        left join t_lira_type_equipment d_e
          on d_e.id = cd.type_equipment
        left join t_lira_model_equipment m_e
          on m_e.id = cd.model_equipment
        left join t_lira_type_operations d_o
          on d_o.id = cd.op_type_id
        left join t_users u_cr
          on u_cr.usr_id = cd.user_card_req
        left join t_organizations o_cr
          on o_cr.org_id = cd.org_card_req
        left join t_users u_cd
          on u_cd.usr_id = cd.user_card_device
        left join t_organizations o_cd
          on o_cd.org_id = cd.org_card_device

        left join t_dic_region dr
          on dr.reg_id = r.region_id
        left join t_dic_mrf dm
          on dm.id = dr.mrf_id
        left join t_Organizations orgM
          on dm.org_id = orgM.Org_id
        left join t_Organizations orgF
          on dr.org_id = orgf.Org_id
        left join t_organizations o_rtk
          on o_rtk.org_id = 0
        join t_organizations oo
          on oo.org_id = r.org_id
        left join t_dic_request_state drs
          on s.state_id = drs.state_id
        join t_users u
          on u.usr_id = r.worker_create;

    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;
end request_list;
/