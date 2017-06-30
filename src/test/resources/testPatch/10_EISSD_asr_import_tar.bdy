CREATE OR REPLACE PACKAGE BODY ASR_IMPORT_TAR is

  c_package constant varchar2(30) := 'ASR_IMPORT_TAR.';
  ----- типы для импорта тарифных планов
  -- tariff
  type rec_tar is record(
    tar_id             number,
    region_id          number,
    title              varchar2(255),
    startdate          date,
    is_for_dealer      number,
    category           number,
    is_active          number,
    tar_type           number,
    pay_type           number,
    vdvd_type          number,
    tar_md5            varchar(32),
    phone_federal      number,
    phone_color        number,
    cost               number,
    advance            number,
    tech_tar           number,
    equipment_required number);
  -- service
  type rec_srv is record(
    srv_id          number,
    srv_name        varchar2(32),
    srv_description varchar2(255),
    srv_type        number,
    P_SERVICE       VARCHAR2(255),
    P_SERVICE_DVO   NUMBER,
    TAR_ID          NUMBER,
    REGION_ID       NUMBER);
  -- service_cost
  type rec_srv_c is record(
    srv_id        number,
    tar_id        number,
    region_id     number,
    cost          number,
    advance       number,
    category      number,
    is_enable     number,
    is_on         number,
    startdate     date,
    group_id      number,
    phone_federal number,
    phone_color   number,
    PACK_ID       NUMBER,
    DISC_ID       NUMBER,
    TYPE_TARIFF   NUMBER,
    TYPE          NUMBER);
  -- service_group
  type rec_srv_g is record(
    group_id      number,
    group_name    varchar(64),
    group_type    number,
    is_necessary  number,
    is_single_pay number);
  -- rel_service_service
  type rec_srv_r is record(
    srv_id_1  number,
    srv_id_2  number,
    tar_id    number,
    region_id number,
    rel_type  number,
    PACK_ID_1 NUMBER,
    PACK_ID_2 NUMBER);
  type rec_tar_srv is record(
    srv_id        number,
    srv_name      varchar(32),
    srv_descr     varchar2(255),
    srv_type      number,
    cost          number,
    advance       number,
    category      number,
    is_enable     number,
    is_on         number,
    startdate     date,
    group_id      number,
    group_name    varchar(64),
    group_type    number,
    is_necessary  number,
    is_single_pay number,
    phone_federal number,
    phone_color   number);
  -- Пакеты
  type rec_pack is record(
    PACK_ID   NUMBER,
    PACK_NAME VARCHAR2(255),
    P_SERVICE VARCHAR2(255),
    TAR_ID    NUMBER,
    REGION_ID NUMBER);
  -- Скидки
  type rec_discount is record(
    DISC_ID   NUMBER,
    DISC_NAME VARCHAR2(255),
    P_SERVICE VARCHAR2(255),
    PACK_ID   NUMBER,
    TAR_ID    NUMBER,
    REGION_ID NUMBER);
  -- Ограничения
  type rec_restrict_ab is record(
    SRV_ID    NUMBER,
    PACK_ID   NUMBER,
    DISC_ID   NUMBER,
    IS_ACTION NUMBER);

  -- типы оплаты по тарифному плану
  -- авансовый
  c_tar_pay_type_adv_prot constant number := 0;
  c_tar_pay_type_adv_dic  constant number := 71;
  -- кредитный
  c_tar_pay_type_cr_prot constant number := 1;
  c_tar_pay_type_cr_dic  constant number := 72;

  -- типы тарифных планов
  -- prepaid
  c_tar_type_prepaid_prot constant number := 0;
  c_tar_type_prepaid_dic  constant number := 61;
  -- postpaid
  c_tar_type_postpaid_prot constant number := 1;
  c_tar_type_postpaid_dic  constant number := 62;

  -- типы подключений
  -- GSM
  c_tar_vdvd_type_gsm_prot constant number := 1;
  c_tar_vdvd_type_gsm_dic  constant number := 51;
  -- PSTN
  c_tar_vdvd_type_pstn_prot constant number := 2;
  c_tar_vdvd_type_pstn_dic  constant number := 52;
  -- CDMA
  c_tar_vdvd_type_cdma_prot constant number := 3;
  c_tar_vdvd_type_cdma_dic  constant number := 53;
  -- NMT
  c_tar_vdvd_type_nmt_prot constant number := 4;
  c_tar_vdvd_type_nmt_dic  constant number := 54;
  -- ADSL
  c_tar_vdvd_type_adsl_prot constant number := 5;
  c_tar_vdvd_type_adsl_dic  constant number := 56;

  -- типы связей между услугами
  -- parent
  c_serv_rel_type_par_prot constant number := 0;
  c_serv_rel_type_par_dic  constant number := 81;
  -- enemy
  c_serv_rel_type_enem_prot constant number := 1;
  c_serv_rel_type_enem_dic  constant number := 82;
  -- both
  c_serv_rel_type_both_prot constant number := 2;
  c_serv_rel_type_both_dic  constant number := 83;

  -- типы телефонного номера
  -- федеральный
  c_phone_federal_prot constant number := 1;
  c_phone_federal_dic  constant number := 9001;
  -- городской
  c_phone_city_prot constant number := 2;
  c_phone_city_dic  constant number := 9002;

  -- типы привилегированности номера
 /* --простой
  c_phone_color_simple_prot constant number := 1;
  c_phone_color_simple_dic  constant number := 9101;
  -- серебряный
  c_phone_color_silver_prot constant number := 2;
  c_phone_color_silver_dic  constant number := 9104;
  -- золотой
  c_phone_color_gold_prot constant number := 3;
  c_phone_color_gold_dic  constant number := 9102;
  -- платиновый
  c_phone_color_plat_prot constant number := 4;
  c_phone_color_plat_dic  constant number := 9103;*/

  -- Имя таблицы сессий импорта
  subtype t_table_nm is varchar2(40);
  subtype t_md5 is varchar2(32);

  subtype t_sql is varchar2(8000);

  g_tar_tbl_nm           t_table_nm;
  g_srv_tbl_nm           t_table_nm;
  g_srv_c_tbl_nm         t_table_nm;
  g_srv_g_tbl_nm         t_table_nm;
  g_srv_r_tbl_nm         t_table_nm;
  g_eissd_package_tbl_nm t_table_nm;
  g_discount_tbl_nm      t_table_nm;
  g_restrict_ab_tbl_nm   t_table_nm;

  g_tar_tbl_nm2           t_table_nm;
  g_srv_tbl_nm2           t_table_nm;
  g_srv_c_tbl_nm2         t_table_nm;
  g_srv_g_tbl_nm2         t_table_nm;
  g_srv_r_tbl_nm2         t_table_nm;
  g_eissd_package_tbl_nm2 t_table_nm;
  g_discount_tbl_nm2      t_table_nm;
  g_restrict_ab_tbl_nm2   t_table_nm;
  --==============================================================================
  -- Формирует полные имена DPA-таблиц
  procedure Set_Dpa_Tables_Names(pi_asr_id in T_ASR.ASR_ID%type) is
  begin
    g_tar_tbl_nm            := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'tariff');
    g_srv_tbl_nm            := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'service');
    g_srv_g_tbl_nm          := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'service_group');
    g_srv_c_tbl_nm          := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'service_cost');
    g_srv_r_tbl_nm          := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'rel_service_service');
    g_eissd_package_tbl_nm  := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'eissd_package');
    g_discount_tbl_nm       := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'discount');
    g_restrict_ab_tbl_nm    := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'restrict_ability');
    g_tar_tbl_nm2           := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'tariff2');
    g_srv_tbl_nm2           := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'service2');
    g_srv_g_tbl_nm2         := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'service_group2');
    g_srv_c_tbl_nm2         := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'service_cost2');
    g_srv_r_tbl_nm2         := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'rel_srv_srv2');
    g_eissd_package_tbl_nm2 := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'eissd_package2');
    g_discount_tbl_nm2      := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'discount2');
    g_restrict_ab_tbl_nm2   := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                                'restrict_ability2');
  end Set_Dpa_Tables_Names;
  --------------------------------------------------------------------------------------
  -- Создание или обновление услуг для подключения городских и цветных номеров (для Питер-сервиса)
  procedure Set_City_Color_ServicesPS(pi_tar_remote_id in number,
                                      pi_asr_id        in number,
                                      pi_region_id     in number) is
    type rec_tar_cost is record(
      startdate     date,
      category      number,
      phone_federal number,
      phone_color   number,
      cost          number,
      advance       number);
    type tp_tar_tab is table of rec_tar_cost;
    l_tar_tab      tp_tar_tab;
    l_phone_types  num_tab := num_tab(9001, 9002);
    --l_phone_colors num_tab := num_tab(9101, 9104, 9102, 9103);
    l_pfederal     number;
    l_pcolor       number;
    l_cost         number;
    l_advance      number;
    l_serv_id      number;
    sql_text       varchar2(1000);
  begin
    if pi_tar_remote_id is null or pi_asr_id is null or
       pi_region_id is null then
      return;
    end if;
    -- формируем массив вариантов тарифного плана для различных типов номеров
    sql_text := 'select startdate, nvl(category, 2), phone_federal, phone_color, cost, advance from ' ||
                g_tar_tbl_nm || '
         where tar_id = :pi_tar_remote_id
           and region_id = :pi_region_id
         order by phone_federal, phone_color nulls first';
    execute immediate sql_text bulk collect
      into l_tar_tab
      using pi_tar_remote_id, pi_region_id;
    for i in l_tar_tab.first .. l_tar_tab.last loop
      l_pfederal := nvl(l_tar_tab(i).phone_federal, 1);
      --l_pcolor   := nvl(l_tar_tab(i).phone_color, 1);
      begin
        select c.id
          into l_pcolor
          from t_dic_sim_color c
         where c.ps_id = l_tar_tab(i).phone_color;
      exception
        when no_data_found then
          l_pcolor:=tmc_sim.get_color_default;
      end;    
    
      begin
        -- Проверяем наличие виртуальной услуги
        select serv_id
          into l_serv_id
          from t_service2 s
         where s.serv_asr_id = pi_asr_id
           and s.serv_type =
               (l_phone_types(l_pfederal) || l_pcolor);--l_phone_colors(l_pcolor));
      exception
        when no_data_found then
          -- услуга еще не добавлена - добавляем в справочник услуг
          insert into t_service2
            (remote_serv_id,
             serv_asr_id,
             serv_name,
             serv_desc,
             serv_type,
             serv_ver_num,
             serv_imp_id)
          values
            (-1,
             pi_asr_id,
             'Виртуальная услуга',
             'Эмуляция услуги предоставления доступа к сети для ПС',
             l_phone_types(l_pfederal) || l_pcolor,--l_phone_colors(l_pcolor),
             -1,
             null)
          returning serv_id into l_serv_id;
      end;
    
      if (i > 1) then
        l_cost    := l_tar_tab(i).cost - l_tar_tab(1).cost;
        l_advance := 0;
      else
        l_cost    := l_tar_tab(i).cost;
        l_advance := l_tar_tab(i).advance;
      end if;
      -- добавляем стоимость услуги
      insert into t_serv_tar2
        (serv_id,
         tar_id,
         serv_cost,
         serv_advance,
         startdate,
         category,
         is_enable,
         is_on,
         phone_federal,
         phone_color,
         type,
         type_tariff)
        select l_serv_id,
               tar.id,
               l_cost,
               l_advance,
               l_tar_tab(i).startdate,
               nvl(l_tar_tab(i).category, 2),
               0,
               1,
               l_phone_types(l_pfederal),
               l_pcolor,--l_phone_colors(l_pcolor),
               1,
               tar.tariff_type
          from t_tariff2 tar
         where tar.id in (select max(t.id)
                            from t_tariff2 t, t_abstract_tar a
                           where t.at_id = a.at_id
                             and a.at_remote_id = pi_tar_remote_id
                             and a.at_region_id = pi_region_id
                             and a.at_asr_id = pi_asr_id);
    end loop;
  exception
    when others then
      logging_pkg.error('pi_asr_id = ' || pi_asr_id || ' ' ||
                        'pi_region_id = ' || pi_region_id || ' ' ||
                        'pi_tar_remote_id =  ' || pi_tar_remote_id || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Set_City_Color_ServicesPS');
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Set_City_Color_ServicesPS');
  end Set_City_Color_ServicesPS;
  --------------------------------------------------------------------------------------
  -- Вычисление MD5 хэша из выгрузки тарифных планов
  function Compute_MD5Hash_Of_Tariffs return varchar2 is
    l_row         varchar2(4000);
    l_cur         sys_refcursor;
    l_tar_rec     rec_tar;
    l_srv_rec     rec_srv;
    l_srv_g_rec   rec_srv_g;
    l_srv_c_rec   rec_srv_c;
    l_srv_r_rec   rec_srv_r;
    l_pack_req    rec_pack;
    l_disc_req    rec_discount;
    l_restrict_ab rec_restrict_ab;
  begin
    Util.MD5_Init();
    -- tariff
    open l_cur for '
        select tar_id,
               region_id,
               title,
               startdate,
               is_for_dealer,
               nvl(category, 2),
               is_active,
               tar_type,
               pay_type,
               vdvd_type,
               null,
               phone_federal,
               phone_color,
               cost,
               advance,
               tech_tar,
               decode(equipment_required, 1, 4, 2, -4, NVL(equipment_required, 0)) equipment_required
          from ' || g_tar_tbl_nm || ' t
         order by tar_id, phone_federal, phone_color';
    loop
      fetch l_cur
        into l_tar_rec;
      exit when l_cur%notfound;
      l_row := l_tar_rec.tar_id || l_tar_rec.region_id ||
               trim(upper(l_tar_rec.title)) || l_tar_rec.startdate ||
               l_tar_rec.is_for_dealer || l_tar_rec.category ||
               l_tar_rec.is_active || l_tar_rec.tar_type ||
               l_tar_rec.pay_type || l_tar_rec.vdvd_type ||
               l_tar_rec.phone_federal || l_tar_rec.phone_color ||
               l_tar_rec.cost || l_tar_rec.advance || l_tar_rec.tech_tar ||
               l_tar_rec.equipment_required;
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    -- service
    open l_cur for 'select distinct * from ' || g_srv_tbl_nm || ' order by SRV_ID';
    loop
      fetch l_cur
        into l_srv_rec;
      exit when l_cur%notfound;
      l_row := l_srv_rec.srv_id || trim(upper(l_srv_rec.srv_name)) ||
               trim(upper(l_srv_rec.srv_description)) || l_srv_rec.srv_type ||
               l_srv_rec.P_SERVICE || l_srv_rec.P_SERVICE_DVO ||
               l_srv_rec.TAR_ID || l_srv_rec.REGION_ID;
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    -- service_group
    open l_cur for 'select * from ' || g_srv_g_tbl_nm || ' order by GROUP_ID';
    loop
      fetch l_cur
        into l_srv_g_rec;
      exit when l_cur%notfound;
      l_row := l_srv_g_rec.group_id || trim(upper(l_srv_g_rec.group_name)) ||
               l_srv_g_rec.group_type || l_srv_g_rec.is_necessary ||
               l_srv_g_rec.is_single_pay;
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    -- service_cost
    open l_cur for '
        select distinct sc.srv_id,
                        sc.tar_id,
                        sc.region_id,
                        sc.cost,
                        sc.advance,
                        sc.category,
                        sc.is_enable,
                        sc.is_on,
                        sc.startdate,
                        sc.group_id,
                        sc.phone_federal,
                        sc.phone_color,
                        sc.PACK_ID,
                        sc.DISC_ID,
                        sc.TYPE_TARIFF,
                        sc.TYPE
          from ' || g_srv_c_tbl_nm || ' sc, ' || g_tar_tbl_nm || ' tar ' || '
         where sc.tar_id = tar.tar_id
           and sc.region_id = tar.region_id
         order by sc.srv_id,
                  sc.tar_id,
                  sc.region_id,
                  sc.phone_federal,
                  sc.phone_color ';
    loop
      fetch l_cur
        into l_srv_c_rec;
      exit when l_cur%notfound;
      l_row := l_srv_c_rec.srv_id || l_srv_c_rec.tar_id ||
               l_srv_c_rec.region_id || l_srv_c_rec.cost ||
               l_srv_c_rec.advance || l_srv_c_rec.category ||
               l_srv_c_rec.is_enable || l_srv_c_rec.is_on ||
               l_srv_c_rec.startdate || l_srv_c_rec.startdate ||
               l_srv_c_rec.group_id || l_srv_c_rec.phone_federal ||
               l_srv_c_rec.phone_color || l_srv_c_rec.PACK_ID ||
               l_srv_c_rec.DISC_ID || l_srv_c_rec.TYPE_TARIFF ||
               l_srv_c_rec.TYPE;
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    -- rel_service_service
    open l_cur for 'select * from mv_rel_service_service order by SRV_ID_1, SRV_ID_2, TAR_ID, REGION_ID';
    loop
      fetch l_cur
        into l_srv_r_rec;
      exit when l_cur%notfound;
      l_row := l_srv_r_rec.srv_id_1 || l_srv_r_rec.srv_id_2 ||
               l_srv_r_rec.tar_id || l_srv_r_rec.rel_type ||
               l_srv_r_rec.PACK_ID_1 || l_srv_r_rec.PACK_ID_2;
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    -- eissd_package
    open l_cur for 'select * from ' || g_eissd_package_tbl_nm || ' order by pack_id';
    loop
      fetch l_cur
        into l_pack_req;
      exit when l_cur%notfound;
      l_row := l_pack_req.PACK_ID || l_pack_req.PACK_NAME ||
               l_pack_req.P_SERVICE || l_pack_req.TAR_ID ||
               l_pack_req.REGION_ID;
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    -- discount
    open l_cur for 'select * from ' || g_discount_tbl_nm || ' order by disc_id';
    loop
      fetch l_cur
        into l_disc_req;
      exit when l_cur%notfound;
      l_row := l_disc_req.DISC_ID || l_disc_req.DISC_NAME ||
               l_disc_req.P_SERVICE || l_disc_req.PACK_ID ||
               l_disc_req.TAR_ID || l_disc_req.REGION_ID;
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    -- restrict_ability
    open l_cur for 'select * from ' || g_restrict_ab_tbl_nm || ' order by pack_id';
    loop
      fetch l_cur
        into l_restrict_ab;
      exit when l_cur%notfound;
      l_row := l_restrict_ab.SRV_ID || l_restrict_ab.PACK_ID ||
               l_restrict_ab.DISC_ID || l_restrict_ab.IS_ACTION;
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    return Util.MD5_Get_Final();
  end Compute_MD5Hash_Of_Tariffs;
  --------------------------------------------------------------------------------------
  -- Вычисление MD5 хэша справочника услуг
  function Compute_MD5Hash_Of_Services return varchar2 is
    l_row     varchar2(4000);
    l_srv_rec rec_srv;
    l_cur     sys_refcursor;
  begin
    Util.MD5_Init();
    open l_cur for 'select distinct * from ' || g_srv_tbl_nm || ' order by SRV_ID';
    loop
      fetch l_cur
        into l_srv_rec;
      exit when l_cur%notfound;
      l_row := l_srv_rec.srv_id || trim(upper(l_srv_rec.srv_name)) ||
               trim(upper(l_srv_rec.srv_description)) || l_srv_rec.srv_type ||
               trim(upper(l_srv_rec.P_SERVICE)) || l_srv_rec.P_SERVICE_DVO ||
               l_srv_rec.TAR_ID || l_srv_rec.REGION_ID;
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    return Util.MD5_Get_Final();
  end Compute_MD5Hash_Of_Services;
  --------------------------------------------------------------------------------------
  -- Вычисление MD5 хэша справочника групп услуг
  function Compute_MD5Hash_Of_Serv_Groups return varchar2 is
    l_row       varchar2(4000);
    l_srv_g_rec rec_srv_g;
    l_cur       sys_refcursor;
  begin
    Util.MD5_Init();
    open l_cur for 'select * from ' || g_srv_g_tbl_nm || ' order by GROUP_ID';
    loop
      fetch l_cur
        into l_srv_g_rec;
      exit when l_cur%notfound;
      l_row := l_srv_g_rec.group_id ||
               rtrim(ltrim(upper(l_srv_g_rec.group_name))) ||
               l_srv_g_rec.group_type || l_srv_g_rec.is_necessary ||
               l_srv_g_rec.is_single_pay;
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    return Util.MD5_Get_Final();
  end Compute_MD5Hash_Of_Serv_Groups;
  --------------------------------------------------------------------------------------
  -- Вычисление MD5 хэша тарифного плана со всеми услугами
  function Compute_MD5Hash_Of_Tariff(pi_tar_id        in number,
                                     pi_region_id     in number,
                                     pi_phone_federal in number := null,
                                     pi_phone_color   in number := null)
    return varchar2 is
    l_row         varchar2(4000);
    l_tar_rec     rec_tar;
    l_srv_r_rec   rec_srv_r;
    l_tar_srv_rec rec_tar_srv;
    l_cur         sys_refcursor;
  begin
    logging_pkg.info('pi_tar_id=' || pi_tar_id || ' pi_region_id=' ||
                     pi_region_id || ' pi_phone_federal=' ||
                     pi_phone_federal || ' pi_phone_color=' ||
                     pi_phone_color,
                     c_package || 'Compute_MD5Hash_Of_Tariff');
    Util.MD5_Init();
    -- Тарифный план
    open l_cur for '
			select tar_id,
						 region_id,
						 title,
						 startdate,
						 is_for_dealer,
						 nvl(category, 2),
						 is_active,
						 tar_type,
						 pay_type,
						 vdvd_type,
						 null,
						 phone_federal,
						 phone_color,
						 cost,
						 advance,
						 tech_tar,
						 decode(equipment_required, 1, 4, 2, -4, NVL(equipment_required, 0)) equipment_required
				from ' || g_tar_tbl_nm || ' t
			 where TAR_ID = :pi_tar_id
				 and REGION_ID = :pi_region_id
				 and (:pi_phone_federal is null or phone_federal = :pi_phone_federal)
				 and (:pi_phone_color is null or phone_color = :pi_phone_color)
       order by tar_id, phone_federal, phone_color'
      using pi_tar_id, pi_region_id, pi_phone_federal, pi_phone_federal, pi_phone_color, pi_phone_color;
    fetch l_cur
      into l_tar_rec;
    if (l_cur%notfound) then
      raise no_data_found;
    end if;
    l_row := rtrim(ltrim(upper(l_tar_rec.title))) || l_tar_rec.startdate ||
             l_tar_rec.is_for_dealer || l_tar_rec.category ||
             l_tar_rec.is_active || l_tar_rec.tar_type ||
             l_tar_rec.pay_type || l_tar_rec.vdvd_type ||
             l_tar_rec.phone_federal || l_tar_rec.phone_color ||
             l_tar_rec.cost || l_tar_rec.advance || l_tar_rec.tech_tar ||
             l_tar_rec.equipment_required;
    Util.MD5_Update(l_row);
    close l_cur;
  
    -- Услуги данного тарифного плана
    open l_cur for '
			select distinct sc.SRV_ID,
                      SRV_NAME,
                      SRV_DESCRIPTION,
                      SRV_TYPE,
                      COST,
                      ADVANCE,
                      CATEGORY,
                      IS_ENABLE,
                      IS_ON,
                      STARTDATE,
                      sc.GROUP_ID,
                      GROUP_NAME,
                      GROUP_TYPE,
                      IS_NECESSARY,
                      IS_SINGLE_PAY,
                      PHONE_FEDERAL,
                      PHONE_COLOR
				from ' || g_srv_tbl_nm || ' s,
						 ' || g_srv_c_tbl_nm || ' sc,
						 ' || g_srv_g_tbl_nm || ' sg
			 where s.TAR_ID = :pi_tar_id
				 and s.REGION_ID = :pi_region_id
				 and sc.SRV_ID = s.SRV_ID
				 and sc.GROUP_ID = sg.GROUP_ID(+)
			 order by sc.SRV_ID'
      using pi_tar_id, pi_region_id;
    loop
      fetch l_cur
        into l_tar_srv_rec;
      exit when l_cur%notfound;
      l_row := l_tar_srv_rec.srv_id ||
               rtrim(ltrim(upper(l_tar_srv_rec.srv_name))) ||
               rtrim(ltrim(upper(l_tar_srv_rec.srv_descr))) ||
               l_tar_srv_rec.srv_type || l_tar_srv_rec.cost ||
               l_tar_srv_rec.advance || l_tar_srv_rec.category ||
               l_tar_srv_rec.is_enable || l_tar_srv_rec.is_on ||
               l_tar_srv_rec.startdate || l_tar_srv_rec.group_id ||
               rtrim(ltrim(upper(l_tar_srv_rec.group_name))) ||
               l_tar_srv_rec.group_type || l_tar_srv_rec.is_necessary ||
               l_tar_srv_rec.is_single_pay || l_tar_srv_rec.phone_federal ||
               l_tar_srv_rec.phone_color;
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    -- Взаимосвязи услуг в данном ТП
    open l_cur for '
        select *
          from mv_rel_service_service r
        where
          TAR_ID    = :pi_tar_id and
          REGION_ID = :pi_region_id
        order by SRV_ID_1, SRV_ID_2'
      using pi_tar_id, pi_region_id;
    loop
      fetch l_cur
        into l_srv_r_rec;
      exit when l_cur%notfound;
      l_row := l_srv_r_rec.srv_id_1 || l_srv_r_rec.srv_id_2 ||
               l_srv_r_rec.rel_type || l_srv_r_rec.PACK_ID_1 ||
               l_srv_r_rec.PACK_ID_2;
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    return Util.MD5_Get_Final();
  end Compute_MD5Hash_Of_Tariff;
  --------------------------------------------------------------------------------------
  -- Вычисление MD5 хэша услуги
  function Compute_MD5Hash_Of_Service(pi_serv_name     in T_SERVICE2.SERV_NAME%type,
                                      pi_serv_desc     in T_SERVICE2.SERV_DESC%type,
                                      pi_serv_type     in T_SERVICE2.SERV_TYPE%type,
                                      pi_P_SERVICE     in T_SERVICE2.P_SERVICE%type,
                                      pi_P_SERVICE_DVO in T_SERVICE2.P_SERVICE_DVO%type,
                                      pi_TAR_ID        in T_SERVICE2.TAR_ID%type,
                                      pi_REGION_ID     in T_SERVICE2.REGION_ID%type)
    return varchar2 is
  begin
    Util.MD5_Init();
    Util.MD5_Update(rtrim(ltrim(upper(pi_serv_name))) ||
                    rtrim(ltrim(upper(pi_serv_desc))) || pi_serv_type ||
                    rtrim(ltrim(upper(pi_P_SERVICE))) || pi_P_SERVICE_DVO ||
                    pi_TAR_ID || pi_REGION_ID);
    return Util.MD5_Get_Final();
  end Compute_MD5Hash_Of_Service;
  --------------------------------------------------------------------------------------
  -- Вычисление MD5 хэша группы услуг
  function Compute_MD5Hash_Of_Serv_Group(pi_gr_name          in T_SERVICE_GROUP2.GROUP_NAME%type,
                                         pi_gr_type          in T_SERVICE_GROUP2.GROUP_TYPE%type,
                                         pi_gr_is_necessary  in T_SERVICE_GROUP2.IS_NECESSARY%type,
                                         pi_gr_is_single_pay in T_SERVICE_GROUP2.IS_SINGLE_PAY%type)
    return varchar2 is
  begin
    Util.MD5_Init();
    Util.MD5_Update(rtrim(ltrim(upper(pi_gr_name))) || pi_gr_type ||
                    pi_gr_is_necessary || pi_gr_is_single_pay);
    return Util.MD5_Get_Final();
  end Compute_MD5Hash_Of_Serv_Group;
  --------------------------------------------------------------------------------------
  -- Вычисление MD5 хэша пакета
  function Compute_MD5Hash_Of_Package(PACK_NAME in t_eissd_package2.Pack_Name%type,
                                      P_SERVICE in t_eissd_package2.p_Service%type,
                                      TAR_ID    in t_eissd_package2.p_Service%type,
                                      REGION_ID in t_eissd_package2.p_Service%type)
    return varchar2 is
  begin
    Util.MD5_Init();
    Util.MD5_Update(rtrim(ltrim(upper(PACK_NAME))) ||
                    rtrim(ltrim(upper(P_SERVICE))) ||
                    rtrim(ltrim(upper(TAR_ID))) ||
                    rtrim(ltrim(upper(REGION_ID))));
    return Util.MD5_Get_Final();
  end Compute_MD5Hash_Of_Package;
  --------------------------------------------------------------------------------------
  -- Вычисление MD5 хэша скидки
  function Compute_MD5Hash_Of_Discount(pi_DISC_NAME in t_discount2.DISC_NAME%type,
                                       pi_P_SERVICE in t_discount2.P_SERVICE%type,
                                       pi_PACK_ID   in t_discount2.PACK_ID%type,
                                       pi_TAR_ID    in t_discount2.TAR_ID%type,
                                       pi_REGION_ID in t_discount2.REGION_ID%type)
    return varchar2 is
  begin
    Util.MD5_Init();
    Util.MD5_Update(rtrim(ltrim(upper(pi_DISC_NAME))) ||
                    rtrim(ltrim(upper(pi_P_SERVICE))) || pi_PACK_ID ||
                    pi_TAR_ID || pi_REGION_ID);
    return Util.MD5_Get_Final();
  end Compute_MD5Hash_Of_Discount;
  --------------------------------------------------------------------------------------
  -- Вычисление MD5 хэша справочника пакетов
  function Compute_MD5Hash_Of_packages return varchar2 is
    l_row      varchar2(4000);
    l_pack_rec rec_pack;
    l_cur      sys_refcursor;
  begin
    Util.MD5_Init();
    open l_cur for 'select distinct * from ' || g_eissd_package_tbl_nm || ' order by PACK_ID';
    loop
      fetch l_cur
        into l_pack_rec;
      exit when l_cur%notfound;
      l_row := l_pack_rec.PACK_ID ||
               rtrim(ltrim(upper(l_pack_rec.PACK_NAME))) ||
               rtrim(ltrim(upper(l_pack_rec.P_SERVICE))) ||
               rtrim(ltrim(upper(l_pack_rec.TAR_ID))) ||
               rtrim(ltrim(upper(l_pack_rec.REGION_ID)));
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    return Util.MD5_Get_Final();
  end Compute_MD5Hash_Of_packages;
  --------------------------------------------------------------------------------------
  -- Вычисление MD5 хэша справочника скидок
  function Compute_MD5Hash_Of_discounts return varchar2 is
    l_row          varchar2(4000);
    l_discount_rec rec_discount;
    l_cur          sys_refcursor;
  begin
    Util.MD5_Init();
    open l_cur for 'select * from ' || g_discount_tbl_nm || ' order by DISC_ID';
    loop
      fetch l_cur
        into l_discount_rec;
      exit when l_cur%notfound;
      l_row := l_discount_rec.DISC_ID ||
               rtrim(ltrim(upper(l_discount_rec.DISC_NAME))) ||
               rtrim(ltrim(upper(l_discount_rec.P_SERVICE))) ||
               l_discount_rec.PACK_ID || l_discount_rec.TAR_ID ||
               l_discount_rec.REGION_ID;
      Util.MD5_Update(l_row);
    end loop;
    close l_cur;
    return Util.MD5_Get_Final();
  end Compute_MD5Hash_Of_discounts;
  -----------------------------------------------------------------------------------------------
  procedure Sync_Cost_New_Tariff2(pi_at_id  number,
                                  pi_imp_id number,
                                  pi_date   date) is
    l_is_org_usi boolean;
    l_is_org_gph boolean;
    l_err_msg    varchar2(2000) := 'Ошибка при пересчете стоимости ТМЦ. Отсутствует идентификатор договора.';
  begin
    insert into asr_tmc_update_cost_log
      (imp_id,
       tar_id,
       tmc_id,
       org_id,
       root_org_id,
       dog_id,
       old_tmc_cost,
       new_tmc_cost)
      select pi_imp_id,
             pi_at_id,
             t.tmc_id,
             ots.org_id,
             o.root_org_id,
             ots.dog_id,
             t.tmc_tmp_cost,
             get_tar_pack_cost(t.tmc_id, t.tmc_tmp_cost, pi_date)
        from t_tmc_sim s, t_tmc t, t_org_tmc_status ots, t_organizations o
       where s.tar_id = pi_at_id
         and s.tmc_id = t.tmc_id
         and t.tmc_type = 8
         and t.tmc_id = ots.tmc_id
         and ots.status = 11
         and ots.org_id = o.org_id;
  
    merge into T_TMC Tt
    using (Select t.imp_id, t.tar_id, t.tmc_id, t.new_tmc_cost
             from ASR_TMC_UPDATE_COST_LOG t
            where t.IMP_ID = pi_imp_id
              and t.tar_id = pi_at_id
              and t.old_tmc_cost <> t.NEW_TMC_COST) t
    on (tt.tmc_id = t.tmc_id)
    when matched then
      update SET tT.TMC_TMP_COST = t.NEW_TMC_COST;
  
    for item in (select al.root_org_id,
                        al.dog_id,
                        sum(al.old_tmc_cost - al.new_tmc_cost) amount
                   from asr_tmc_update_cost_log al
                  where al.imp_id = pi_imp_id
                    and al.tar_id = pi_at_id
                  group by al.root_org_id, al.dog_id) loop
    
      l_is_org_usi := IS_ORG_USI(item.root_org_id) > 0;
      l_is_org_gph := supervizor.isOrgGPH(item.root_org_id) > 0;
      if (not l_is_org_usi and item.dog_id is null) then
        logging_pkg.error(l_err_msg || ' org_id=' || item.root_org_id,
                          'Sync_Cost_New_Tariff2');
        logging_pkg.Raise_App_Err(-1000,
                                  l_err_msg || ' org_id=' ||
                                  item.root_org_id || ' tar_id=' ||
                                  pi_at_id || ' imp_id=' || pi_imp_id);
        return;
      end if;
    
      if (not l_is_org_usi and not l_is_org_gph) then
        if (item.amount > 0) then
          -- стоимость ТМЦ стала меньше значит нужно разницу вернуть на лицевой с резерва
          ACC_OPERATIONS.Back_Funds_Tmc(pi_date,
                                        null,
                                        null,
                                        abs(item.amount),
                                        null,
                                        0,
                                        0,
                                        Acc_Operations.c_op_type_tr_res_lic_back,
                                        item.dog_id);
        elsif (item.amount < 0) then
          -- стоимость ТМЦ стала больше значит нужно разницу досписать с лицевого в резерв
          ACC_OPERATIONS.Reserve_Funds_Tmc(pi_date,
                                           null,
                                           null,
                                           abs(item.amount),
                                           null,
                                           0,
                                           0,
                                           item.dog_id);
        end if;
      end if;
    end loop;
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Sync_Cost_New_Tariff2');
      raise;
  end Sync_Cost_New_Tariff2;
  -------------------------------------------------------------------
  -- обновляет стоимости ТМЦ и счета Дилеров согласно измененному тарифу
  procedure Sync_Cost_New_Tariff(l_at_id        number,
                                 l_last_ver_num number,
                                 pi_max_iter    number,
                                 l_cur_time     date) is
    l_tar_id2     number;
    l_cost_differ number;
    l_max_iter    number := 0;
    l_cur3        sys_refcursor;
  
    type t_org_cost is record(
      org_id   number,
      old_cost number,
      new_cost number);
    cur_org_cost  t_org_cost;
    l_cur_org_pid number;
  begin
    -- получим идентификаторы нового тарифа
    select t2.id
      into l_tar_id2
      from t_tariff2 t2
     where t2.at_id = l_at_id
       and t2.ver_num = l_last_ver_num;
    l_max_iter := pi_max_iter + 1;
  
    -- сохраним информацию по каким организациям, тарифному плану и тмц
    -- будет изменение, а также какая была цена тмц и какая станет.
    insert into z_t_org_tmc_status
      (iteration,
       at_id,
       org_id,
       tmc_id,
       tmc_cost,
       new_cost,
       org_pid,
       rdate)
      (select l_max_iter,
              l_at_id,
              ots.org_id,
              ots.tmc_id,
              t.tmc_tmp_cost tmc_cost,
              get_tar_pack_cost(t.tmc_id, t.tmc_tmp_cost, l_cur_time) new_cost,
              o.root_org_id org_pid,
              l_cur_time rdate
         from (select tun.st_sklad_1 status,
                      tun.owner_id_1 org_id,
                      tun.tmc_id
                 from t_tmc_operation_units tun
                where tun.unit_id =
                      (select max(tun2.unit_id)
                         from t_tmc_operations      top2,
                              t_tmc_operation_units tun2
                        where trunc(top2.op_date) < trunc(l_cur_time)
                          and top2.op_id = tun2.op_id
                          and tun2.tmc_id = tun.tmc_id)
                  and tun.owner_id_1 is not null
                order by tun.unit_id) ots,
              t_tmc t,
              t_tmc_sim ts,
              t_organizations o
        where ots.tmc_id = t.tmc_id
          and t.tmc_type = 8
          and t.tmc_id = ts.tmc_id
          and ts.tar_id = l_at_id
          and ots.org_id = o.org_id);
  
    open l_cur3 for '
			select org_pid, sum(tmc_cost), sum(new_cost)
				from z_t_org_tmc_status
			 where iteration = :l_max_iter
				 and at_id = :l_at_id
			 group by org_pid'
      using l_max_iter, l_at_id;
    loop
      fetch l_cur3
        into cur_org_cost;
      exit when l_cur3%notfound;
    
      -- Меняем стоимость Т;МЦ
      update t_tmc t
         set t.tmc_tmp_cost =
             (select zots1.new_cost
                from z_t_org_tmc_status zots1
               where zots1.org_pid = cur_org_cost.org_id
                 and zots1.iteration = l_max_iter
                 and zots1.at_id = l_at_id
                 and zots1.tmc_id = t.tmc_id)
       where t.tmc_id in (select zots.tmc_id
                            from z_t_org_tmc_status zots
                           where zots.org_pid = cur_org_cost.org_id
                             and zots.iteration = l_max_iter
                             and zots.at_id = l_at_id);
      -- Если не принципал то тогда операции со счетами
      if (IS_ORG_USI(cur_org_cost.org_id) = 0) then
        -- Найдем куратора
        l_cur_org_pid := get_curator_org(cur_org_cost.org_id);
      
        -- разница в стоимости ТМЦ в связи с изменением тарифа
        l_cost_differ := (cur_org_cost.new_cost - cur_org_cost.old_cost);
        if (l_cost_differ < 0) then
          -- стоимость ТМЦ стала меньше значит нужно разницу вернуть на лицевой с резерва
          ACC_OPERATIONS.Back_Funds_Tmc(l_cur_time,
                                        cur_org_cost.org_id,
                                        l_cur_org_pid,
                                        (0 - l_cost_differ),
                                        null,
                                        0,
                                        0);
        elsif (l_cost_differ > 0) then
          -- стоимость ТМЦ стала больше значит нужно разницу досписать с лицевого в резерв
          ACC_OPERATIONS.Reserve_Funds_Tmc(l_cur_time,
                                           cur_org_cost.org_id,
                                           l_cur_org_pid,
                                           l_cost_differ,
                                           null,
                                           0,
                                           0);
        end if;
      end if;
    end loop;
    close l_cur3;
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Sync_Cost_New_Tariff');
      raise;
  end Sync_Cost_New_Tariff;
  -------------------------------------------------------------------
  -- Полный импорт тарифных планов (вызывается при самой первой выгрузке от АСР)
  procedure Tariff_Import_Full(pi_asr_id in T_ASR.ASR_ID%type,
                               pi_imp_id in ASR_TAR_IMP_MD5.TIM_ID%type,
                               pi_date   in date) is
    l_ver_num   pls_integer := 1;
    sql_text    t_sql;
    l_cur       sys_refcursor;
    l_asr_id    T_ABSTRACT_TAR.AT_ASR_ID%type;
    l_region_id T_ABSTRACT_TAR.AT_REGION_ID%type;
    l_tar_id    T_ABSTRACT_TAR.AT_REMOTE_ID%type;
    l_at_id     T_ABSTRACT_TAR.AT_ID%type;
  begin
    -- формируем полные имена таблиц
    Set_Dpa_Tables_Names(pi_asr_id);
    -- Пересчитаем вьюху
    dbms_mview.refresh('MV_REL_SERVICE_SERVICE');
    
    open l_cur for '
			select distinct ' || pi_asr_id || ', REGION_ID, TAR_ID
				from ' || g_tar_tbl_nm || '
			 where (phone_federal is null or phone_federal = 1)
				 and (phone_color is null or phone_color = 1)';
    loop
      fetch l_cur
        into l_asr_id, l_region_id, l_tar_id;
      exit when l_cur%notfound;
      begin
        select AT_ID
          into l_at_id
          from T_ABSTRACT_TAR
         where AT_ASR_ID = l_asr_id
           and AT_REGION_ID = l_region_id
           and AT_REMOTE_ID = l_tar_id;
      exception
        when no_data_found then
          insert into T_ABSTRACT_TAR
            (AT_ASR_ID, AT_REGION_ID, AT_REMOTE_ID)
          values
            (l_asr_id, l_region_id, l_tar_id);
      end;
    end loop;
    close l_cur;
    sql_text := '
			insert into T_TARIFF2
				(AT_ID,
				 START_DATE,
				 TITLE,
				 IS_FOR_DEALER,
				 CATEGORY,
				 PAY_TYPE,
				 TARIFF_TYPE,
				 TYPE_VDVD_ID,
				 VER_NUM,
				 VER_DATE_BEG,
				 IMP_ID,
				 PHONE_FEDERAL,
				 PHONE_COLOR,
				 COST,
				 ADVANCE,
				 IS_TECH,
				 TECH_AT_ID,
				 equipment_required)
				select distinct at.AT_ID,
							 STARTDATE,
							 TITLE,
							 IS_FOR_DEALER,
							 nvl(category, 2),
							 case PAY_TYPE
								 when :c_tar_pay_type_adv_prot then
									:c_tar_pay_type_adv_dic
								 when :c_tar_pay_type_cr_prot then
									:c_tar_pay_type_cr_dic
							 end,
							 case TAR_TYPE
								 when :c_tar_type_prepaid_prot then
									:c_tar_type_prepaid_dic
								 when :c_tar_type_postpaid_prot then
									:c_tar_type_postpaid_dic
							 end,
							 case VDVD_TYPE
								 when :c_tar_vdvd_type_gsm_prot then
									:c_tar_vdvd_type_gsm_dic
								 when :c_tar_vdvd_type_pstn_prot then
									:c_tar_vdvd_type_pstn_dic
								 when :c_tar_vdvd_type_cdma_prot then
									:c_tar_vdvd_type_cdma_dic
								 when :c_tar_vdvd_type_nmt_prot then
									:c_tar_vdvd_type_nmt_dic
								 when :c_tar_vdvd_type_adsl_prot then
									:c_tar_vdvd_type_adsl_dic
							 end,
							 :l_ver_num,
							 :date_beg,
							 :imp_id,
							 case PHONE_FEDERAL
								 when :c_phone_federal_prot then
									:c_phone_federal_dic
								 when :c_phone_city_prot then
									:c_phone_city_dic
								 else
									:c_phone_federal_dic
							 end,
							 nvl(c.id,tmc_sim.get_color_default),
							 COST,
							 ADVANCE,
							 decode(TECH_TAR, 0, 1, 0),
							 decode(TECH_TAR, -1, null, 0, null, TECH_TAR),
							 Decode(t.equipment_required, 1, 4, 2, -4, NVL(t.equipment_required, 0)) equipment_required
					from ' || g_tar_tbl_nm || ' t,
							 T_ABSTRACT_TAR at,
               t_dic_sim_color c
				 where t.TAR_ID = at.AT_REMOTE_ID
					 and t.REGION_ID = at.AT_REGION_ID
					 and at.AT_ASR_ID = :pi_asr_id
           and at.PHONE_COLOR = c.ps_id(+)
					 and (t.phone_federal is null or phone_federal = 1)
					 and (t.phone_color is null or phone_color = 1) ';
    execute immediate sql_text
      using c_tar_pay_type_adv_prot, c_tar_pay_type_adv_dic, c_tar_pay_type_cr_prot, c_tar_pay_type_cr_dic, c_tar_type_prepaid_prot, c_tar_type_prepaid_dic, c_tar_type_postpaid_prot, c_tar_type_postpaid_dic, c_tar_vdvd_type_gsm_prot, c_tar_vdvd_type_gsm_dic, c_tar_vdvd_type_pstn_prot, c_tar_vdvd_type_pstn_dic, c_tar_vdvd_type_cdma_prot, c_tar_vdvd_type_cdma_dic, c_tar_vdvd_type_nmt_prot, c_tar_vdvd_type_nmt_dic, c_tar_vdvd_type_adsl_prot, c_tar_vdvd_type_adsl_dic, l_ver_num, pi_date, pi_imp_id, c_phone_federal_prot, c_phone_federal_dic, c_phone_city_prot, c_phone_city_dic, c_phone_federal_dic, /*c_phone_color_simple_prot, c_phone_color_simple_dic, c_phone_color_silver_prot, c_phone_color_silver_dic, c_phone_color_gold_prot, c_phone_color_gold_dic, c_phone_color_plat_prot, c_phone_color_plat_dic, c_phone_color_gold_dic,*/ pi_asr_id;
    -- Исправляем ссылки на технологический ТП
    update t_tariff2 t2
       set t2.tech_at_id =
           (select atar.at_id
              from t_abstract_tar atar
             where atar.at_remote_id = t2.tech_at_id)
     where t2.tech_at_id is not null;
    -- справочник услуг
    sql_text := '
      insert into T_SERVICE2
        (REMOTE_SERV_ID,
         SERV_ASR_ID,
         SERV_NAME,
         SERV_DESC,
         SERV_TYPE,
         SERV_VER_NUM,
         SERV_IMP_ID,
         P_SERVICE,
         P_SERVICE_DVO,
         TAR_ID,
         REGION_ID)
      select
        distinct SRV_ID,
                 :pi_asr_id,
                 SRV_NAME,
                 SRV_DESCRIPTION,
                 SRV_TYPE,
                 :l_ver_num,
                 :imp_id,
                 P_SERVICE,
                 P_SERVICE_DVO,
                 TAR_ID,
                 REGION_ID
      from ' || g_srv_tbl_nm;
    execute immediate sql_text
      using pi_asr_id, l_ver_num, pi_imp_id;
    -----------------------------------------------------------------
    -- справочник групп услуг
    sql_text := '
      insert into T_SERVICE_GROUP2
        (REMOTE_ID,
         GROUP_NAME,
         GROUP_TYPE,
         IS_NECESSARY,
         IS_SINGLE_PAY,
         ASR_ID,
         VER_NUM,
         IMP_ID)
      select distinct
        GROUP_ID,
        GROUP_NAME,
        GROUP_TYPE,
        IS_NECESSARY,
        IS_SINGLE_PAY,
        :pi_asr_id,
        :l_ver_num,
        :imp_id
      from ' || g_srv_g_tbl_nm;
    execute immediate sql_text
      using pi_asr_id, l_ver_num, pi_imp_id;
    -----------------------------------------------------------------
    -- справочник пакетов
    sql_text := '
      insert into T_EISSD_PACKAGE2
        (PACK_ID,
         PACK_NAME,
         P_SERVICE,
         ASR_ID,
         VER_NUM,
         IMP_ID,
         TAR_ID,
         REGION_ID)
      select distinct
        PACK_ID,
        PACK_NAME,
        P_SERVICE,
        :pi_asr_id,
        :l_ver_num,
        :imp_id,
        TAR_ID,
        REGION_ID
      from ' || g_eissd_package_tbl_nm;
    execute immediate sql_text
      using pi_asr_id, l_ver_num, pi_imp_id;
    -----------------------------------------------------------------
    -- справочник скидок
    sql_text := '
      insert into T_DISCOUNT2
        (DISC_ID,
         DISC_NAME,
         P_SERVICE,
         PACK_ID,
         TAR_ID,
         REGION_ID,
         ASR_ID,
         VER_NUM,
         IMP_ID)
      select distinct
        DISC_ID,
        DISC_NAME,
        P_SERVICE,
        PACK_ID,
        TAR_ID,
        REGION_ID,
        :pi_asr_id,
        :l_ver_num,
        :imp_id
      from ' || g_discount_tbl_nm;
    execute immediate sql_text
      using pi_asr_id, l_ver_num, pi_imp_id;
    -----------------------------------------------------------------
    -- атрибуты услуг
    ----- Загрузим тарифы и сервисы для данного АСР во временные таблицы для
    -- ускорения импорта в таблицы T_SERV_TAR и T_SERV_SERV
    insert into TMP_IMPORT_TAR
      (TAR_ID, TAR_REMOTE_ID, TAR_REGION_ID, TAR_ASR_ID)
      select DISTINCT ID, AT_REMOTE_ID, AT_REGION_ID, AT_ASR_ID
        from T_ABSTRACT_TAR at, T_TARIFF2 t
       where at.AT_ID = t.AT_ID
         and at.AT_ASR_ID = pi_asr_id;
  
    insert into TMP_IMPORT_SERVICE
      (SERV_ID, REMOTE_SERV_ID, SERV_ASR_ID, TAR_ID, REGION_ID)
      select DISTINCT SERV_ID, REMOTE_SERV_ID, SERV_ASR_ID, TAR_ID, REGION_ID
        from T_SERVICE2
       where SERV_ASR_ID = pi_asr_id
         and REMOTE_SERV_ID <> -1
         and TAR_ID is not null;
  
    insert into TMP_IMPORT_PACKAGE
      (PACK_ID, PACK_ASR_ID)
      select DISTINCT PACK_ID, ASR_ID
        from T_EISSD_PACKAGE2 P
       where P.ASR_ID = pi_asr_id
         and PACK_ID NOT IN (-1, 0);
  
    insert into TMP_IMPORT_DISCOUNT
      (DISC_ID, DISC_ASR_ID)
      select DISTINCT DISC_ID, ASR_ID
        from T_DISCOUNT2
       where ASR_ID = pi_asr_id
         and DISC_ID NOT IN (-1, 0);
    ---------------------------------------------------------------------------
    -- загружаем параметры услуг
    sql_text := '
			insert into T_SERV_TAR2
				(SERV_ID,
				 TAR_ID,
				 SERV_COST,
				 SERV_ADVANCE,
				 STARTDATE,
				 CATEGORY,
				 IS_ENABLE,
				 IS_ON,
				 GROUP_ID,
				 PHONE_FEDERAL,
				 PHONE_COLOR,
         PACK_ID,
         DISC_ID,
         TYPE_TARIFF,
         TYPE)
    select distinct s.SERV_ID,
                    t.TAR_ID,
                    sc.COST,
                    sc.ADVANCE,
                    sc.STARTDATE,
                    nvl(sc.CATEGORY, 2),
                    sc.IS_ENABLE,
                    sc.IS_ON,
                    sg.GROUP_ID,
                    case sc.PHONE_FEDERAL
                    when :c_phone_federal_prot then
                         :c_phone_federal_dic
                    when :c_phone_city_prot then
                         :c_phone_city_dic
                    else
                         :c_phone_federal_dic
                    end,
                    nvl(cc.id,tmc_sim.get_color_default),
                    E.PACK_ID,
                    D.DISC_ID,
                    sc.TYPE_TARIFF,
                    sc.TYPE
      from TMP_IMPORT_TAR t
      join ' || g_srv_c_tbl_nm || ' sc
        on t.TAR_REMOTE_ID = sc.TAR_ID
       and t.TAR_REGION_ID = sc.REGION_ID
      left join TMP_IMPORT_SERVICE s
        on s.REMOTE_SERV_ID = sc.SRV_ID
       AND S.TAR_ID = sc.TAR_ID
       AND S.REGION_ID = sc.REGION_ID
      left join T_SERVICE_GROUP2 sg
        on sc.GROUP_ID = sg.REMOTE_ID
       and sg.ASR_ID = :pi_asr_id
      left join TMP_IMPORT_PACKAGE e
        on e.PACK_ID = sc.PACK_ID
      left join TMP_IMPORT_DISCOUNT d
        on d.disc_id = sc.disc_id
      left join t_dic_sim_color cc
        on cc.ps_id = sc.PHONE_COLOR';
    execute immediate sql_text
      using c_phone_federal_prot, c_phone_federal_dic, c_phone_city_prot, c_phone_city_dic, c_phone_federal_dic,/* c_phone_color_simple_prot, c_phone_color_simple_dic, c_phone_color_silver_prot, c_phone_color_silver_dic, c_phone_color_gold_prot, c_phone_color_gold_dic, c_phone_color_plat_prot, c_phone_color_plat_dic, c_phone_color_simple_dic,*/ pi_asr_id;
  
    --- Взаимосвязи услуг
    sql_text := '
			insert into T_SERV_SERV2
				(SERV1_ID, SERV2_ID, REL_TYPE, TAR_ID, PACK_ID_1, PACK_ID_2)
				select distinct s1.SERV_ID,
                        s2.SERV_ID,
                        case rs.REL_TYPE
                          when :c_serv_rel_type_par_prot then
                           :c_serv_rel_type_par_dic
                          when :c_serv_rel_type_enem_prot then
                           :c_serv_rel_type_enem_dic
                          when :c_serv_rel_type_both_prot then
                           :c_serv_rel_type_both_dic
                        end,
                        t.TAR_ID,
                        rs.PACK_ID_1,
                        rs.PACK_ID_2
          from TMP_IMPORT_TAR t
          join mv_rel_service_service rs
            on rs.TAR_ID = t.TAR_REMOTE_ID
           and rs.REGION_ID = t.TAR_REGION_ID
          left join TMP_IMPORT_SERVICE s1
            on s1.REMOTE_SERV_ID = rs.SRV_ID_1
           AND S1.TAR_ID = RS.TAR_ID
           AND S1.REGION_ID = RS.REGION_ID
          left join TMP_IMPORT_SERVICE s2
            on s2.REMOTE_SERV_ID = rs.SRV_ID_2
           AND S2.TAR_ID = RS.TAR_ID
           AND S2.REGION_ID = RS.REGION_ID';
    execute immediate sql_text
      using c_serv_rel_type_par_prot, c_serv_rel_type_par_dic, c_serv_rel_type_enem_prot, c_serv_rel_type_enem_dic, c_serv_rel_type_both_prot, c_serv_rel_type_both_dic;
    -- Ограничения
    sql_text := '
    insert into t_restrict_ability
      (SRV_ID, PACK_ID, DISC_ID, IS_ACTION)
      select distinct SRV_ID, PACK_ID, DISC_ID, IS_ACTION
        from ' || g_restrict_ab_tbl_nm;
    execute immediate sql_text;
    -- если импорт выполняется для Питерсервиса, добавим виртуальные услуги для подключения городских и цветных номеров
    /*if (pi_asr_id = 3 and l_tar_id is not null) then
      Set_City_Color_ServicesPS(l_tar_id, pi_asr_id, l_region_id);
    end if;*/ -- Потом раскомментировать, а вообще хрень и запускается только для одного тарифа почему-то
    Add_tariff_by_at_id;
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Tariff_Import_Full');
      raise;
  end Tariff_Import_Full;
  -------------------------------------------------------------------
  -- Сохранение слепка текущей выгрузки тарифных планов (для синхронизации при следующих
  -- выгрузках)
  procedure Store_Tar_Upload is
    sql_text t_sql;
  begin
    execute immediate 'delete from ' || g_tar_tbl_nm2;
    execute immediate 'delete from ' || g_srv_tbl_nm2;
    execute immediate 'delete from ' || g_srv_g_tbl_nm2;
    execute immediate 'delete from ' || g_srv_c_tbl_nm2;
    execute immediate 'delete from ' || g_srv_r_tbl_nm2;
    execute immediate 'delete from ' || g_eissd_package_tbl_nm2;
    execute immediate 'delete from ' || g_discount_tbl_nm2;
    execute immediate 'delete from ' || g_restrict_ab_tbl_nm2;
    -- TARIFF
    sql_text := '
      insert into ' || g_tar_tbl_nm2 || '
			(tar_id,
			 region_id,
			 title,
			 startdate,
			 is_for_dealer,
			 category,
			 is_active,
			 tar_type,
			 pay_type,
			 vdvd_type,
			 tar_md5,
			 phone_federal,
			 phone_color,
			 cost,
			 advance,
			 tech_tar,
			 equipment_required)
			select distinct tar_id,
						 region_id,
						 title,
						 startdate,
						 is_for_dealer,
						 nvl(category, 2),
						 is_active,
						 tar_type,
						 pay_type,
						 vdvd_type,
						 ASR_Import_Tar.Compute_MD5Hash_Of_Tariff(tar_id,
																											region_id,
																											phone_federal,
																											phone_color),
						 phone_federal,
						 phone_color,
						 cost,
						 advance,
						 tech_tar,
						 decode(equipment_required, 1, 4, 2, -4, NVL(equipment_required, 0))
				from ' || g_tar_tbl_nm;
    execute immediate sql_text;
    -- SERVICE
    sql_text := '
      insert into ' || g_srv_tbl_nm2 || '
        (SRV_ID,
         SRV_NAME,
         SRV_DESCRIPTION,
         SRV_TYPE,
         SRV_MD5,
         P_SERVICE,
         P_SERVICE_DVO,
         TAR_ID,
         REGION_ID)
      select distinct
         SRV_ID,
         SRV_NAME,
         SRV_DESCRIPTION,
         SRV_TYPE,
         ASR_Import_Tar.Compute_MD5Hash_Of_Service(SRV_NAME,
                                                   SRV_DESCRIPTION,
                                                   SRV_TYPE,
                                                   P_SERVICE,
                                                   P_SERVICE_DVO,
                                                   TAR_ID,
                                                   REGION_ID),
         P_SERVICE,
         P_SERVICE_DVO,
         TAR_ID,
         REGION_ID
        from ' || g_srv_tbl_nm;
    execute immediate sql_text;
    -- SERVICE_GROUP
    sql_text := '
      insert into ' || g_srv_g_tbl_nm2 || '
        (GROUP_ID,
         GROUP_NAME,
         GROUP_TYPE,
         IS_NECESSARY,
         IS_SINGLE_PAY,
         GROUP_MD5)
      select distinct
         GROUP_ID,
         GROUP_NAME,
         GROUP_TYPE,
         IS_NECESSARY,
         IS_SINGLE_PAY,
         ASR_Import_Tar.Compute_MD5Hash_Of_Serv_Group(GROUP_NAME, GROUP_TYPE, IS_NECESSARY, IS_SINGLE_PAY)
        from ' || g_srv_g_tbl_nm;
    execute immediate sql_text;
    -- SERVICE_COST
    sql_text := '
      insert into ' || g_srv_c_tbl_nm2 || '
      select distinct *
        from ' || g_srv_c_tbl_nm;
    execute immediate sql_text;
    -- REL_SERVICE_SERVICE
    sql_text := '
      insert into ' || g_srv_r_tbl_nm2 || '
      select distinct *
        from mv_rel_service_service';
    execute immediate sql_text;
    -- EISSD_PACKAGE
    sql_text := '
      insert into ' || g_eissd_package_tbl_nm2 || '
        (PACK_ID,
         PACK_NAME,
         P_SERVICE,
         PACK_MD5,
         TAR_ID,
         REGION_ID)
      select distinct
         PACK_ID,
         PACK_NAME,
         P_SERVICE,
         ASR_Import_Tar.Compute_MD5Hash_Of_Package(PACK_NAME,
                                                   P_SERVICE,
                                                   TAR_ID,
                                                   REGION_ID),
         TAR_ID,
         REGION_ID
        from ' || g_eissd_package_tbl_nm;
    execute immediate sql_text;
    -- DISCOUNT
    sql_text := '
      insert into ' || g_discount_tbl_nm2 || '
      (DISC_NAME,
       P_SERVICE,
       PACK_ID,
       TAR_ID,
       REGION_ID,
       DISC_MD5)
      select distinct DISC_NAME,
             P_SERVICE,
             PACK_ID,
             TAR_ID,
             REGION_ID,
             ASR_Import_Tar.Compute_MD5Hash_Of_discount(DISC_NAME,
                                                        P_SERVICE,
                                                        PACK_ID,
                                                        TAR_ID,
                                                        REGION_ID)
        from ' || g_discount_tbl_nm;
    execute immediate sql_text;
    -- RESTRICT_ABILITY
    sql_text := '
      insert into ' || g_restrict_ab_tbl_nm2 || '
      (SRV_ID,
       PACK_ID,
       DISC_ID,
       IS_ACTION)
      select distinct SRV_ID,
             PACK_ID,
             DISC_ID,
             IS_ACTION
        from ' || g_restrict_ab_tbl_nm;
    execute immediate sql_text;
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Store_Tar_Upload');
      raise;
  end Store_Tar_Upload;
  -------------------------------------------------------------------
  -- Добавляет новый тарифный план
  procedure Add_Tariff(pi_asr_id   in T_ASR.ASR_ID%type,
                       pi_at_id    in T_ABSTRACT_TAR.AT_ID%type,
                       pi_tar_rec  in rec_tar,
                       pi_ver_date in date,
                       pi_ver_num  in T_TARIFF2.VER_NUM%type,
                       pi_imp_id   in ASR_TAR_IMP_MD5.TIM_ID%type) is
    l_at_id      T_ABSTRACT_TAR.AT_ID%type := pi_at_id;
    l_loc_tar_id T_TARIFF2.ID%type;
    sql_text     t_sql;
    l_color number;
  begin
    if (l_at_id is null) then
      insert into T_ABSTRACT_TAR
        (AT_ASR_ID, AT_REGION_ID, AT_REMOTE_ID)
      values
        (pi_asr_id, pi_tar_rec.region_id, pi_tar_rec.tar_id)
      returning AT_ID into l_at_id;
    end if;
    
    --найдем цвет
    begin
      select c.id
        into l_color
        from t_dic_sim_color c
       where c.ps_id = pi_tar_rec.phone_color
         and c.is_actual=1;
    exception
      when no_data_found then
        l_color:=tmc_sim.get_color_default;
    end;    
           
    -- добавляем атрибуты ТП
    insert into T_TARIFF2
      (AT_ID,
       START_DATE,
       TITLE,
       IS_FOR_DEALER,
       CATEGORY,
       PAY_TYPE,
       TARIFF_TYPE,
       TYPE_VDVD_ID,
       VER_NUM,
       VER_DATE_BEG,
       IMP_ID,
       PHONE_FEDERAL,
       PHONE_COLOR,
       COST,
       ADVANCE,
       IS_TECH,
       TECH_AT_ID,
       equipment_required)
    values
      (l_at_id,
       pi_tar_rec.startdate,
       pi_tar_rec.title,
       pi_tar_rec.is_for_dealer,
       pi_tar_rec.category,
       case pi_tar_rec.pay_type when c_tar_pay_type_adv_prot then
       c_tar_pay_type_adv_dic when c_tar_pay_type_cr_prot then
       c_tar_pay_type_cr_dic end,
       case pi_tar_rec.tar_type when c_tar_type_prepaid_prot then
       c_tar_type_prepaid_dic when c_tar_type_postpaid_prot then
       c_tar_type_postpaid_dic end,
       case pi_tar_rec.vdvd_type when c_tar_vdvd_type_gsm_prot then
       c_tar_vdvd_type_gsm_dic when c_tar_vdvd_type_pstn_prot then
       c_tar_vdvd_type_pstn_dic when c_tar_vdvd_type_cdma_prot then
       c_tar_vdvd_type_cdma_dic when c_tar_vdvd_type_nmt_prot then
       c_tar_vdvd_type_nmt_dic when c_tar_vdvd_type_adsl_prot then
       c_tar_vdvd_type_adsl_dic end,
       pi_ver_num,
       pi_ver_date,
       pi_imp_id,
       case pi_tar_rec.phone_federal when c_phone_federal_prot then
       c_phone_federal_dic when c_phone_city_prot then c_phone_city_dic else
       c_phone_federal_dic end,
       l_color,
       pi_tar_rec.cost,
       pi_tar_rec.advance,
       decode(pi_tar_rec.tech_tar, 0, 1, 0),
       decode(pi_tar_rec.tech_tar, -1, null, 0, null, null, null, -1),
       Decode(pi_tar_rec.equipment_required,
              1,
              4,
              2,
              -4,
              NVL(pi_tar_rec.equipment_required, 0)))
    returning ID into l_loc_tar_id;
  
    -- Добавляем параметры услуг для ТП
    sql_text := '
			insert into T_SERV_TAR2
				(SERV_ID,
				 TAR_ID,
				 SERV_COST,
				 SERV_ADVANCE,
				 STARTDATE,
				 CATEGORY,
				 IS_ENABLE,
				 IS_ON,
				 GROUP_ID,
				 PHONE_FEDERAL,
				 PHONE_COLOR,
         PACK_ID,
         DISC_ID,
         TYPE_TARIFF,
         TYPE)
        select distinct s.SERV_ID,
               :l_loc_tar_id,
               sc.COST,
               sc.ADVANCE,
               sc.STARTDATE,
               nvl(sc.CATEGORY, 2),
               sc.IS_ENABLE,
               sc.IS_ON,
               sg.GROUP_ID,
               case sc.PHONE_FEDERAL
                when :c_phone_federal_prot then
                 :c_phone_federal_dic
                when :c_phone_city_prot then
                 :c_phone_city_dic
                else
                 :c_phone_federal_dic
              end,
              nvl(cc.id,tmc_sim.get_color_default),
               sc.PACK_ID,
               sc.DISC_ID,
               sc.TYPE_TARIFF,
               sc.TYPE
          from ' || g_srv_c_tbl_nm ||
                ' sc,
               T_SERVICE2                      s,
               T_SERVICE_GROUP2                sg,
               T_EISSD_PACKAGE2                e,
               t_discount2                     d,
               t_dic_sim_color cc
         where s.REMOTE_SERV_ID(+) = sc.SRV_ID
           and (sc.SRV_ID is null or s.SERV_ASR_ID = :asr_id)
           and sc.GROUP_ID = sg.REMOTE_ID(+)
           and (sg.REMOTE_ID is null or sg.ASR_ID = :asr_id)
           and e.PACK_ID(+) = sc.PACK_ID
           and d.disc_id(+) = sc.disc_id
           and sc.phone_color = cc.ps_id(+)
           and (s.serv_id is null or
               s.serv_id = (select max(s2.serv_id)
                               from t_service2 s2
                              where s2.remote_serv_id = s.remote_serv_id
                                and s2.serv_asr_id = s.serv_asr_id))
           and sc.TAR_ID = :tar_id
           and sc.REGION_ID = :region_id';
    execute immediate sql_text
      using l_loc_tar_id, c_phone_federal_prot, c_phone_federal_dic, c_phone_city_prot, c_phone_city_dic, c_phone_federal_dic, /*c_phone_color_simple_prot, c_phone_color_simple_dic, c_phone_color_silver_prot, c_phone_color_silver_dic, c_phone_color_gold_prot, c_phone_color_gold_dic, c_phone_color_plat_prot, c_phone_color_plat_dic, c_phone_color_simple_dic,*/ pi_asr_id, pi_asr_id, pi_tar_rec.tar_id, pi_tar_rec.region_id;
  
    -- Добавляем связи между услугами в ТП
    sql_text := '
			insert into T_SERV_SERV2
				(SERV1_ID, SERV2_ID, REL_TYPE, TAR_ID, PACK_ID_1, PACK_ID_2)
				with SERV as
         (select s.remote_serv_id, max(s.serv_id) max_serv_id
            from t_service2 s
           where s.serv_asr_id = :asr_id
           group by s.remote_serv_id)
        select distinct s1.max_serv_id SERV_ID,
                        s2.max_serv_id SERV_ID,
                        case rs.REL_TYPE
                          when :c_serv_rel_type_par_prot then
                           :c_serv_rel_type_par_dic
                          when :c_serv_rel_type_enem_prot then
                           :c_serv_rel_type_enem_dic
                          when :c_serv_rel_type_both_prot then
                           :c_serv_rel_type_both_dic
                        end,
                        :l_loc_tar_id,
                        null PACK_ID_1,
                        null PACK_ID_2
          from mv_rel_service_service rs,
               serv                   s1,
               serv                   s2
         where s1.REMOTE_SERV_ID = rs.SRV_ID_1
           and s2.REMOTE_SERV_ID = rs.SRV_ID_2
           and rs.TAR_ID = :tar_id
           and rs.REGION_ID = :region_id
        union all
        select distinct null SERV_ID,
                        null SERV_ID,
                        case rs.REL_TYPE
                          when :c_serv_rel_type_par_prot then
                           :c_serv_rel_type_par_dic
                          when :c_serv_rel_type_enem_prot then
                           :c_serv_rel_type_enem_dic
                          when :c_serv_rel_type_both_prot then
                           :c_serv_rel_type_both_dic
                        end,
                        :l_loc_tar_id,
                        rs.PACK_ID_1,
                        rs.PACK_ID_2
          from mv_rel_service_service rs
         where rs.pack_id_1 is not null
           and rs.TAR_ID = :tar_id
           and rs.REGION_ID = :region_id';
    execute immediate sql_text
      using pi_asr_id, c_serv_rel_type_par_prot, c_serv_rel_type_par_dic, c_serv_rel_type_enem_prot, c_serv_rel_type_enem_dic, c_serv_rel_type_both_prot, c_serv_rel_type_both_dic, l_loc_tar_id, pi_tar_rec.tar_id, pi_tar_rec.region_id, c_serv_rel_type_par_prot, c_serv_rel_type_par_dic, c_serv_rel_type_enem_prot, c_serv_rel_type_enem_dic, c_serv_rel_type_both_prot, c_serv_rel_type_both_dic, l_loc_tar_id, pi_tar_rec.tar_id, pi_tar_rec.region_id;
    -- Добавляем ограничения
    sql_text := '
      insert into t_restrict_ability
      select *
        from ' || g_restrict_ab_tbl_nm;
    execute immediate sql_text;
    -- если импорт выполняется для Питерсервиса, добавим виртуальные услуги для подключения городских и цветных номеров
    if (pi_asr_id = 3) then
      Set_City_Color_ServicesPS(pi_tar_rec.tar_id,
                                pi_asr_id,
                                pi_tar_rec.region_id);
    end if;
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Add_Tariff');
      raise;
  end Add_Tariff;
  -------------------------------------------------------------------
  -- Синхронизация справочника услуг
  procedure Sync_Services(pi_asr_id in T_ASR.ASR_ID%type,
                          pi_imp_id in ASR_TAR_IMP_MD5.TIM_ID%type) is
    l_cur1         sys_refcursor;
    l_cur2         sys_refcursor;
    l_srv_rec      rec_srv;
    l_srv_old_md5  t_md5;
    l_srv_new_md5  t_md5;
    l_last_ver_num T_SERVICE2.SERV_VER_NUM%type := -1;
  begin
    open l_cur1 for 'select distinct *
           from ' || g_srv_tbl_nm;
    loop
      fetch l_cur1
        into l_srv_rec;
      exit when l_cur1%notfound;
      open l_cur2 for 'select distinct SRV_MD5
             from ' || g_srv_tbl_nm2 || '
           where SRV_ID = :srv_id'
        using l_srv_rec.srv_id;
      fetch l_cur2
        into l_srv_old_md5;
      if (l_cur2%found) then
        l_srv_new_md5 := Compute_MD5Hash_Of_Service(l_srv_rec.srv_name,
                                                    l_srv_rec.srv_description,
                                                    l_srv_rec.srv_type,
                                                    l_srv_rec.P_SERVICE,
                                                    l_srv_rec.P_SERVICE_DVO,
                                                    l_srv_rec.TAR_ID,
                                                    l_srv_rec.REGION_ID);
        if (l_srv_new_md5 <> l_srv_old_md5) then
          --формируем номер версии сервиса
          select max(SERV_VER_NUM)
            into l_last_ver_num
            from T_SERVICE2
           where REMOTE_SERV_ID = l_srv_rec.srv_id
             and SERV_ASR_ID = pi_asr_id;
          l_last_ver_num := l_last_ver_num + 1;
        end if;
      else
        l_last_ver_num := 1;
      end if;
    
      if (l_last_ver_num > -1) then
        ---- создаем новую версию сервиса
        insert into T_SERVICE2
          (REMOTE_SERV_ID,
           SERV_ASR_ID,
           SERV_NAME,
           SERV_DESC,
           SERV_TYPE,
           SERV_VER_NUM,
           SERV_IMP_ID,
           P_SERVICE,
           P_SERVICE_DVO,
           TAR_ID,
           REGION_ID)
        values
          (l_srv_rec.srv_id,
           pi_asr_id,
           l_srv_rec.srv_name,
           l_srv_rec.srv_description,
           l_srv_rec.srv_type,
           l_last_ver_num,
           pi_imp_id,
           l_srv_rec.P_SERVICE,
           l_srv_rec.P_SERVICE_DVO,
           l_srv_rec.TAR_ID,
           l_srv_rec.REGION_ID);
      end if;
      close l_cur2;
    end loop;
    close l_cur1;
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Sync_Services');
      raise;
  end Sync_Services;
  -------------------------------------------------------------------
  -- Синхронизация справочника групп услуг
  procedure Sync_Serv_Groups(pi_asr_id in T_ASR.ASR_ID%type,
                             pi_imp_id in ASR_TAR_IMP_MD5.TIM_ID%type) is
    l_cur1          sys_refcursor;
    l_cur2          sys_refcursor;
    l_srv_g_rec     rec_srv_g;
    l_srv_g_old_md5 t_md5;
    l_srv_g_new_md5 t_md5;
    l_last_ver_num  T_SERVICE2.SERV_VER_NUM%type := -1;
  begin
    g_srv_g_tbl_nm  := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                        'service_group');
    g_srv_g_tbl_nm2 := Protocol_Base.Get_DPA_Table_Name(pi_asr_id,
                                                        'service_group2');
  
    open l_cur1 for 'select *
           from ' || g_srv_g_tbl_nm;
    loop
      fetch l_cur1
        into l_srv_g_rec;
      exit when l_cur1%notfound;
      open l_cur2 for 'select GROUP_MD5
             from ' || g_srv_g_tbl_nm2 || '
           where GROUP_ID = :group_id'
        using l_srv_g_rec.group_id;
      fetch l_cur2
        into l_srv_g_old_md5;
      if (l_cur2%found) then
        l_srv_g_new_md5 := Compute_MD5Hash_Of_Serv_Group(l_srv_g_rec.group_name,
                                                         l_srv_g_rec.group_type,
                                                         l_srv_g_rec.is_necessary,
                                                         l_srv_g_rec.is_single_pay);
        if (l_srv_g_new_md5 <> l_srv_g_old_md5) then
          ---- создаем новую версию группы сервисов
          --формируем номер версии
          select max(VER_NUM)
            into l_last_ver_num
            from T_SERVICE_GROUP2
           where REMOTE_ID = l_srv_g_rec.group_id
             and ASR_ID = pi_asr_id;
          l_last_ver_num := l_last_ver_num + 1;
        end if;
      else
        l_last_ver_num := 1;
      end if;
      if (l_last_ver_num > -1) then
        ---- создаем новую версию группы сервисов
        insert into T_SERVICE_GROUP2
          (GROUP_NAME,
           GROUP_TYPE,
           IS_NECESSARY,
           IS_SINGLE_PAY,
           REMOTE_ID,
           ASR_ID,
           VER_NUM,
           IMP_ID)
        values
          (l_srv_g_rec.group_name,
           l_srv_g_rec.group_type,
           l_srv_g_rec.is_necessary,
           l_srv_g_rec.is_single_pay,
           l_srv_g_rec.group_id,
           pi_asr_id,
           l_last_ver_num,
           pi_imp_id);
      end if;
      close l_cur2;
    end loop;
    close l_cur1;
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Sync_Serv_Groups');
      raise;
  end Sync_Serv_Groups;
  -------------------------------------------------------------------
  -- Синхронизация справочника пакетов
  procedure Sync_Package(pi_asr_id in T_ASR.ASR_ID%type,
                         pi_imp_id in ASR_TAR_IMP_MD5.TIM_ID%type) is
    l_cur1         sys_refcursor;
    l_cur2         sys_refcursor;
    l_pack_rec     rec_pack;
    l_pack_old_md5 t_md5;
    l_pack_new_md5 t_md5;
    l_last_ver_num t_eissd_package2.Ver_Num%type := -1;
  begin
    open l_cur1 for 'select distinct *
           from ' || g_eissd_package_tbl_nm;
    loop
      fetch l_cur1
        into l_pack_rec;
      exit when l_cur1%notfound;
    
      open l_cur2 for 'select distinct PACK_MD5
             from ' || g_eissd_package_tbl_nm2 || '
           where PACK_ID = :pack_id'
        using l_pack_rec.pack_id;
      fetch l_cur2
        into l_pack_old_md5;
      if (l_cur2%found) then
        l_pack_new_md5 := Compute_MD5Hash_Of_package(l_pack_rec.PACK_NAME,
                                                     l_pack_rec.P_SERVICE,
                                                     l_pack_rec.TAR_ID,
                                                     l_pack_rec.REGION_ID);
        if (l_pack_new_md5 <> l_pack_old_md5) then
          --формируем номер версии сервиса
          begin
            select max(T.VER_NUM)
              into l_last_ver_num
              from T_EISSD_PACKAGE2 T
             where T.PACK_ID = l_pack_rec.PACK_ID
               and T.ASR_ID = pi_asr_id;
          exception
            when no_data_found then
              l_last_ver_num := 0;
          end;
          l_last_ver_num := l_last_ver_num + 1;
        end if;
      else
        l_last_ver_num := 1;
      end if;
    
      if l_last_ver_num > -1 then
        ---- создаем новую версию пакета
        insert into T_EISSD_PACKAGE2
          (PACK_ID,
           PACK_NAME,
           P_SERVICE,
           ASR_ID,
           IMP_ID,
           VER_NUM,
           TAR_ID,
           REGION_ID)
        values
          (l_pack_rec.PACK_ID,
           l_pack_rec.PACK_NAME,
           l_pack_rec.P_SERVICE,
           pi_asr_id,
           pi_imp_id,
           l_last_ver_num,
           l_pack_rec.TAR_ID,
           l_pack_rec.REGION_ID);
      end if;
      close l_cur2;
    end loop;
    close l_cur1;
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Sync_Package');
      raise;
  end Sync_Package;
  -------------------------------------------------------------------
  -- Синхронизация справочника скидок
  procedure Sync_Discount(pi_asr_id in T_ASR.ASR_ID%type,
                          pi_imp_id in ASR_TAR_IMP_MD5.TIM_ID%type) is
    l_cur1             sys_refcursor;
    l_cur2             sys_refcursor;
    l_discount_rec     rec_discount;
    l_discount_old_md5 t_md5;
    l_discount_new_md5 t_md5;
    l_last_ver_num     t_discount2.Ver_Num%type := -1;
  begin
    open l_cur1 for 'select *
           from ' || g_discount_tbl_nm;
    loop
      fetch l_cur1
        into l_discount_rec;
      exit when l_cur1%notfound;
      open l_cur2 for 'select DISC_MD5
             from ' || g_discount_tbl_nm2 || '
           where DISC_ID = :disc_id'
        using l_discount_rec.DISC_ID;
      fetch l_cur2
        into l_discount_old_md5;
      if (l_cur2%found) then
        l_discount_new_md5 := Compute_MD5Hash_Of_Discount(l_discount_rec.DISC_NAME,
                                                          l_discount_rec.P_SERVICE,
                                                          l_discount_rec.PACK_ID,
                                                          l_discount_rec.TAR_ID,
                                                          l_discount_rec.REGION_ID);
        if (l_discount_new_md5 <> l_discount_old_md5) then
          --формируем номер версии сервиса
          begin
            select max(T.VER_NUM)
              into l_last_ver_num
              from T_DISCOUNT2 T
             where T.DISC_ID = l_discount_rec.DISC_ID
               and T.ASR_ID = pi_asr_id;
          exception
            when no_data_found then
              l_last_ver_num := 0;
          end;
          l_last_ver_num := l_last_ver_num + 1;
        end if;
      else
        l_last_ver_num := 1;
      end if;
    
      if l_last_ver_num > -1 then
        ---- создаем новую версию пакета
        insert into T_DISCOUNT2
          (DISC_ID,
           DISC_NAME,
           P_SERVICE,
           PACK_ID,
           TAR_ID,
           REGION_ID,
           ASR_ID,
           VER_NUM,
           IMP_ID)
        values
          (l_discount_rec.DISC_ID,
           l_discount_rec.DISC_NAME,
           l_discount_rec.P_SERVICE,
           l_discount_rec.PACK_ID,
           l_discount_rec.TAR_ID,
           l_discount_rec.REGION_ID,
           pi_asr_id,
           l_last_ver_num,
           pi_imp_id);
      end if;
      close l_cur2;
    end loop;
    close l_cur1;
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Sync_Discount');
      raise;
  end Sync_Discount;
  -------------------------------------------------------------------
  -- Закрывает предыдущую версию ТП
  procedure Close_Tar_Ver(pi_asr_id    in T_ASR.ASR_ID%type,
                          pi_ver_date  in date,
                          pi_remote_id in T_ABSTRACT_TAR.AT_REMOTE_ID%type,
                          pi_region_id in T_ABSTRACT_TAR.AT_REGION_ID%type,
                          pi_imp_id    in ASR_TAR_IMP_MD5.TIM_ID%type) is
  begin
    update T_TARIFF2 t
       set VER_DATE_END = pi_ver_date, IMP_NEXT_ID = pi_imp_id
     where VER_DATE_END is null
       and t.AT_ID in (select at.AT_ID
                         from T_ABSTRACT_TAR at
                        where at.AT_ASR_ID = pi_asr_id
                          and at.AT_REMOTE_ID = pi_remote_id
                          and at.AT_REGION_ID = pi_region_id);
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Close_Tar_Ver');
      raise;
  end Close_Tar_Ver;
  -------------------------------------------------------------------
  -- Создает новую версию версию ТП
  procedure Add_Tar_Ver(pi_tar_rec  in rec_tar,
                        pi_asr_id   in t_abstract_tar.at_asr_id%type,
                        pi_imp_id   in t_tariff2.imp_id%type,
                        pi_ver_date in t_tariff2.ver_date_beg%type) is
    l_last_ver_num number;
    l_at_id        t_abstract_tar.at_id%type;
  begin
    select max(VER_NUM)
      into l_last_ver_num
      from T_TARIFF2 t, T_ABSTRACT_TAR at
     where t.AT_ID = at.AT_ID
       and at.AT_REMOTE_ID = pi_tar_rec.tar_id
       and at.AT_REGION_ID = pi_tar_rec.region_id
       and at.AT_ASR_ID = pi_asr_id;
    l_last_ver_num := nvl(l_last_ver_num, 0) + 1;
    -- найдем абстрактный идентификатор ТП
    Begin
      select AT_ID
        into l_at_id
        from T_ABSTRACT_TAR
       where AT_REMOTE_ID = pi_tar_rec.tar_id
         and AT_REGION_ID = pi_tar_rec.region_id
         and AT_ASR_ID = pi_asr_id;
    Exception
      -- Если данных в ЕИССД о данном ТП нет.
      when others then
        l_at_id := null;
    end;
    -- добавим новую версию ТП
    Add_Tariff(pi_asr_id,
               l_at_id,
               pi_tar_rec,
               pi_ver_date,
               l_last_ver_num,
               pi_imp_id);
    -- Обновим стоимость ТМЦ
    Sync_Cost_New_Tariff2(l_at_id, pi_imp_id, pi_ver_date);
  end Add_Tar_Ver;
  ----------------------------------------------------------------------------------------------------
  -- Импорт услуг для закрытых ТП
  ----------------------------------------------------------------------------------------------------
  procedure addServiceCloseTar(pi_asr_id   in T_ASR.ASR_ID%type,
                               pi_tar_rec  in rec_tar,
                               pi_ver_date in date,
                               pi_ver_num  in T_TARIFF2.VER_NUM%type,
                               pi_imp_id   in ASR_TAR_IMP_MD5.TIM_ID%type) is
    l_at_id      T_ABSTRACT_TAR.AT_ID%type;
    l_loc_tar_id T_TARIFF2.ID%type;
    sql_text     t_sql;
  begin
    -- Достаем ИД тарифа
    select tat.at_id, tat.id
      into l_at_id, l_loc_tar_id
      from t_tarif_by_at_id tat
     where tat.at_remote_id = pi_tar_rec.tar_id
       and tat.at_region_id = pi_tar_rec.region_id;
  
    -- Добавляем параметры услуг для ТП
    sql_text := '
    MERGE INTO T_SERV_TAR2 T
    USING (select distinct s.SERV_ID,
                           :l_loc_tar_id TAR_ID,
                           sc.COST SERV_COST,
                           sc.ADVANCE SERV_ADVANCE,
                           sc.STARTDATE,
                           nvl(sc.CATEGORY, 2) CATEGORY,
                           sc.IS_ENABLE,
                           sc.IS_ON,
                           sg.GROUP_ID,
                           case sc.PHONE_FEDERAL
                             when :c_phone_federal_prot then
                              :c_phone_federal_dic
                             when :c_phone_city_prot then
                              :c_phone_city_dic
                             else
                              :c_phone_federal_dic
                           end PHONE_FEDERAL,
                           nvl(cc.id,tmc_sim.get_color_default) PHONE_COLOR,
                           sc.PACK_ID,
                           sc.DISC_ID,
                           sc.TYPE_TARIFF,
                           sc.TYPE
             from ' || g_srv_c_tbl_nm ||
                ' sc,
                  T_SERVICE2 s,
                  T_SERVICE_GROUP2 sg,
                  T_EISSD_PACKAGE2 e,
                  t_discount2 d,
                  t_dic_sim_color cc
            where s.REMOTE_SERV_ID(+) = sc.SRV_ID
              and (sc.SRV_ID is null or s.SERV_ASR_ID = :asr_id)
              and sc.GROUP_ID = sg.REMOTE_ID(+)
              and (sg.REMOTE_ID is null or sg.ASR_ID = :asr_id)
              and e.PACK_ID(+) = sc.PACK_ID
              and d.disc_id(+) = sc.disc_id
              and sc.PHONE_COLOR = cc.ps_id(+)
              and (s.serv_id is null or
                   s.serv_id =
                   (select max(s2.serv_id)
                      from t_service2 s2
                     where s2.remote_serv_id = s.remote_serv_id
                       and s2.serv_asr_id = s.serv_asr_id))
              and sc.TAR_ID = :tar_id
              and sc.REGION_ID = :region_id
              and (e.pack_id is null or e.tar_id = :tar_id)
              and (d.disc_id is null or d.tar_id = :tar_id)) T_NEW
    ON (NVL(T_NEW.TAR_ID, -2) = NVL(T.TAR_ID, -2) AND NVL(T_NEW.SERV_ID, -2) = NVL(T.SERV_ID, -2)
        AND NVL(T_NEW.PACK_ID, -2) = NVL(T.PACK_ID, -2) AND NVL(T_NEW.DISC_ID, -2) = NVL(T.DISC_ID, -2))
    WHEN NOT MATCHED THEN
      INSERT
        (T.SERV_ID,
         T.TAR_ID,
         T.SERV_COST,
         T.SERV_ADVANCE,
         T.STARTDATE,
         T.CATEGORY,
         T.IS_ENABLE,
         T.IS_ON,
         T.GROUP_ID,
         T.PHONE_FEDERAL,
         T.PHONE_COLOR,
         T.PACK_ID,
         T.DISC_ID,
         T.TYPE_TARIFF,
         T.TYPE)
      VALUES
        (T_NEW.SERV_ID,
         T_NEW.TAR_ID,
         T_NEW.SERV_COST,
         T_NEW.SERV_ADVANCE,
         T_NEW.STARTDATE,
         T_NEW.CATEGORY,
         T_NEW.IS_ENABLE,
         T_NEW.IS_ON,
         T_NEW.GROUP_ID,
         T_NEW.PHONE_FEDERAL,
         T_NEW.PHONE_COLOR,
         T_NEW.PACK_ID,
         T_NEW.DISC_ID,
         T_NEW.TYPE_TARIFF,
         T_NEW.TYPE)';
    execute immediate sql_text
      using l_loc_tar_id, c_phone_federal_prot, c_phone_federal_dic, c_phone_city_prot, c_phone_city_dic, c_phone_federal_dic, /*c_phone_color_simple_prot, c_phone_color_simple_dic, c_phone_color_silver_prot, c_phone_color_silver_dic, c_phone_color_gold_prot, c_phone_color_gold_dic, c_phone_color_plat_prot, c_phone_color_plat_dic, c_phone_color_simple_dic,*/ pi_asr_id, pi_asr_id, pi_tar_rec.tar_id, pi_tar_rec.region_id, pi_tar_rec.tar_id, pi_tar_rec.tar_id;
    -- Добавляем связи между услугами в ТП
    sql_text := '
        MERGE INTO T_SERV_SERV2 SS
        USING (
          with SERV as
           (select s.remote_serv_id, max(s.serv_id) max_serv_id
              from t_service2 s
             where s.serv_asr_id = :asr_id
             group by s.remote_serv_id)
          select distinct s1.max_serv_id SERV1_ID,
                          s2.max_serv_id SERV2_ID,
                          case rs.REL_TYPE
                            when :c_serv_rel_type_par_prot then
                             :c_serv_rel_type_par_dic
                            when :c_serv_rel_type_enem_prot then
                             :c_serv_rel_type_enem_dic
                            when :c_serv_rel_type_both_prot then
                             :c_serv_rel_type_both_dic
                          end REL_TYPE,
                          :l_loc_tar_id  TAR_ID,
                          null PACK_ID_1,
                          null PACK_ID_2
            from mv_rel_service_service rs, serv s1, serv s2
           where s1.REMOTE_SERV_ID = rs.SRV_ID_1
             and s2.REMOTE_SERV_ID = rs.SRV_ID_2
             and rs.TAR_ID = :tar_id
             and rs.REGION_ID = :region_id
          union all
          select distinct null SERV_ID,
                          null SERV_ID,
                          case rs.REL_TYPE
                            when :c_serv_rel_type_par_prot then
                             :c_serv_rel_type_par_dic
                            when :c_serv_rel_type_enem_prot then
                             :c_serv_rel_type_enem_dic
                            when :c_serv_rel_type_both_prot then
                             :c_serv_rel_type_both_dic
                          end,
                          :l_loc_tar_id TAR_ID,
                          rs.PACK_ID_1,
                          rs.PACK_ID_2
            from mv_rel_service_service rs
           where rs.pack_id_1 is not null
             and rs.TAR_ID = :tar_id
             and rs.REGION_ID = :region_id) S
            ON (NVL(S.SERV1_ID, -2) = NVL(SS.SERV1_ID, -2) AND NVL(S.SERV2_ID, -2) = NVL(SS.SERV2_ID, -2)
               AND NVL(S.REL_TYPE, -2) = NVL(SS.REL_TYPE, -2) AND NVL(S.TAR_ID, -2) = NVL(SS.TAR_ID, -2)
               AND NVL(S.PACK_ID_1, -2) = NVL(SS.PACK_ID_1, -2) AND NVL(S.PACK_ID_2, -2) = NVL(SS.PACK_ID_2, -2))
           WHEN NOT MATCHED THEN
            INSERT
              (SS.SERV1_ID,
               SS.SERV2_ID,
               SS.REL_TYPE,
               SS.TAR_ID,
               SS.PACK_ID_1,
               SS.PACK_ID_2)
            VALUES
              (S.SERV1_ID,
               S.SERV2_ID,
               S.REL_TYPE,
               S.TAR_ID,
               S.PACK_ID_1,
               S.PACK_ID_2)';
    execute immediate sql_text
      using pi_asr_id, c_serv_rel_type_par_prot, c_serv_rel_type_par_dic, c_serv_rel_type_enem_prot, c_serv_rel_type_enem_dic, c_serv_rel_type_both_prot, c_serv_rel_type_both_dic, l_loc_tar_id, pi_tar_rec.tar_id, pi_tar_rec.region_id, c_serv_rel_type_par_prot, c_serv_rel_type_par_dic, c_serv_rel_type_enem_prot, c_serv_rel_type_enem_dic, c_serv_rel_type_both_prot, c_serv_rel_type_both_dic, l_loc_tar_id, pi_tar_rec.tar_id, pi_tar_rec.region_id;
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Add_Tariff');
      raise;
  end addServiceCloseTar;
  -------------------------------------------------------------------
  -- Синхронизация тарифных планов
  procedure Sync_Tariffs_PS(pi_asr_id in T_ASR.ASR_ID%type,
                            pi_imp_id in ASR_TAR_IMP_MD5.TIM_ID%type,
                            pi_date   in date) is
    l_tar_rec1     rec_tar;
    l_tar_rec2     rec_tar;
    l_tar_rec1_1   rec_tar;
    l_cur1         sys_refcursor;
    l_cur1_1       sys_refcursor;
    l_cur2         sys_refcursor;
    l_new_tar_md5  t_md5;
    l_last_ver_num T_TARIFF2.VER_NUM%type;
    l_at_id        T_ABSTRACT_TAR.AT_ID%type;
    l_ver_date     date := NVL(pi_date, sysdate);
    l_count        pls_integer;
    l_cur_time     date;
    l_countf       number;
    l_changed      boolean;
    l_active       number;
    sql_text       varchar2(4000);
    begin
      l_cur_time := pi_date;

      open l_cur1 for 'select *
           from ' || g_tar_tbl_nm2 || '
         where (phone_federal is null or phone_federal = 1)
           and (phone_color is null or phone_color = 1)
           and is_active = 1';
      loop
        fetch l_cur1
        into l_tar_rec1;
        exit when l_cur1%notfound;
        l_countf  := 0;
        l_changed := false;
        l_active  := 1;
        open l_cur1_1 for 'select *
             from ' || g_tar_tbl_nm2 || '
           where tar_id = :tar_id
             and region_id = :region_id
             and region_id is not null'
        using l_tar_rec1.tar_id, l_tar_rec1.region_id;
        loop
          fetch l_cur1_1
          into l_tar_rec1_1;
          exit when l_cur1_1%notfound;
          open l_cur2 for '
					select TAR_ID,
								 REGION_ID,
								 TITLE,
								 STARTDATE,
								 IS_FOR_DEALER,
								 nvl(category, 2),
								 IS_ACTIVE,
								 TAR_TYPE,
								 PAY_TYPE,
								 VDVD_TYPE,
								 null,
								 PHONE_FEDERAL,
								 PHONE_COLOR,
								 COST,
								 ADVANCE,
								 TECH_TAR,
								 decode(equipment_required, 1, 4, 2, -4, NVL(equipment_required, 0))
						from ' || g_tar_tbl_nm || ' t
					 where TAR_ID = :tar_id
						 and REGION_ID = :region_id
             and region_id is not null
						 and PHONE_FEDERAL = :phone_federal
						 and PHONE_COLOR = :phone_color'
          using l_tar_rec1_1.tar_id, l_tar_rec1_1.region_id, l_tar_rec1_1.phone_federal, l_tar_rec1_1.phone_color;
          fetch l_cur2
          into l_tar_rec2;
          if (l_cur2%found) then
            l_active      := nvl(l_tar_rec2.is_active, 0);
            l_countf      := l_countf + 1;
            l_new_tar_md5 := Compute_MD5Hash_Of_Tariff(l_tar_rec2.tar_id,
                                                       l_tar_rec2.region_id,
                                                       l_tar_rec2.phone_federal,
                                                       l_tar_rec2.phone_color);
            if (l_tar_rec1_1.tar_md5 is null or
                l_new_tar_md5 <> l_tar_rec1_1.tar_md5) then
              l_changed := true;
            end if;
          else
            l_changed := true; -- не нашли запись о ТП с данным типом номера, поэтому обновляем ТП в ЕИССД
          end if;
          close l_cur2;
        end loop;
        close l_cur1_1;
        if (l_countf <> 0) then
          if (l_changed) then
            -- закроем предыдущую версию ТП
            Close_Tar_Ver(pi_asr_id,
                          l_ver_date,
                          l_tar_rec1.tar_id,
                          l_tar_rec1.region_id,
                          pi_imp_id);
            if l_active = 1 then
              -- создаем новую версию ТП
              Add_Tar_Ver(l_tar_rec2, pi_asr_id, pi_imp_id, l_ver_date);
            end if;
          end if;
        else
          -- Тариф не найден в новой выгрузке - закрываем версию тарифа
          l_last_ver_num := -1;
          Close_Tar_Ver(pi_asr_id,
                        l_ver_date,
                        l_tar_rec1.tar_id,
                        l_tar_rec1.region_id,
                        pi_imp_id);
        end if;
      end loop;
      close l_cur1;
      -- Если в текущей выгрузке появились новые ТП,
      -- то просто добавляем их в ЕИССД
      open l_cur1 for '
			select TAR_ID,
						 REGION_ID,
						 TITLE,
						 STARTDATE,
						 IS_FOR_DEALER,
						 nvl(category, 2),
						 IS_ACTIVE,
						 TAR_TYPE,
						 PAY_TYPE,
						 VDVD_TYPE,
						 null,
						 PHONE_FEDERAL,
						 PHONE_COLOR,
						 COST,
						 ADVANCE,
						 TECH_TAR,
						 decode(equipment_required, 1, 4, 2, -4, NVL(equipment_required, 0))
				from ' || g_tar_tbl_nm || ' t ' || '
			 where (phone_federal is null or phone_federal = 1)
				 and (phone_color is null or phone_color = 1)
         and region_id is not null';
      loop
        fetch l_cur1
        into l_tar_rec1;
        exit when l_cur1%notfound;
        l_active      := nvl(l_tar_rec1.is_active, 0);
        l_changed := false;
        l_countf  := 0;
        open l_cur1_1 for '
				select TAR_ID,
							 REGION_ID,
							 TITLE,
							 STARTDATE,
							 IS_FOR_DEALER,
							 nvl(category, 2),
							 IS_ACTIVE,
							 TAR_TYPE,
							 PAY_TYPE,
							 VDVD_TYPE,
							 null,
							 PHONE_FEDERAL,
							 PHONE_COLOR,
							 COST,
							 ADVANCE,
							 TECH_TAR,
							 decode(equipment_required, 1, 4, 2, -4, NVL(equipment_required, 0))
					from ' || g_tar_tbl_nm || ' t ' || '
				 where tar_id = :tar_id
					 and region_id = :region_id
           and region_id is not null'
        using l_tar_rec1.tar_id, l_tar_rec1.region_id;
        loop
          fetch l_cur1_1
          into l_tar_rec1_1;
          exit when l_cur1_1%notfound;
          open l_cur2 for '
					select count(*)
						into :l_count
						from ' || g_tar_tbl_nm2 || '
					 where tar_id = :tar_id
						 and region_id = :region_id
             and region_id is not null
						 and phone_federal = :phone_federal
						 and phone_color = :phone_color'
          using l_tar_rec1_1.tar_id, l_tar_rec1_1.region_id, l_tar_rec1_1.phone_federal, l_tar_rec1_1.phone_color;
          fetch l_cur2
          into l_count;
          close l_cur2;
          if (l_count != 0) then
            l_countf := l_count;
          else
            l_changed := true;
          end if;
        end loop;
        close l_cur1_1;

        if (l_countf != 0) then
          -- найдена хотя бы одна запись о ТП
          if (l_changed) then
            -- осутствовал один из вариантов ТП (для федерального, городского, сер. или золотого номера)
            -- закроем старую версию ТП
            Close_Tar_Ver(pi_asr_id,
                          l_ver_date,
                          l_tar_rec1.tar_id,
                          l_tar_rec1.region_id,
                          pi_imp_id);

            if l_active = 1 then
              -- создадим новую версию ТП
              Add_Tar_Ver(l_tar_rec1, pi_asr_id, pi_imp_id, l_ver_date);
            end if;
          end if;
        else
          -- Если тариф отсутствовал в предыдущей выгрузке, т.е. уже закрыт,
          -- присутствует в новой и встречался раньше, то мы продолжим
          -- использовать его at_id
          select count(*)
          into l_count
          from t_abstract_tar at
          where at.at_asr_id = pi_asr_id
          and at.at_remote_id = l_tar_rec1.tar_id
          and at.at_region_id = l_tar_rec1.region_id;

          if (l_count > 0) then
            ---- создаем новую версию ТП
            --найдем предыдущий номер версии ТП
            select nvl(max(VER_NUM), 0)
            into l_last_ver_num
            from T_TARIFF2 t, T_ABSTRACT_TAR at
            where t.AT_ID = at.AT_ID
            and at.AT_REMOTE_ID = l_tar_rec1.tar_id
            and at.AT_REGION_ID = l_tar_rec1.region_id
            and at.AT_ASR_ID = pi_asr_id;
            if (l_last_ver_num is null) then
              l_last_ver_num := l_last_ver_num;
            end if;
            l_last_ver_num := l_last_ver_num + 1;
            -- закроем предыдущую версию ТП
            Close_Tar_Ver(pi_asr_id,
                          l_ver_date,
                          l_tar_rec1.tar_id,
                          l_tar_rec1.region_id,
                          pi_imp_id);
            -- найдем абстрактный идентификатор ТП
            select AT_ID
            into l_at_id
            from T_ABSTRACT_TAR
            where AT_REMOTE_ID = l_tar_rec1.tar_id
                  and AT_REGION_ID = l_tar_rec1.region_id
                  and AT_ASR_ID = pi_asr_id;
            if l_active = 1 then
              -- добавим новую версию ТП
              Add_Tariff(pi_asr_id,
                         l_at_id,
                         l_tar_rec1,
                         l_ver_date,
                         l_last_ver_num,
                         pi_imp_id);
            end if;
            -- Обновим стоимость ТМЦ
            Sync_Cost_New_Tariff2(l_at_id, pi_imp_id, l_cur_time);
          else
            -- тариф никогда не встречался
            -- добавим 1-ю версию ТП
            l_last_ver_num := 1;
            Add_Tariff(pi_asr_id,
                       null,
                       l_tar_rec1,
                       l_ver_date,
                       l_last_ver_num,
                       pi_imp_id);
          end if;
        end if;
      end loop;
      close l_cur1;
      -- Сформируем ссылки на технологический ТП
      sql_text := '
			update t_tariff2 t2
				 set t2.tech_at_id = (select atar2.at_id
																from t_abstract_tar atar,
																		 t_abstract_tar atar2,
																		 ' || g_tar_tbl_nm || ' dt ' || '
															 where t2.at_id = atar.at_id
                                 and region_id is not null
																 and atar.at_remote_id = dt.tar_id
																 and dt.tech_tar = atar2.at_remote_id
																 and dt.phone_federal = 1
																 and dt.phone_color = 1
                                 and atar.AT_ASR_ID=' ||
                  pi_asr_id || '
                                 and atar2.AT_ASR_ID=' ||
                  pi_asr_id || ')
			 where t2.tech_at_id = -1';
      begin
        execute immediate sql_text;
        exception
        when others then
        null;
      end;
      -- Загружаем услуги для закрытых ТП
      addServiceAllCloseTar(pi_asr_id);
    end Sync_Tariffs_PS;
  -------------------------------------------------------------------
  -- Синхронизация тарифных планов
  procedure Sync_Tariffs(pi_asr_id in T_ASR.ASR_ID%type,
                         pi_imp_id in ASR_TAR_IMP_MD5.TIM_ID%type,
                         pi_date   in date) is
    l_tar_rec1     rec_tar;
    l_tar_rec2     rec_tar;
    l_cur1         sys_refcursor;
    l_cur2         sys_refcursor;
    l_new_tar_md5  t_md5;
    l_last_ver_num T_TARIFF2.VER_NUM%type;
    l_at_id        T_ABSTRACT_TAR.AT_ID%type;
    l_ver_date     date := nvl(pi_date, sysdate);
    l_count        pls_integer;
    l_cur_time     date;
  begin
    l_cur_time := pi_date;
    open l_cur1 for '
			select *
				from ' || g_tar_tbl_nm2 || '
			 where (phone_federal is null or phone_federal = 1)
				 and (phone_color is null or phone_color = 1)';
    loop
      fetch l_cur1
        into l_tar_rec1;
      exit when l_cur1%notfound;
      open l_cur2 for '
				select TAR_ID,
							 REGION_ID,
							 TITLE,
							 STARTDATE,
							 IS_FOR_DEALER,
							 nvl(category, 2),
							 IS_ACTIVE,
							 TAR_TYPE,
							 PAY_TYPE,
							 VDVD_TYPE,
							 null,
							 PHONE_FEDERAL,
							 PHONE_COLOR,
							 COST,
							 ADVANCE
					from ' || g_tar_tbl_nm || ' t
				 where TAR_ID = :tar_id
					 and REGION_ID = :region_id
					 and (PHONE_FEDERAL is null or PHONE_FEDERAL = 1)
					 and (PHONE_COLOR is null or PHONE_COLOR = 1)'
        using l_tar_rec1.tar_id, l_tar_rec1.region_id;
      fetch l_cur2
        into l_tar_rec2;
    
      if (l_cur2%found) then
        l_new_tar_md5 := Compute_MD5Hash_Of_Tariff(l_tar_rec2.tar_id,
                                                   l_tar_rec2.region_id);
        if (l_tar_rec1.tar_md5 is null or
           l_new_tar_md5 <> l_tar_rec1.tar_md5) then
          ---- создаем новую версию ТП
          --найдем предыдущий номер версии ТП
          select max(VER_NUM)
            into l_last_ver_num
            from T_TARIFF2 t, T_ABSTRACT_TAR at
           where t.AT_ID = at.AT_ID
             and at.AT_REMOTE_ID = l_tar_rec1.tar_id
             and at.AT_REGION_ID = l_tar_rec1.region_id
             and at.AT_ASR_ID = pi_asr_id;
        
          l_last_ver_num := l_last_ver_num + 1;
        
          -- закроем предыдущую версию ТП
          Close_Tar_Ver(pi_asr_id,
                        l_ver_date,
                        l_tar_rec1.tar_id,
                        l_tar_rec1.region_id,
                        pi_imp_id);
          -- найдем абстрактный идентификатор ТП
          select AT_ID
            into l_at_id
            from T_ABSTRACT_TAR
           where AT_REMOTE_ID = l_tar_rec1.tar_id
             and AT_REGION_ID = l_tar_rec1.region_id
             and AT_ASR_ID = pi_asr_id;
          -- добавим новую версию ТП
          Add_Tariff(pi_asr_id,
                     l_at_id,
                     l_tar_rec2,
                     l_ver_date,
                     l_last_ver_num,
                     pi_imp_id);
          Sync_Cost_New_Tariff2(l_at_id, pi_imp_id, l_cur_time);
        end if;
      else
        -- данный тариф не найден в новой выгрузке -
        -- закрываем версию данного тарифа
        l_last_ver_num := -1;
        Close_Tar_Ver(pi_asr_id,
                      l_ver_date,
                      l_tar_rec1.tar_id,
                      l_tar_rec1.region_id,
                      pi_imp_id);
      end if;
      close l_cur2;
    end loop;
    close l_cur1;
    -- Если в текущей выгрузке появились новые ТП,
    -- то просто добавляем их в ЕИССД
    open l_cur1 for '
			select TAR_ID,
						 REGION_ID,
						 TITLE,
						 STARTDATE,
						 IS_FOR_DEALER,
						 nvl(category, 2),
						 IS_ACTIVE,
						 TAR_TYPE,
						 PAY_TYPE,
						 VDVD_TYPE,
						 null,
						 PHONE_FEDERAL,
						 PHONE_COLOR,
						 COST,
						 ADVANCE
				from ' || g_tar_tbl_nm || ' t ' || '
			 where (PHONE_FEDERAL is null or PHONE_FEDERAL = 1)
				 and (PHONE_COLOR is null or PHONE_COLOR = 1)';
    loop
      fetch l_cur1
        into l_tar_rec1;
      exit when l_cur1%notfound;
      open l_cur2 for '
				select count(*)
					into :l_count
					from ' || g_tar_tbl_nm2 || '
				 where tar_id = :tar_id
					 and region_id = :region_id
					 and (phone_federal is null or phone_federal = 1)
					 and (phone_color is null or phone_color = 1)'
        using l_tar_rec1.tar_id, l_tar_rec1.region_id;
      fetch l_cur2
        into l_count;
      close l_cur2;
      if (l_count = 0) then
        ------------- Genie update (01.04.2008) ---------------------------
        -- Если тариф отсутствовал в предыдущей выгрузке, т.е. уже закрыт,
        -- присутствует в новой и встречался раньше, то мы продолжим
        -- использовать его at_id
        select count(*)
          into l_count
          from t_abstract_tar at
         where at.at_asr_id = pi_asr_id
           and at.at_remote_id = l_tar_rec1.tar_id
           and at.at_region_id = l_tar_rec1.region_id;
        if (l_count > 0) then
          ---- создаем новую версию ТП
          --найдем предыдущий номер версии ТП
          select nvl(max(VER_NUM), 0)
            into l_last_ver_num
            from T_TARIFF2 t, T_ABSTRACT_TAR at
           where t.AT_ID = at.AT_ID
             and at.AT_REMOTE_ID = l_tar_rec1.tar_id
             and at.AT_REGION_ID = l_tar_rec1.region_id
             and at.AT_ASR_ID = pi_asr_id;
          if (l_last_ver_num is null) then
            l_last_ver_num := l_last_ver_num;
          end if;
          l_last_ver_num := l_last_ver_num + 1;
          -- закроем предыдущую версию ТП
          Close_Tar_Ver(pi_asr_id,
                        l_ver_date,
                        l_tar_rec1.tar_id,
                        l_tar_rec1.region_id,
                        pi_imp_id);
          -- найдем абстрактный идентификатор ТП
          select AT_ID
            into l_at_id
            from T_ABSTRACT_TAR
           where AT_REMOTE_ID = l_tar_rec1.tar_id
             and AT_REGION_ID = l_tar_rec1.region_id
             and AT_ASR_ID = pi_asr_id;
          -- добавим новую версию ТП
          Add_Tariff(pi_asr_id,
                     l_at_id,
                     l_tar_rec1,
                     l_ver_date,
                     l_last_ver_num,
                     pi_imp_id);
          Sync_Cost_New_Tariff2(l_at_id, pi_imp_id, l_cur_time);
        else
          l_last_ver_num := 1;
          Add_Tariff(pi_asr_id,
                     null,
                     l_tar_rec1,
                     l_ver_date,
                     l_last_ver_num,
                     pi_imp_id);
        end if;
      end if;
    end loop;
    close l_cur1;
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Sync_Tariffs');
      raise;
  end Sync_Tariffs;
  -------------------------------------------------------------------
  procedure Add_tariff_by_at_id is
  begin
    delete from t_tarif_by_at_id;
    insert into t_tarif_by_at_id
      (at_region_id,
       at_asr_id,
       at_remote_id,
       at_seg_id,
       id,
       start_date,
       title,
       is_for_dealer,
       category,
       pay_type,
       type_vdvd_id,
       tariff_type,
       end_date,
       ver_num,
       at_id,
       ver_date_beg,
       ver_date_end,
       imp_id,
       imp_next_id,
       phone_federal,
       phone_color,
       cost,
       advance,
       equipment_required,
       is_tech,
       tech_at_id)
      select at.at_region_id,
             at.at_asr_id,
             at.at_remote_id,
             at.at_seg_id,
             tar.id,
             tar.start_date,
             tar.title,
             nvl(tar.is_for_dealer, th.is_for_dealer),
             nvl(tar.category, th.category),
             tar.pay_type,
             tar.type_vdvd_id,
             tar.tariff_type,
             tar.end_date,
             tar.ver_num,
             tar.at_id,
             tar.ver_date_beg,
             tar.ver_date_end,
             tar.imp_id,
             tar.imp_next_id,
             tar.phone_federal,
             tar.phone_color,
             tar.cost,
             tar.advance,
             tar.equipment_required,
             tar.is_tech,
             tar.tech_at_id
        from t_abstract_tar at
        join t_tariff2 tar
          on (at.at_id = tar.at_id)
        left join t_tariff2_history th
          on (tar.at_id = th.at_id and th.is_actual = 1)
       where tar.id in (select max(id) keep(dense_rank first order by at_id, ver_num desc)
                          from t_tariff2
                         group by at_id);
  end Add_tariff_by_at_id;
  --=================================================================================================
  -- Импорт тарифных планов (Точка входа)
  function Tariff_Import_Sync(pi_asr_id in T_ASR.ASR_ID%type,
                              pi_date   in date := sysdate) return boolean is
    l_prev_full_md5     t_md5; -- полный хэш предыдущей выгрузки
    l_prev_serv_md5     t_md5; -- хэш справочника услуг
    l_prev_gr_md5       t_md5; -- хэш справочника групп услуг
    l_prev_pack_md5     t_md5; -- хэш справочника пакетов
    l_prev_discount_md5 t_md5; -- хэш справочника скидок
  
    l_new_full_md5     t_md5; -- полный хэш текущей выгрузки
    l_new_serv_md5     t_md5; -- хэш справочника услуг из тек. выгрузки
    l_new_gr_md5       t_md5; -- хэш справочника групп услуг из тек. выгрузки
    l_new_pack_md5     t_md5; -- хэш справочника пакетов из тек. выгрузки
    l_new_discount_md5 t_md5; -- хэш справочника скидок из тек. выгрузки
    l_updated          boolean; -- флаг, означающий, что выгрузка содержит новые данные
    l_cur_imp_id       ASR_TAR_IMP_MD5.TIM_ID%type; -- идентификатор текущей выгрузки данных
    l_recipients       varchar2(255) := 'MAIN';
    l_msg              varchar2(8000);
    l_msg_reg          varchar2(8000);
    l_subject          varchar2(255) := 'Сеанс загрузки тарифных планов от АСР';
  begin
    l_updated := true;
    -- формируем полные имена таблиц
    Set_Dpa_Tables_Names(pi_asr_id);
    -- Пересчитаем вьюху
    dbms_mview.refresh('MV_REL_SERVICE_SERVICE');
    -- 1. Получим хэши предыдущей выгрузки (если была)
    begin
      select tim_full_md5,
             tim_serv_md5,
             tim_gr_md5,
             t.tim_pack_md5,
             t.tim_discount_md5
        into l_prev_full_md5,
             l_prev_serv_md5,
             l_prev_gr_md5,
             l_prev_pack_md5,
             l_prev_discount_md5
        from asr_tar_imp_md5 t
       where tim_id = (select max(tim_id)
                         from asr_tar_imp_md5
                        where tim_asr_id = pi_asr_id)
         for update nowait;
    exception
      when no_data_found then
        null;
    end;
    -- получим идентификатор выгрузки
    select seq_asr_tar_imp_md5.nextval into l_cur_imp_id from dual;
    --l_prev_full_md5 := null; на боевой первый раз запустить с этой строкой
    if (l_prev_full_md5 is null) then
      -- выгрузок не было, поэтому просто выгружаем данные в
      --  еиссд и создаем первую версию
      Tariff_Import_Full(pi_asr_id, l_cur_imp_id, pi_date);
    else
      -- 2. Посчитаем хэш новой выгрузки и сравним со старым
      l_new_full_md5 := Compute_MD5Hash_Of_Tariffs();
      if (l_new_full_md5 = l_prev_full_md5) then
        -- Хэши совпадают, значит выгрузка идентична предыдущей
        l_updated := false;
        l_msg     := 'Обновлениий не было';
        l_subject := l_subject || '. Изменений нет';
      elsif l_new_full_md5 = 'd41d8cd98f00b204e9800998ecf8427e' then
        l_updated := false;
        l_msg     := 'Транспортные таблице не заполнены.';
        l_subject := l_subject || '. Транспортные таблице не заполнены.';
      else
        l_msg     := 'Полные хеши не совпадают' || utl_tcp.CRLF || '
select get_tariff_cost(tar.id) tar_cost,
       at.at_id,
       tar.title,
       at.at_region_id,
       tar.ver_date_beg,
       tar.ver_num
  from t_abstract_tar at, t_tariff2 tar
 where at.at_id = tar.at_id
   and tar.ver_date_end is null
 order by tar.ver_date_beg desc, at.at_region_id;

select *
  from (select f.title,
               f.ver_date_beg ver_beg,
               f.ver_date_end ver_end,
               lag(f_1) over(order by at_id, ver_num) f_1_p,
               f_1,
               lag(f_2) over(order by at_id, ver_num) f_2_p,
               f_2,
               lag(f_3) over(order by at_id, ver_num) f_3_p,
               f_3,
               lag(f_4) over(order by at_id, ver_num) f_4_p,
               f_4,
               lag(c_1) over(order by at_id, ver_num) c_1_p,
               c_1,
               lag(c_2) over(order by at_id, ver_num) c_2_p,
               c_2,
               lag(c_3) over(order by at_id, ver_num) c_3_p,
               c_3,
               lag(c_4) over(order by at_id, ver_num) c_4_p,
               c_4,
               lag(at_id) over(order by at_id, ver_num) at_id_prev,
               f.at_id,
               f.ver_num
          from (select get_tariff_cost(t.id, 1, 9001, 9101) f_1,
                       get_tariff_cost(t.id, 1, 9001, 9102) f_2,
                       get_tariff_cost(t.id, 1, 9001, 9103) f_3,
                       get_tariff_cost(t.id, 1, 9001, 9104) f_4,
                       get_tariff_cost(t.id, 1, 9002, 9101) c_1,
                       get_tariff_cost(t.id, 1, 9002, 9102) c_2,
                       get_tariff_cost(t.id, 1, 9002, 9103) c_3,
                       get_tariff_cost(t.id, 1, 9002, 9104) c_4,
                       t.*
                  from t_tariff2 t
                 where trunc(t.ver_date_beg) = trunc(sysdate)
                    or trunc(t.ver_date_end) = trunc(sysdate)) f)
 where (f_1 <> f_1_p or f_2 <> f_2_p or f_3 <> f_3_p or f_4 <> f_4_p or c_1 <> c_1_p or
       c_2 <> c_2_p or c_3 <> c_3_p or c_4 <> c_4_p)
   and at_id = at_id_prev
 order by at_id, ver_num;
   ';
        l_subject := l_subject || '. Есть изменения';
        -- 3. Проверим, изменился ли справочник услуг
        -- посчитаем хэш справочника из текущей выгрузки
        l_new_serv_md5 := Compute_MD5Hash_Of_Services;
        if (l_new_serv_md5 <> l_prev_serv_md5) then
          -- синхронизируем справочник услуг
          l_msg := l_msg || utl_tcp.CRLF || 'Изменились услуги';
          Sync_Services(pi_asr_id, l_cur_imp_id);
        end if;
      
        -- 4. Проверим, изменился ли справочник групп услуг
        -- посчитаем хэш справочника из текущей выгрузки
        l_new_gr_md5 := Compute_MD5Hash_Of_Serv_Groups;
        if (l_new_gr_md5 <> l_prev_gr_md5) then
          -- синхронизируем справочник групп услуг
          l_msg := l_msg || utl_tcp.CRLF || 'Изменились группы услуг';
          Sync_Serv_Groups(pi_asr_id, l_cur_imp_id);
        end if;
      
        -- 4.1. Проверим, изменился ли справочник пакетов
        -- посчитаем хэш справочника из текущей выгрузки
        l_new_pack_md5 := Compute_MD5Hash_Of_packages;
        if (l_new_pack_md5 <> l_prev_pack_md5) then
          -- синхронизируем справочник групп услуг
          l_msg := l_msg || utl_tcp.CRLF || 'Изменились пакеты';
          Sync_Package(pi_asr_id, l_cur_imp_id);
        end if;
      
        -- 4.2. Проверим, изменился ли справочник скидок
        -- посчитаем хэш справочника из текущей выгрузки
        l_new_discount_md5 := Compute_MD5Hash_Of_discounts;
        if (l_new_discount_md5 <> l_prev_discount_md5) then
          -- синхронизируем справочник групп услуг
          l_msg := l_msg || utl_tcp.CRLF || 'Изменились скидки';
          Sync_Discount(pi_asr_id, l_cur_imp_id);
        end if;
        -- 5. Синхронизируем тарифные планы
        if (pi_asr_id = 2) then
          Sync_Tariffs(pi_asr_id, l_cur_imp_id, pi_date);
        else
          Sync_Tariffs_PS(pi_asr_id, l_cur_imp_id, pi_date);
        end if;
        -- Сформируем сообщение о тарифах без региона
        l_msg_reg := 'Регион не заполнен' || utl_tcp.CRLF || '
                          select t.tar_id, t.title, t.startdate
                            from ' || g_tar_tbl_nm || ' t
                           where t.region_id is null
                        order by t.startdate desc';
        if l_msg_reg is not null then
          l_msg := l_msg || utl_tcp.CRLF || l_msg_reg;
          /*logging_pkg.error(l_msg_reg,
          'ASR_IMPORT_TAR.Tariff_Import_Sync');*/
        end if;
      end if;
      begin
        mail(recipients => l_recipients,
             message    => 'Идентификатор выгрузки: ' || l_cur_imp_id || '. ' ||
                           l_msg,
             subject    => l_subject);
      exception
        when others then
          null;
      end;
    end if;
  
    if (l_updated) then
      -- 6. Загружаем данные текущей выгрузки в промежуточные таблицы
      Store_Tar_Upload;
    
      -- 7. Обновляем хэши
      if (l_new_full_md5 is null) then
        l_new_full_md5 := Compute_MD5Hash_Of_Tariffs;
      end if;
    
      if (l_new_serv_md5 is null) then
        l_new_serv_md5 := Compute_MD5Hash_Of_Services;
      end if;
    
      if (l_new_gr_md5 is null) then
        l_new_gr_md5 := Compute_MD5Hash_Of_Serv_Groups;
      end if;
    
      if (l_new_pack_md5 is null) then
        l_new_pack_md5 := Compute_MD5Hash_Of_Packages;
      end if;
    
      if (l_new_discount_md5 is null) then
        l_new_discount_md5 := Compute_MD5Hash_Of_discounts;
      end if;
    else
      l_new_full_md5     := l_prev_full_md5;
      l_new_serv_md5     := l_prev_serv_md5;
      l_new_gr_md5       := l_prev_gr_md5;
      l_new_pack_md5     := l_prev_pack_md5;
      l_new_discount_md5 := l_prev_discount_md5;
    end if;
  
    -- сохраняем информацию о текущей выгрузке
    insert into ASR_TAR_IMP_MD5
      (TIM_ID,
       TIM_FULL_MD5,
       TIM_SERV_MD5,
       TIM_GR_MD5,
       TIM_ASR_ID,
       TIM_DATE,
       TIM_PACK_MD5,
       TIM_DISCOUNT_MD5)
    values
      (l_cur_imp_id,
       l_new_full_md5,
       l_new_serv_md5,
       l_new_gr_md5,
       pi_asr_id,
       pi_date,
       l_new_pack_md5,
       l_new_discount_md5);
  
    Add_tariff_by_at_id; --22.09.2010 задача23910
    logging_pkg.info('Идентификатор выгрузки: ' || to_char(l_cur_imp_id) || '. ' ||
                     l_msg,
                     'ASR_IMPORT_TAR.Tariff_Import_Sync');
  
    return l_updated;
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Tariff_Import_Sync');
      begin
        mail(recipients => l_recipients,
             message    => dbms_utility.format_error_backtrace || '
                         ' || sqlerrm,
             subject    => 'Сеанс загрузки тарифных планов от АСР ERR');
      exception
        when others then
          null;
      end;
      raise;
  end Tariff_Import_Sync;
  -------------------------------------------------------------------
  -- Откат последней версии импорта до предыдущей версии
  -- используется для очистки БД после импорта из
  -- ошибочной выгрузки от АСР
  procedure Rollback_Last_Ver(pi_asr_id in T_ASR.ASR_ID%type) is
    l_last_imp_id number;
  begin
    -- Получим идентификатор импорта последней версии
    select max(TIM_ID)
      into l_last_imp_id
      from ASR_TAR_IMP_MD5
     where TIM_ASR_ID = pi_asr_id;
  
    -- удаляем параметры услуг тарифных планов версий,
    -- созданных в результате импорта с идентификатором
    -- l_last_imp_id
    delete from T_SERV_TAR2 st
     where st.TAR_ID in
           (select ID from T_TARIFF2 where IMP_ID = l_last_imp_id);
  
    -- удаляем связи услуг тарифных планов версий,
    -- созданных в результате импорта с идентификатором
    -- l_last_imp_id
    delete from T_SERV_SERV2 ss
     where ss.TAR_ID in
           (select ID from T_TARIFF2 where IMP_ID = l_last_imp_id);
  
    -- удаляем группы услуг тарифных планов версий,
    -- созданных в результате импорта с идентификатором
    -- l_last_imp_id
    delete from T_SERVICE_GROUP2 sg where sg.imp_id = l_last_imp_id;
  
    -- удаляем услуги тарифных планов версий,
    -- созданных в результате импорта с идентификатором
    -- l_last_imp_id
    delete from T_SERVICE2 s where s.serv_imp_id = l_last_imp_id;
  
    -- удаляем тарифные планы версий,
    -- созданных в результате импорта с идентификатором
    -- l_last_imp_id
    delete from T_TARIFF2 where IMP_ID = l_last_imp_id;
  
    -- удаляем пакеты
    delete from t_eissd_package2 where IMP_ID = l_last_imp_id;
  
    -- удаляем скидки
    delete from t_discount2 where IMP_ID = l_last_imp_id;
  
    -- удаляем ограничения
    delete from t_restrict_ability t
     where t.srv_id in (select s.remote_serv_id
                          from T_SERVICE2 s
                         where s.serv_imp_id = l_last_imp_id)
        or t.pack_id in (select e.pack_id
                           from t_eissd_package2 e
                          where IMP_ID = l_last_imp_id)
        or t.disc_id in
           (select d.disc_id from t_discount2 d where IMP_ID = l_last_imp_id);
  
    -- удаляем версию
    delete from ASR_TAR_IMP_MD5
     where TIM_ID = l_last_imp_id
       and TIM_ASR_ID = pi_asr_id;
  
    -- открываем версии тарифных планов,
    -- которые были закрыты в результате последнего импорта
    update T_TARIFF2
       set VER_DATE_END = null, IMP_NEXT_ID = null
     where IMP_NEXT_ID = l_last_imp_id;
  end Rollback_Last_Ver;
  ----------------------------------------------------------------------------------------------------
  -- Импорт услуг для всех закрытых ТП
  ----------------------------------------------------------------------------------------------------
  procedure addServiceAllCloseTar(pi_asr_id in T_ASR.ASR_ID%type) is
    l_at_id      T_ABSTRACT_TAR.AT_ID%type;
    l_loc_tar_id T_TARIFF2.ID%type;
    sql_text     t_sql;
  begin
    -- Добавляем услуги, которых нет
    sql_text := 'INSERT INTO T_SERV_TAR2
                  (SERV_ID,
                   TAR_ID,
                   SERV_COST,
                   SERV_ADVANCE,
                   STARTDATE,
                   CATEGORY,
                   IS_ENABLE,
                   IS_ON,
                   PHONE_FEDERAL,
                   PHONE_COLOR,
                   PACK_ID,
                   DISC_ID,
                   TYPE_TARIFF,
                   TYPE)
                 select DISTINCT SS1.SERV_ID,
                                  TAT1.ID,
                                  SC.COST,
                                  SC.ADVANCE,
                                  SC.STARTDATE,
                                  NVL(SC.CATEGORY, 2),
                                  SC.IS_ENABLE,
                                  SC.IS_ON,
                                  case sc.PHONE_FEDERAL
                                    when :c_phone_federal_prot then
                                     :c_phone_federal_dic
                                    when :c_phone_city_prot then
                                     :c_phone_city_dic
                                    else
                                     :c_phone_federal_dic
                                  end PHONE_FEDERAL,
                                  nvl(cc.id,tmc_sim.get_color_default) PHONE_COLOR,
                                  SC.PACK_ID,
                                  SC.DISC_ID,
                                  SC.TYPE_TARIFF,
                                  SC.TYPE
                   from ' || g_srv_c_tbl_nm ||
                ' SC,
                        T_SERVICE2               SS1,
                        T_TARIF_BY_AT_ID         TAT1,
                        ' || g_tar_tbl_nm ||
                '                T,
                    t_dic_sim_color cc 
                  WHERE SC.SRV_ID = SS1.REMOTE_SERV_ID
                    AND TAT1.AT_REMOTE_ID = SC.TAR_ID
                    AND SC.TAR_ID = T.TAR_ID
                    AND SC.REGION_ID = T.REGION_ID
                    AND T.IS_ACTIVE = 0
                    and sc.PHONE_COLOR = cc.ps_id(+)
                    AND (SC.SRV_ID, SC.TAR_ID, SC.REGION_ID) NOT IN
                        (select distinct SS.REMOTE_SERV_ID, TAT.AT_REMOTE_ID, TAT.AT_REGION_ID
                           from T_SERV_TAR2 ST, T_TARIF_BY_AT_ID TAT, T_SERVICE2 SS
                          WHERE ST.TAR_ID = TAT.ID
                            AND ST.SERV_ID = SS.SERV_ID)';
    execute immediate sql_text
      using c_phone_federal_prot, c_phone_federal_dic, c_phone_city_prot, c_phone_city_dic, c_phone_federal_dic/*, c_phone_color_simple_prot, c_phone_color_simple_dic, c_phone_color_silver_prot, c_phone_color_silver_dic, c_phone_color_gold_prot, c_phone_color_gold_dic, c_phone_color_plat_prot, c_phone_color_plat_dic, c_phone_color_simple_dic*/;
    -- Добавляем пакеты, которых нет
    sql_text := 'INSERT INTO T_SERV_TAR2
                  (SERV_ID,
                   TAR_ID,
                   SERV_COST,
                   SERV_ADVANCE,
                   STARTDATE,
                   CATEGORY,
                   IS_ENABLE,
                   IS_ON,
                   PHONE_FEDERAL,
                   PHONE_COLOR,
                   PACK_ID,
                   DISC_ID,
                   TYPE_TARIFF,
                   TYPE)
                 select DISTINCT NULL,
                TAT1.ID,
                SC.COST,
                SC.ADVANCE,
                SC.STARTDATE,
                NVL(SC.CATEGORY, 2),
                SC.IS_ENABLE,
                SC.IS_ON,
                case sc.PHONE_FEDERAL
                  when :c_phone_federal_prot then
                   :c_phone_federal_dic
                  when :c_phone_city_prot then
                   :c_phone_city_dic
                  else
                   :c_phone_federal_dic
                end PHONE_FEDERAL,
                nvl(cc.id,tmc_sim.get_color_default) PHONE_COLOR,
                SC.PACK_ID,
                SC.DISC_ID,
                SC.TYPE_TARIFF,
                SC.TYPE
  from ' || g_srv_c_tbl_nm || ' SC,
       T_TARIF_BY_AT_ID TAT1,
       ' || g_tar_tbl_nm || ' T,
       t_dic_sim_color cc 
 WHERE TAT1.AT_REMOTE_ID = SC.TAR_ID
   AND SC.TAR_ID = T.TAR_ID
   AND SC.REGION_ID = T.REGION_ID
   AND T.IS_ACTIVE = 0
   AND NVL(SC.PACK_ID, 0) <> 0
   and sc.PHONE_COLOR = cc.ps_id(+)
   AND (SC.PACK_ID, SC.TAR_ID, SC.REGION_ID) NOT IN
       (select distinct ST.PACK_ID, TAT.AT_REMOTE_ID, TAT.AT_REGION_ID
          from T_SERV_TAR2 ST, T_TARIF_BY_AT_ID TAT
         WHERE ST.TAR_ID = TAT.ID
           AND NVL(ST.PACK_ID, 0) <> 0)';
    execute immediate sql_text
      using c_phone_federal_prot, c_phone_federal_dic, c_phone_city_prot, c_phone_city_dic, c_phone_federal_dic/*, c_phone_color_simple_prot, c_phone_color_simple_dic, c_phone_color_silver_prot, c_phone_color_silver_dic, c_phone_color_gold_prot, c_phone_color_gold_dic, c_phone_color_plat_prot, c_phone_color_plat_dic, c_phone_color_simple_dic*/;
    -- Добавляем скидки, которых нет
    sql_text := 'INSERT INTO T_SERV_TAR2
                  (SERV_ID,
                   TAR_ID,
                   SERV_COST,
                   SERV_ADVANCE,
                   STARTDATE,
                   CATEGORY,
                   IS_ENABLE,
                   IS_ON,
                   PHONE_FEDERAL,
                   PHONE_COLOR,
                   PACK_ID,
                   DISC_ID,
                   TYPE_TARIFF,
                   TYPE)
                 select DISTINCT NULL,
                TAT1.ID,
                SC.COST,
                SC.ADVANCE,
                SC.STARTDATE,
                NVL(SC.CATEGORY, 2),
                SC.IS_ENABLE,
                SC.IS_ON,
                case sc.PHONE_FEDERAL
                  when :c_phone_federal_prot then
                   :c_phone_federal_dic
                  when :c_phone_city_prot then
                   :c_phone_city_dic
                  else
                   :c_phone_federal_dic
                end PHONE_FEDERAL,
                nvl(cc.id,tmc_sim.get_color_default) PHONE_COLOR,
                SC.PACK_ID,
                SC.DISC_ID,
                SC.TYPE_TARIFF,
                SC.TYPE
  from ' || g_srv_c_tbl_nm || ' SC,
       T_TARIF_BY_AT_ID TAT1,
       ' || g_tar_tbl_nm || ' T,
       t_dic_sim_color cc 
 WHERE TAT1.AT_REMOTE_ID = SC.TAR_ID
   AND SC.TAR_ID = T.TAR_ID
   AND SC.REGION_ID = T.REGION_ID
   AND T.IS_ACTIVE = 0
   AND NVL(SC.DISC_ID, 0) <> 0
   and sc.PHONE_COLOR = cc.ps_id(+)
   AND (SC.DISC_ID, SC.TAR_ID, SC.REGION_ID) NOT IN
       (select DISTINCT ST.DISC_ID, TAT.AT_REMOTE_ID, TAT.AT_REGION_ID
          from T_SERV_TAR2 ST, T_TARIF_BY_AT_ID TAT
         WHERE ST.TAR_ID = TAT.ID
           AND NVL(ST.DISC_ID, 0) <> 0)';
    execute immediate sql_text
      using c_phone_federal_prot, c_phone_federal_dic, c_phone_city_prot, c_phone_city_dic, c_phone_federal_dic/*, c_phone_color_simple_prot, c_phone_color_simple_dic, c_phone_color_silver_prot, c_phone_color_silver_dic, c_phone_color_gold_prot, c_phone_color_gold_dic, c_phone_color_plat_prot, c_phone_color_plat_dic, c_phone_color_simple_dic*/;
    -- Добавляем связи между услугами в ТП
    sql_text := '
        MERGE INTO T_SERV_SERV2 SS
        USING (with SERV as
               (select s.remote_serv_id, max(s.serv_id) max_serv_id
                  from t_service2 s
                 where s.serv_asr_id = :asr_id
                 group by s.remote_serv_id)
              select distinct s1.max_serv_id SERV1_ID,
                              s2.max_serv_id SERV2_ID,
                              case rs.REL_TYPE
                              when :c_serv_rel_type_par_prot then
                               :c_serv_rel_type_par_dic
                              when :c_serv_rel_type_enem_prot then
                               :c_serv_rel_type_enem_dic
                              when :c_serv_rel_type_both_prot then
                               :c_serv_rel_type_both_dic
                              end REL_TYPE,
                              TAT.ID      TAR_ID,
                              null        PACK_ID_1,
                              null        PACK_ID_2
                from mv_rel_service_service rs,
                     serv s1,
                     serv s2,
                     T_TARIF_BY_AT_ID TAT,
                     ' || g_tar_tbl_nm || ' T
               where s1.REMOTE_SERV_ID = rs.SRV_ID_1
                 and s2.REMOTE_SERV_ID = rs.SRV_ID_2
                 AND RS.TAR_ID = TAT.AT_REMOTE_ID
                 AND RS.REGION_ID = TAT.AT_REGION_ID
                 AND TAT.AT_REMOTE_ID = T.TAR_ID
                 AND T.IS_ACTIVE = 0
              union all
              select distinct null SERV_ID,
                              null SERV_ID,
                              case rs.REL_TYPE
                              when :c_serv_rel_type_par_prot then
                                 :c_serv_rel_type_par_dic
                                when :c_serv_rel_type_enem_prot then
                                 :c_serv_rel_type_enem_dic
                                when :c_serv_rel_type_both_prot then
                                 :c_serv_rel_type_both_dic
                              end,
                              TAT.ID       TAR_ID,
                              rs.PACK_ID_1,
                              rs.PACK_ID_2
                from mv_rel_service_service rs,
                     T_TARIF_BY_AT_ID TAT,
                     ' || g_tar_tbl_nm || ' T
               where rs.pack_id_1 is not null
                 AND RS.TAR_ID = TAT.AT_REMOTE_ID
                 AND RS.REGION_ID = TAT.AT_REGION_ID
                 AND TAT.AT_REMOTE_ID = T.TAR_ID
                 AND T.IS_ACTIVE = 0) S
            ON (NVL(S.SERV1_ID, -2) = NVL(SS.SERV1_ID, -2) AND NVL(S.SERV2_ID, -2) = NVL(SS.SERV2_ID, -2)
               AND NVL(S.REL_TYPE, -2) = NVL(SS.REL_TYPE, -2) AND NVL(S.TAR_ID, -2) = NVL(SS.TAR_ID, -2)
               AND NVL(S.PACK_ID_1, -2) = NVL(SS.PACK_ID_1, -2) AND NVL(S.PACK_ID_2, -2) = NVL(SS.PACK_ID_2, -2))
           WHEN NOT MATCHED THEN
            INSERT
              (SS.SERV1_ID,
               SS.SERV2_ID,
               SS.REL_TYPE,
               SS.TAR_ID,
               SS.PACK_ID_1,
               SS.PACK_ID_2)
            VALUES
              (S.SERV1_ID,
               S.SERV2_ID,
               S.REL_TYPE,
               S.TAR_ID,
               S.PACK_ID_1,
               S.PACK_ID_2)';
    execute immediate sql_text
      using pi_asr_id, c_serv_rel_type_par_prot, c_serv_rel_type_par_dic, c_serv_rel_type_enem_prot, c_serv_rel_type_enem_dic, c_serv_rel_type_both_prot, c_serv_rel_type_both_dic, c_serv_rel_type_par_prot, c_serv_rel_type_par_dic, c_serv_rel_type_enem_prot, c_serv_rel_type_enem_dic, c_serv_rel_type_both_prot, c_serv_rel_type_both_dic;
  exception
    when others then
      logging_pkg.error(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'ASR_IMPORT_TAR.Add_Tariff');
      raise;
  end addServiceAllCloseTar;

end ASR_IMPORT_TAR;
  /
