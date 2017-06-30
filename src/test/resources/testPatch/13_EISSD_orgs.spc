CREATE OR REPLACE PACKAGE ORGS is
  -- Author  : LAU
  -- Created : 15.03.2007 13:44:25
  --- типы связей организаций
  -- связь "parent-child"
  c_rel_tp_parent constant integer := 1001;
  -- связь курирования головной организации (в системе эту связь имеет только УСИ)
  c_rel_tp_crtr_main constant integer := 1002;
  -- связь курирования сервис-провайдера (для продажи подключений)
  c_rel_tp_sp_sl constant integer := 1003;
  -- связь курирования дилера (для продажи подключений)
  c_rel_tp_dlr_sl constant integer := 1004;
  -- связь по доверенности
  c_rel_tp_warrant constant integer := 1006;
  -- связь по договору ГПХ
  c_rel_gph constant integer := 1007;
  -- связь по договору Телемаркетинг
  c_rel_tm constant integer := 1008;

  -- Класс договора - Агент
  c_dog_class_agent constant integer := 1;
  -- Класс договора - Комиссионер
  c_dog_class_broker constant integer := 2;
  -- Класс договора - Мерчант
  c_dog_class_merchant constant integer := 3;
  -- Класс договора - Прием заявок
  c_dog_class_req constant integer := 4;

  -- договорной пермишен
  c_prm_type_dogovor constant integer := 8500;

  -- Types
  subtype t_Err_Msg is varchar2(2000);
  subtype t_vc200 is varchar2(20000);

  type t_orgs_tbl is table of T_ORGANIZATIONS.ORG_ID%type;

  type rec_org_tree_node is record(
    org_id   T_ORGANIZATIONS.ORG_ID%type,
    org_pid  T_ORGANIZATIONS.ORG_ID%type,
    org_name T_ORGANIZATIONS.ORG_NAME%type);

  type org_tree_tab is table of rec_org_tree_node;

  type rec_org_info is record(
    org_id               T_ORGANIZATIONS.ORG_ID%type,
    org_name             T_ORGANIZATIONS.ORG_NAME%type,
    org_ogrn             T_ORGANIZATIONS.ORG_OGRN%type,
    org_type             T_ORGANIZATIONS.ORG_TYPE%type,
    org_region_id        T_ORGANIZATIONS.REGION_ID%type,
    org_settl_account    T_ORGANIZATIONS.ORG_SETTL_ACCOUNT%type,
    org_con_account      T_ORGANIZATIONS.ORG_CON_ACCOUNT%type,
    org_kpp              T_ORGANIZATIONS.ORG_KPP%type,
    org_bik              T_ORGANIZATIONS.ORG_BIK%type,
    org_okpo             T_ORGANIZATIONS.ORG_OKPO%type,
    org_okonx            T_ORGANIZATIONS.ORG_OKONX%type,
    org_adr1_id          T_ADDRESS.ADDR_ID%type,
    org_adr1_country     T_ADDRESS.ADDR_COUNTRY%type,
    org_adr1_index       T_ADDRESS.ADDR_INDEX%type,
    org_adr1_city        T_ADDRESS.ADDR_CITY%type,
    org_adr1_street      T_ADDRESS.ADDR_STREET%type,
    org_adr1_building    T_ADDRESS.ADDR_BUILDING%type,
    org_adr1_office      T_ADDRESS.ADDR_OFFICE%type,
    org_adr2_id          T_ADDRESS.ADDR_ID%type,
    org_adr2_country     T_ADDRESS.ADDR_COUNTRY%type,
    org_adr2_index       T_ADDRESS.ADDR_INDEX%type,
    org_adr2_city        T_ADDRESS.ADDR_CITY%type,
    org_adr2_street      T_ADDRESS.ADDR_STREET%type,
    org_adr2_building    T_ADDRESS.ADDR_BUILDING%type,
    org_adr2_office      T_ADDRESS.ADDR_OFFICE%type,
    org_resp_id          T_PERSON.PERSON_ID%type,
    org_resp_phone       T_PERSON.PERSON_PHONE%type,
    org_resp_email       T_PERSON.PERSON_EMAIL%type,
    org_resp_lastname    T_PERSON.PERSON_LASTNAME%type,
    org_resp_firstname   T_PERSON.PERSON_FIRSTNAME%type,
    org_resp_middlename  T_PERSON.PERSON_MIDDLENAME%type,
    org_resp_inn         T_PERSON.PERSON_INN%type,
    org_resp_birthday    T_PERSON.PERSON_BIRTHDAY%type,
    org_resp_sex         T_PERSON.PERSON_SEX%type,
    org_resp_doc_id      T_DOCUMENTS.DOC_ID%type,
    org_resp_doc_series  T_DOCUMENTS.DOC_SERIES%type,
    org_resp_doc_number  T_DOCUMENTS.DOC_NUMBER%type,
    org_resp_doc_date    T_DOCUMENTS.DOC_REGDATE%type,
    org_resp_doc_info    T_DOCUMENTS.DOC_EXTRAINFO%type,
    org_resp_doc_type    T_DOCUMENTS.DOC_TYPE%type,
    org_touch_id         T_PERSON.PERSON_ID%type,
    org_touch_phone      T_PERSON.PERSON_PHONE%type,
    org_touch_email      T_PERSON.PERSON_EMAIL%type,
    org_touch_lastname   T_PERSON.PERSON_LASTNAME%type,
    org_touch_firstname  T_PERSON.PERSON_FIRSTNAME%type,
    org_touch_middlename T_PERSON.PERSON_MIDDLENAME%type,
    org_touch_inn        T_PERSON.PERSON_INN%type,
    org_touch_birthday   T_PERSON.PERSON_BIRTHDAY%type,
    org_touch_sex        T_PERSON.PERSON_SEX%type,
    org_touch_doc_id     T_DOCUMENTS.DOC_ID%type,
    org_touch_doc_series T_DOCUMENTS.DOC_SERIES%type,
    org_touch_doc_number T_DOCUMENTS.DOC_NUMBER%type,
    org_touch_doc_date   T_DOCUMENTS.DOC_REGDATE%type,
    org_touch_doc_info   T_DOCUMENTS.DOC_EXTRAINFO%type,
    org_touch_doc_type   T_DOCUMENTS.DOC_TYPE%type,
    org_pid              T_ORGANIZATIONS.ORG_ID%type,
    org_comment          T_ORGANIZATIONS.ORG_COMMENT%type,
    org_email            t_Organizations.Email%type);

  type cur_ref_org_info is ref cursor return rec_org_info;

  type rec_usr_info is record(
    usr_id            T_USERS.USR_ID%type,
    usr_org_id        T_ORGANIZATIONS.ORG_ID%type,
    usr_login         T_USERS.USR_LOGIN%type,
    person_lastname   T_PERSON.PERSON_LASTNAME%type,
    person_firstname  T_PERSON.PERSON_FIRSTNAME%type,
    person_middlename T_PERSON.PERSON_MIDDLENAME%type,
    person_email      T_PERSON.PERSON_EMAIL%type,
    usr_status        T_USERS.USR_STATUS%type);
  ------------------------------------------------------------------------
  procedure Add_subOrgs(pi_org_id     in number,
                        pi_dog_id     in number,
                        pi_region_tab in string_tab,
                        pi_worker_id  in number,
                        po_err_num    out pls_integer,
                        po_err_msg    out varchar2);
  -----------------------------------------------------------------------------
  --Создание организации
  -----------------------------------------------------------------------------
  function Ins_org(pi_org_type            in T_ORGANIZATIONS.ORG_TYPE%type,
                   pi_org_name            in T_ORGANIZATIONS.ORG_NAME%type,
                   pi_org_ogrn            in T_ORGANIZATIONS.ORG_OGRN%type,
                   pi_parent_id           in T_ORGANIZATIONS.ORG_ID%type,
                   pi_org_address         in address_type,
                   pi_org_post_index      in T_ADDRESS.ADDR_INDEX%type,
                   pi_org_fact_address    in address_type,
                   pi_org_fact_post_index in T_ADDRESS.ADDR_INDEX%type,
                   pi_resp                in client_type,
                   pi_resp_inn            in T_PERSON.PERSON_INN%type,
                   pi_touch               in client_type,

                   pi_org_region_id in t_dic_region.REG_ID%type,
                   pi_comment       in T_ORGANIZATIONS.ORG_COMMENT%type,
                   pi_org_email     in T_ORGANIZATIONS.Email%type,
                   pi_worker_id     in T_USERS.USR_ID%type,

                   pi_v_lice             in T_ORGANIZATIONS.V_LICE%type := null,
                   pi_na_osnovanii       in T_ORGANIZATIONS.NA_OSNOVANII%type := null,
                   pi_org_full_name      in t_organizations.org_full_name%type := null,
                   pi_v_lice_podkl       in T_ORGANIZATIONS.V_LICE%type := null,
                   pi_na_osnovanii_podkl in T_ORGANIZATIONS.NA_OSNOVANII%type := null,
                   pi_is_pay_espp        in t_organizations.is_pay_espp%type,
                   pi_boss_name          in T_ORGANIZATIONS.Boss_Name%type,
                   pi_type_org           in t_organizations.type_org%type,

                   pi_is_stamp              in number := 1,
                   pi_is_with_rekv          in number,
                   pi_channel_tab           in array_num_3,
                   pi_is_with_ip            in number,
                   PI_IS_WITH_PERSONAL_INFO in number,
                   pi_start_date            in t_organizations.start_date%type,
                   pi_USE_CHILD_REQ         in t_organizations.USE_CHILD_REQ%type,
                   pi_org_buy               in t_organizations.org_buy%type,

                   PI_IS_SS_CENTER         IN T_ORGANIZATIONS.IS_SS_CENTER%type,
                   PI_SSC_PHONES           IN SSC_PHONES_tab, --Контактный телефон ЦПО для сайта
                   PI_SSC_TIMETABLE        IN ORG_TIMETABLE_TAB,
                   PI_SSC_EMAIL            IN T_ORG_SS_CENTER.email%type,
                   PI_SSC_LATITUDE         IN T_ORG_SS_CENTER.LATITUDE%type,
                   PI_SSC_LONGITUDE        IN T_ORG_SS_CENTER.LONGITUDE%type,
                   PI_SSC_CLOSE_DATE       IN T_ORG_SS_CENTER.CLOSE_DATE%type,
                   PI_SSC_SEGM_SERVICE     IN T_ORG_SS_CENTER.SEGMENT_SERVICE%type,
                   PI_SSC_URL              IN T_ORG_SS_CENTER.URL%type,
                   pi_ss_service           in SS_SERVICE_TAB,
                   pi_OWNERSHIP            in number,
                   pi_square_meter         in t_org_ss_center.square_meter%type,
                   pi_workers_number       in t_org_ss_center.workers_number%type, -- Количество оборудованных рабочих мест
                   PI_METRO                in t_org_ss_center.METRO%type,
                   PI_PRIORITY             in t_org_ss_center.PRIORITY%type,
                   PI_COMMENTS             in t_org_ss_center.COMMENTS%type,
                   PI_UNRESERVED_TMC       in array_num_2,
                   pi_ssc_CNT_TERM_RTC     in t_org_ss_center.CNT_TERM_RTC%type, --Количество терминалов РТК
                   pi_ssc_CNT_TERM_AGENT   in t_org_ss_center.Cnt_Term_Agent%type, --Количество терминалов агентов
                   pi_ssc_IS_ELECTRO_QUEUE in t_org_ss_center.is_electro_queue%type, --Признак наличия электронной очереди: 0 - нет, 1 - есть
                   pi_ssc_IS_GOLD_POOL     in t_org_ss_center.is_gold_pool%type, --Признак принадлежности к Золотому пулу: 0 - нет, 1 - есть

                   pi_income_account  in bank_account_type, --доходный счет
                   pi_expense_account in bank_account_type, --расходный счет

                   pi_erp_r12_num       in number, --Идентификатор организации в ERP R12
                   pi_ssc_cluster       in number, --кластер цпо
                   pi_ssc_fact_district in varchar2, --регион области 40
                   pi_ssc_employee      in ORG_employee_TAB, --информация о сотрудниках
                   pi_ssc_temp_close    in org_temp_close_tab, --периоды временного закрытия
                   pi_ssc_date_open     in date, --дата открытия
                   pi_ssc_contact_phone in string_tab, --Контактный телефон ЦПО
                   pi_ssc_CNT_CASHBOX   in number, --Количество касс (ЕКМ)
                   pi_FULL_NAME_SSC     in varchar2, --полное наименование ЦПО
                   po_err_num           out pls_integer,
                   po_err_msg           out t_Err_Msg)
    return T_ORGANIZATIONS.ORG_ID%type;
  ---------------------------------------------------------------------------------
  --Редактирование организации
  ---------------------------------------------------------------------------------
  function Change_Org(pi_org_id              in T_ORGANIZATIONS.ORG_ID%type,
                      pi_org_type            in T_ORGANIZATIONS.ORG_TYPE%type,
                      pi_org_name            in T_ORGANIZATIONS.ORG_NAME%type,
                      pi_org_ogrn            in T_ORGANIZATIONS.ORG_OGRN%type,
                      pi_parent_id           in T_ORGANIZATIONS.ORG_ID%type,
                      pi_org_address         in address_type,
                      pi_org_post_index      in T_ADDRESS.ADDR_INDEX%type,
                      pi_org_fact_address    in address_type,
                      pi_org_fact_post_index in T_ADDRESS.ADDR_INDEX%type,
                      pi_resp                in client_type,
                      pi_resp_inn            in T_PERSON.PERSON_INN%type,
                      pi_touch               in client_type,

                      pi_org_region_id in t_dic_region.REG_ID%type,
                      pi_comment       in T_ORGANIZATIONS.ORG_COMMENT%type,
                      pi_org_email     in T_ORGANIZATIONS.Email%type,
                      pi_worker_id     in T_USERS.USR_ID%type,

                      pi_v_lice             in T_ORGANIZATIONS.V_LICE%type := null,
                      pi_na_osnovanii       in T_ORGANIZATIONS.NA_OSNOVANII%type := null,
                      pi_org_full_name      in t_organizations.org_full_name%type := null,
                      pi_v_lice_podkl       in T_ORGANIZATIONS.V_LICE%type := null,
                      pi_na_osnovanii_podkl in T_ORGANIZATIONS.NA_OSNOVANII%type := null,
                      pi_is_pay_espp        in t_organizations.is_pay_espp%type,
                      pi_boss_name          in T_ORGANIZATIONS.BOSS_NAME%type,
                      pi_type_org           in t_organizations.type_org%type,

                      pi_is_stamp              in number := 1,
                      pi_is_with_rekv          in number,
                      pi_channel_tab           in array_num_3,
                      pi_is_with_ip            in number,
                      PI_IS_WITH_PERSONAL_INFO in number,
                      pi_start_date            in t_organizations.start_date%type,
                      pi_use_child_req         in t_organizations.use_child_req%type,
                      pi_org_buy               in t_organizations.org_buy%type,

                      PI_IS_SS_CENTER     IN T_ORGANIZATIONS.IS_SS_CENTER%type,
                      PI_SSC_PHONES       IN SSC_PHONES_tab,
                      PI_SSC_TIMETABLE    IN ORG_TIMETABLE_TAB,
                      PI_SSC_EMAIL        IN T_ORG_SS_CENTER.email%type,
                      PI_SSC_LATITUDE     IN T_ORG_SS_CENTER.LATITUDE%type,
                      PI_SSC_LONGITUDE    IN T_ORG_SS_CENTER.LONGITUDE%type,
                      PI_SSC_CLOSE_DATE   IN T_ORG_SS_CENTER.CLOSE_DATE%type,
                      PI_SSC_SEGM_SERVICE IN T_ORG_SS_CENTER.SEGMENT_SERVICE%type,
                      PI_SSC_URL          IN T_ORG_SS_CENTER.URL%type,
                      pi_ss_service       in SS_SERVICE_TAB,
                      pi_OWNERSHIP        in number,
                      pi_square_meter     in t_org_ss_center.square_meter%type,
                      pi_workers_number   in t_org_ss_center.workers_number%type,
                      PI_METRO            in t_org_ss_center.METRO%type,
                      PI_PRIORITY         in t_org_ss_center.PRIORITY%type,
                      PI_COMMENTS         in t_org_ss_center.COMMENTS%type,
                      PI_UNRESERVED_TMC   in array_num_2,
                      pi_ssc_CNT_TERM_RTC   in t_org_ss_center.CNT_TERM_RTC%type, --Количество терминалов РТК
                      pi_ssc_CNT_TERM_AGENT   in t_org_ss_center.Cnt_Term_Agent%type, --Количество терминалов агентов
                      pi_ssc_IS_ELECTRO_QUEUE   in t_org_ss_center.is_electro_queue%type, --Признак наличия электронной очереди: 0 - нет, 1 - есть
                      pi_ssc_IS_GOLD_POOL  in t_org_ss_center.is_gold_pool%type, --Признак принадлежности к Золотому пулу: 0 - нет, 1 - есть

                      pi_income_account  in bank_account_type, --доходный счет
                      pi_expense_account in bank_account_type, --расходный счет

                      pi_erp_r12_num       in number, --Идентификатор организации в ERP R12
                      pi_ssc_cluster       in number, --кластер цпо
                      pi_ssc_fact_district in varchar2, --регион области 40
                      pi_ssc_employee      in ORG_employee_TAB, --информация о сотрудниках
                      pi_ssc_temp_close    in org_temp_close_tab, --периоды временного закрытия
                      pi_ssc_date_open     in date, --дата открытия
                      pi_ssc_contact_phone in string_tab, --Контактный телефон ЦПО
                      pi_ssc_CNT_CASHBOX   in number, --Количество касс (ЕКМ)
                      pi_FULL_NAME_SSC     in varchar2, --полное наименование ЦПО

                      po_err_num out pls_integer,
                      po_err_msg out t_Err_Msg)
    return T_ORGANIZATIONS.ORG_ID%type;
  ------------------------------------------------------------------------
  function Add_Org_Relation(pi_org_id   in T_ORGANIZATIONS.ORG_ID%type,
                            pi_org_pid  in T_ORGANIZATIONS.ORG_ID%type,
                            pi_rel_type in T_ORG_RELATIONS.ORG_RELTYPE%type)
    return T_ORG_RELATIONS.ID%type;
  ------------------------------------------------------------------------
  function Get_Curator_Orgs(pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                            pi_is_up     in pls_integer,
                            pi_with_lic  in number, --получать помимо всего лиц. счет ли нет
                            pi_worker_id in T_USERS.USR_ID%type,
                            po_err_num   out pls_integer,
                            po_err_msg   out t_Err_Msg) return sys_refcursor;
  ------------------------------------------------------------------------
  function Get_Channels_Org(pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                            pi_worker_id in T_USERS.USR_ID%type,
                            po_err_num   out pls_integer,
                            po_err_msg   out varchar2) return sys_refcursor;
  ------------------------------------------------------------------------
  --Получение организации по ИД
  ------------------------------------------------------------------------
  function Get_Org_By_Id(pi_org_id            in T_ORGANIZATIONS.ORG_ID%type,
                         pi_is_check          in number, -- Нужна ли проверка прав
                         pi_worker_id         in T_USERS.USR_ID%type,
                         po_channels          out sys_refcursor,
                         po_ssc_phones        out sys_refcursor,
                         po_ssc_timetable     out sys_refcursor,
                         po_ss_service        out sys_refcursor,
                         po_ssc_employee      out sys_refcursor, --информация о сотрудниках
                         po_ssc_temp_close    out sys_refcursor, --периоды временного закрытия
                         po_ssc_contact_phone out sys_refcursor, --Контактный телефон ЦПО
                         po_tmc_unreserved    out sys_refcursor,
                         po_err_num           out pls_integer,
                         po_err_msg           out t_Err_Msg)
    return sys_refcursor;
  -----------------------------------------------------------------------
  procedure Fix_Org_Tree;
  ---------------------------------------------------------------------------
  --50703(46892)Получение расширенной инфы по регионам
  ---------------------------------------------------------------------------
  function Get_Regions_Info(pi_region_id in t_dic_region.reg_id%type,
                            pi_worker_id in T_USERS.USR_ID%type,
                            -- 51465
                            pi_doc_flag in number,
                            po_err_num  out pls_integer,
                            po_err_msg  out t_Err_Msg) return sys_refcursor;
  ---------------------------------------------------------------------------
  --получение инфы по регионам
  ---------------------------------------------------------------------------
  function Get_Reg_by_Org(pi_org_id    in number, --1 -выводить для РТ Мобайл
                          pi_worker_id in T_USERS.USR_ID%type) return num_tab;
  function Get_Reg_by_Org(pi_org_id    in number, --1 -выводить для РТ Мобайл
                          pi_org_pid   in number,
                          pi_worker_id in T_USERS.USR_ID%type) return num_tab;
  ---------------------------------------------------------------------------
  --получение инфы по регионам
  ---------------------------------------------------------------------------
  function Get_Regions_by_Org(pi_org_id    in number, --1 -выводить для РТ Мобайл
                              pi_worker_id in T_USERS.USR_ID%type,
                              po_err_num   out pls_integer,
                              po_err_msg   out varchar2) return sys_refcursor;
  ---------------------------------------------------------------------------
  -- Получение инфы по регионам
  ---------------------------------------------------------------------------
  function Get_Regions_by_Org(pi_org_id    in number, --1 -выводить для РТ Мобайл
                              pi_org_pid   in number,
                              pi_worker_id in T_USERS.USR_ID%type,
                              po_err_num   out pls_integer,
                              po_err_msg   out varchar2) return sys_refcursor;
  ---------------------------------------------------------------------------
  --получение инфы по регионам
  ------------------------------------------------------------------------
  function Get_Regions(pi_only_ural    in number,
                       pi_only_filial  in number, --если не 1 то достаются все филиалы внутри мрф
                       pi_is_org_rtmob in number, --1 -выводить для РТ Мобайл
                       pi_worker_id    in T_USERS.USR_ID%type,
                       po_err_num      out pls_integer,
                       po_err_msg      out t_Err_Msg) return sys_refcursor;
  ------------------------------------------------------------------------
  function Add_Dogovor(pi_dogovor_number   in T_DOGOVOR.DOG_NUMBER%type,
                       pi_dogovor_date     in T_DOGOVOR.DOG_DATE%type,
                       pi_org_id1          in T_ORGANIZATIONS.ORG_ID%type,
                       pi_org_id2          in T_ORGANIZATIONS.ORG_ID%type,
                       pi_is_auto_vis      in T_DOGOVOR.IS_VIS_AUTO%type,
                       pi_worker_id        in T_USERS.USR_ID%type,
                       pi_with_nds         in T_DOGOVOR.WITH_NDS%type := null,
                       pi_pr_schema        in T_DOGOVOR.PREMIA_SCHEMA%type := null,
                       pi_v_lice           in T_DOGOVOR.V_LICE%type := null,
                       pi_na_osnovanii     in T_DOGOVOR.NA_OSNOVANII%type := null,
                       pi_list_prm         in num_tab, -- список разрешений для договора (мб null)
                       pi_acc_schema       in number, -- схема ведения счетов агента
                       pi_overdraft        in number, -- размер ограниченного овердрафта (для неограниченного и для схем без овердравта = -1)
                       pi_org_list         in num_tab, -- список организаций, которым выдаётся доверенность
                       pi_codes            in varchar2,
                       pi_dogovor_class_id in number, -- класс договора
                       pi_is_accept        in number := 0, -- флаг доверительные запросы
                       pi_style_file1      in varchar2, -- урл1
                       pi_style_file2      in varchar2, -- урл2
                       -- 33537 olia_serg
                       pi_org_name_sprav   in t_dogovor.org_name_sprav%type,
                       pi_dog_number_sprav in t_dogovor.dog_number_sprav%type,
                       pi_org_legal_form   in t_dogovor.org_legal_form%type,
                       pi_type_report_form in num_date_tab,
                       pi_percent_stb      in t_dogovor.percent_stb%type,
                       pi_PRIZNAK_STB      in t_dogovor.PRIZNAK_STB%type,
                       pi_payment_type     in t_dogovor.payment_type%type,
                       pi_m2m_type         in t_dogovor.m2m_type%type,
                       pi_region_tab       in string_tab,
                       pi_temporary_indent in t_dogovor.temporary_indent%type,
                       pi_max_sim_count       in t_dogovor.max_sim_count%type,
                       pi_required_emp_num    t_dogovor.is_employee_number_required%TYPE,
                       po_err_num          out pls_integer,
                       po_err_msg          out t_Err_Msg)
    return T_DOGOVOR.DOG_ID%type;
  ------------------------------------------------------------------------

  ------------------------------------------------------------------------
  function Get_Org_Level(pi_org_id in pls_integer) return pls_integer;
  ------------------------------------------------------------------------
  function Get_Org_By_OGRN(pi_org_ogrn  in T_ORGANIZATIONS.ORG_OGRN%type,
                           pi_worker_id in T_USERS.USR_ID%type,
                           po_err_num   out pls_integer,
                           po_err_msg   out t_Err_Msg) return sys_refcursor;
  ------------------------------------------------------------------------
  -- Функция возвращающая дерево организаций видимое пользователю
  function Get_Orgs_Tree(pi_org_id    in t_organizations.org_id%type := 1, --организация, от котрой строится дерево
                         pi_org_type  in num_tab, --типы организаций, возвращаемых в результате
                         pi_block_org in pls_integer := 0, -- показывать ли заблокированные организации
                         pi_block_dog in pls_integer := 0, -- показывать ли организации с заблокированными договрами
                         pi_worker_id in number, -- пользователь
                         po_err_num   out pls_integer,
                         po_err_msg   out varchar2) return sys_refcursor;
  ------------------------------------------------------------------------
  -- Функция возвращающая список организаций в не зависимости от принадлежности к конкретному пользователю
  function Get_Orgs_By_Type(pi_org_id  in T_ORGANIZATIONS.ORG_ID%type, -- начиная от конкретной организации
                            pi_org2_id in T_ORGANIZATIONS.ORG_ID%type) /* не включая ветку начиная с                                                                                                                                                                                                                                                            конкретной организации организации */
   return num_tab;
  ------------------------------------------------------------------------
  -- Функция возвращающая массив организаций видимых пользователю
  function Get_Orgs(pi_worker_id         in T_USERS.USR_ID%type, -- пользователь
                    pi_org_id            in T_ORGANIZATIONS.ORG_ID%type := null, -- от конкретной организации
                    pi_parents_include   in pls_integer := 0, -- включать ли родителей найденных организаций
                    pi_self_include      in pls_integer := 1, -- включать pi_org_id
                    pi_childrens_include in pls_integer := 1, -- включать детей
                    pi_curated_include   in pls_integer := 1, -- включать курируемых
                    pi_curators_include  in pls_integer := 0, -- включать кураторов
                    pi_incl_only_act_dog in pls_integer := 1) -- включать только активные договоры
   return num_tab;
  ------------------------------------------------------------------------
  -- Функция возвращающая массив организаций видимых пользователю
  function Get_User_Orgs_Tab_With_Param(pi_worker_id         in T_USERS.USR_ID%type, -- пользователь
                                        pi_org_id            in T_ORGANIZATIONS.ORG_ID%type := null, -- организация с которой начинаем
                                        pi_self_include      in pls_integer := 1, -- включать pi_org_id
                                        pi_childrens_include in pls_integer := 1, -- включать детей
                                        pi_curated_include   in pls_integer := 1, -- включать курируемых
                                        pi_curators_include  in pls_integer := 0) -- включать кураторов
   return num_tab;
  ------------------------------------------------------------------------
  function concat_cc_names(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return t_vc200;
  ----------------------------------------------------------------------------
  function Get_Calc_Center_List(pi_org_region_id in T_ORGANIZATIONS.ORG_ID%type, -- организация по региону которой берем CC
                                pi_region_id     in t_dic_region.REG_ID%type, -- регион в котором берем CC
                                pi_kladr_code    in KLADR.CODE%type, -- код населенного пункта в КЛАДР для которого берем CC
                                pi_org_id        in T_ORGANIZATIONS.ORG_ID%type, -- организация (МРК) для которой берем CC
                                pi_worker_id     in T_USERS.USR_ID%type, -- ограничение по пользователю(>организации>регионы)
                                po_err_num       out pls_integer,
                                po_err_msg       out t_Err_Msg)
    return sys_refcursor;
  ----------------------------------------------------------------------------
  function Get_Calc_Center_List2(pi_org_rel_id in T_ORGANIZATIONS.ORG_ID%type, -- организация по связи которой через T_ORG_CALC_CENTER берем CC
                                 pi_region_id  in t_dic_region.REG_ID%type, -- регион в котором берем CC
                                 pi_tariff_id  in T_TARIFF2.ID%type, -- тариф, по типу и региону которого ограничим РЦ
                                 pi_kladr_code in KLADR.CODE%type, -- код населенного пункта в КЛАДР для которого берем CC
                                 pi_org_id     in T_ORGANIZATIONS.ORG_ID%type, -- организация (МРК) для которой берем CC
                                 pi_org_id2    in T_ORGANIZATIONS.ORG_ID%type, -- организация для которой надо взять родителей и кураторов и взять их CC через T_ORG_CALC_CENTER
                                 pi_worker_id  in T_USERS.USR_ID%type, -- ограничение по пользователю(>организации>регионы)
                                 po_err_num    out pls_integer,
                                 po_err_msg    out t_Err_Msg)
    return sys_refcursor;
  ----------------------------------------------------------------------------
  function Get_Calc_Center_List3(pi_org_id       in array_num_2,
                                 pi_block        in number,
                                 pi_org_relation in num_tab,
                                 pi_region_id    in t_dic_region.REG_ID%type, -- регион в котором берем CC
                                 pi_tariff_id    in T_TARIFF2.ID%type, -- тариф, по типу и региону которого ограничим РЦ
                                 pi_kladr_code   in KLADR.CODE%type, -- код населенного пункта в КЛАДР для которого берем CC
                                 pi_worker_id    in T_USERS.USR_ID%type, -- ограничение по пользователю(>организации>регионы)
                                 po_err_num      out pls_integer,
                                 po_err_msg      out t_Err_Msg)
    return sys_refcursor;
  ------------------------------------------------------------------------
  function Is_Have_USI_Job(pi_worker in T_USERS.USR_ID%type)
    return pls_integer;
  ------------------------------------------------------------------------
  function Is_Have_SP_Job(pi_worker in T_USERS.USR_ID%type)
    return pls_integer;
  ------------------------------------------------------------------------
  function Is_Have_Parent_Job(pi_worker in T_USERS.USR_ID%type,
                              pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return pls_integer;
  ------------------------------------------------------------------------
  function Is_Have_Rel_Childrens(pi_worker   in T_USERS.USR_ID%type,
                                 pi_rel_type in T_ORG_RELATIONS.ORG_RELTYPE%type)
    return pls_integer;
  -----------------------------------------------------------------------
  function Get_Jobs_Number(pi_worker in T_USERS.USR_ID%type)
    return pls_integer;
  ------------------------------------------------------------------------
  function Get_Job_Org_Id_If_One(pi_worker in T_USERS.USR_ID%type)
    return T_ORGANIZATIONS.ORG_ID%type;
  ------------------------------------------------------------------------
  function Get_Job_Org_Type_If_One(pi_worker in T_USERS.USR_ID%type)
    return T_RELATION_TYPE.REL_TP_ID%type;
  ------------------------------------------------------------------------
  function Get_Root_Org_Or_Self(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return number;
  ------------------------------------------------------------------------
  function Get_Parent_Org(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return T_ORGANIZATIONS.ORG_ID%type;
  ------------------------------------------------------------------------
  function Get_Parent_Root_Org(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return T_ORGANIZATIONS.ORG_ID%type;
  ------------------------------------------------------------------------
  function Get_Org_Name(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return sys_refcursor;
  ------------------------------------------------------------------------
  function Get_Org_Name1(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return T_ORGANIZATIONS.ORG_NAME%type;
  ----------------------------------------------------------------------------------------------------
  -- возвращает полное наименование юр. лица
  function Get_Org_Full_Name(pi_org_id in number) return varchar2;
  ------------------------------------------------------------------------
  function Get_Dogovor_By_Org(pi_org_id      in T_DOGOVOR.DOG_ID%type,
                              pi_worker_id   in T_USERS.USR_ID%type,
                              po_report_form out sys_refcursor,
                              po_err_num     out pls_integer,
                              po_err_msg     out t_Err_Msg)
    return sys_refcursor;
  ------------------------------------------------------------------------
  function Get_Dogovor(pi_dogovor_id  in T_DOGOVOR.DOG_ID%type,
                       pi_worker_id   in T_USERS.USR_ID%type,
                       po_prm_cur     out sys_refcursor, -- курсор разрешений
                       po_report_form out sys_refcursor,
                       po_region_tab  out sys_refcursor,
                       po_err_num     out pls_integer,
                       po_err_msg     out t_Err_Msg) return sys_refcursor;
  ------------------------------------------------------------------------

  ------------------------------------------------------------------------
  procedure Change_Dogovor(pi_dogovor_id   in T_DOGOVOR.DOG_ID%type,
                           pi_dog_number   in T_DOGOVOR.DOG_NUMBER%type,
                           pi_date         in T_DOGOVOR.DOG_DATE%type,
                           pi_autovis      in T_DOGOVOR.IS_VIS_AUTO%type,
                           pi_worker_id    in T_USERS.USR_ID%type,
                           po_err_num      out pls_integer,
                           po_err_msg      out t_Err_Msg,
                           pi_with_nds     in T_DOGOVOR.WITH_NDS%type := null,
                           pi_pr_schema    in T_DOGOVOR.PREMIA_SCHEMA%type := null,
                           pi_v_lice       in T_DOGOVOR.V_LICE%type := null,
                           pi_na_osnovanii in T_DOGOVOR.NA_OSNOVANII%type := null,
                           pi_conn_type    in T_DOGOVOR.CONN_TYPE%type := null,
                           pi_list_prm     in num_tab, -- список разрешений для обновления договора (мб null)
                           --23.06.09 (задача №8519)
                           pi_acc_schema  in number, -- схема ведения счетов агента
                           pi_overdraft   in number, -- размер ограниченного овердрафта (для неограниченного и для схем без овердравта = -1)
                           pi_org_list    in num_tab, -- список организаций, которым выдаётся доверенность
                           pi_codes       in varchar2,
                           pi_is_pay_espp in number,
                           pi_is_accept   in T_DOGOVOR.IS_ACCEPT%TYPE,
                           pi_style_file1 in varchar2,
                           pi_style_file2 in varchar2,
                           pi_block       in number,
                           -- 33537 olia_serg
                           pi_org_name_sprav   in t_dogovor.org_name_sprav%type,
                           pi_dog_number_sprav in t_dogovor.dog_number_sprav%type,
                           pi_org_legal_form   in t_dogovor.org_legal_form%type,
                           pi_type_report_form in num_date_tab,
                           pi_percent_stb      in t_dogovor.percent_stb%type,
                           pi_PRIZNAK_STB      in t_dogovor.PRIZNAK_STB%type,
                           pi_payment_type     in t_dogovor.payment_type%type,
                           pi_m2m_type         in t_dogovor.m2m_type%type,
                           pi_region_tab       in string_tab,
                           pi_temporary_indent in t_dogovor.temporary_indent%type,
                           pi_max_sim_count    in t_dogovor.max_sim_count%type,
                           pi_required_emp_num    t_dogovor.is_employee_number_required%TYPE);

  ------------------------------------------------------------------------
  procedure Block_Dogovor(pi_dogovor_id in T_DOGOVOR.DOG_ID%type,
                          pi_block      in pls_integer,
                          pi_worker_id  in T_USERS.USR_ID%type,
                          po_err_num    out pls_integer,
                          po_err_msg    out t_Err_Msg);
  -----------------------------------------------------------------------
  -- Функция возвращающая массив организаций видимых пользователю
  function Get_User_Orgs_Tab_With_Param1(pi_worker_id           in T_USERS.USR_ID%type, -- пользователь
                                         pi_org_id              in T_ORGANIZATIONS.ORG_ID%type := null, -- организация с которой начинаем
                                         pi_self_include        in pls_integer := 1, -- включать pi_org_id
                                         pi_childrens_include   in pls_integer := 1, -- включать детей
                                         pi_curated_include     in pls_integer := 1, -- включать курируемых
                                         pi_curated_sub_include in pls_integer := 1, -- включать курируемых субдиллерами
                                         pi_curators_include    in pls_integer := 0, -- включать кураторов
                                         pi_tm_1009_include     in pls_integer := 1) -- включать 1009 связь телемаркетинга
   return num_tab;
  ------------------------------------------------------------------------
  -- Функция возвращающая массив организаций видимых пользователю
  function Get_Orgs1(pi_worker_id           in T_USERS.USR_ID%type, -- пользователь
                     pi_org_id              in T_ORGANIZATIONS.ORG_ID%type := null, -- от конкретной организации
                     pi_parents_include     in pls_integer := 0, -- включать ли родителей найденных организаций
                     pi_self_include        in pls_integer := 1, -- включать pi_org_id
                     pi_childrens_include   in pls_integer := 1, -- включать детей
                     pi_curated_include     in pls_integer := 1, -- включать курируемых
                     pi_curated_sub_include in pls_integer := 1, -- включать курируемых субдиллерами
                     pi_curators_include    in pls_integer := 0, -- включать кураторов
                     pi_incl_only_act_dog   in pls_integer := 1) -- включать только активные договоры
   return num_tab;
  ------------------------------------------------------------------------
  -- Функция возвращающая № договра для данного дилера и курирующей организации
  function Get_Dog_By_Org(pi_org_id           in number,
                          pi_org_pid          in number := null,
                          pi_dogovor_class_id in num_tab,
                          pi_worker_id        in number,
                          po_err_num          out pls_integer,
                          po_err_msg          out varchar2)
    return sys_refcursor;

  ----------------------------------------------------------------------------
  -- Договора при операциях с ТМЦ, PID всегда есть
  -- Новая, дабы не ломать Get_Dog_By_Org, которая много где еще используется
  -- В этой теперь достается договор с конкретным родителем
  ----------------------------------------------------------------------------
  function Get_Dog_By_Org_for_tmc_op(pi_org_id           in number,
                                     pi_org_pid          in number := null,
                                     pi_dogovor_class_id in num_tab,
                                     pi_worker_id        in number,
                                     po_err_num          out pls_integer,
                                     po_err_msg          out varchar2)
    return sys_refcursor;
  ----------------------------------------------------------------------------
  -- 59657 Добавлен параметр pi_ignore_blocked_org
  ----------------------------------------------------------------------------
  function Get_List_User_Podcluch(pi_org_id    t_organizations.org_id%type,
                                  pi_right_id  in t_rights.right_string_id%type,
                                  pi_worker_id in T_USERS.USR_ID%type,
                                  po_err_num   out pls_integer,
                                  po_err_msg   out t_Err_Msg)
    return sys_refcursor;

  ----------------------------------------------------------------------------
  function Is_Dog_Permitted(pi_org_pid   in T_ORGANIZATIONS.ORG_ID%type,
                            pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                            pi_worker_id in T_USERS.USR_ID%type,
                            pi_list_prm  in num_tab, -- список разрешений для договора (мб null)
                            po_err_num   out pls_integer,
                            po_err_msg   out varchar2) return number;
  ----------------------------------------------------------------------------
  function Get_Orgs_By_DogId(pi_dog_id    in t_dogovor.dog_id%type,
                             pi_worker_id in number) return sys_refcursor;

  function Check_Mask_Orgs_Dog(pi_org_id   in number,
                               pi_list_prm in num_tab, -- список разрешений для договора (мб null)
                               po_err_num  out pls_integer,
                               po_err_msg  out t_Err_Msg) return number;
  ------------------------------------------------------------------------
  -- Выдает список направлений использования ТМЦ по указанному договору
  function Get_Tmc_Move_Serv_By_Dog(pi_dog_id    in t_dogovor.dog_id%type,
                                    pi_worker_id in t_users.usr_id%type,
                                    po_err_num   out pls_integer,
                                    po_err_msg   out t_Err_Msg)
    return sys_refcursor;
  ------------------------------------------------------------------------
  -- Определяет, является ли организация оператором (УСИ)
  function Is_Org_Operator(pi_org_id    in t_organizations.org_id%type,
                           pi_worker_id in t_users.usr_id%type,
                           po_err_num   out pls_integer,
                           po_err_msg   out t_Err_Msg) return number;
  procedure Get_Orgs_By_Dog(pi_dog_id  in OUT t_dogovor.dog_id%type,
                            po_org_id  in out number,
                            po_org_pid in out number);
  ----------------------------------------------------------------------
  -- возвращает ИД договора для конкретной организации
  ----------------------------------------------------------------------
  procedure Get_DogId_By_Org(pi_org_id    in T_DOGOVOR.DOG_ID%type,
                             pi_worker_id in T_USERS.USR_ID%type,
                             po_dog_id    out t_dogovor.dog_id%type);
  ----------------------------------------------------------------------
  -- проверяет, есть ли у организации право на заключение договоров
  -- 0 => нет, 1 => да
  function Is_Make_Dog_Perm(pi_org_id in number) return number;
  ----------------------------------------------------------------------
  -- выдаёт список прав организации по bl организации
  function Get_Rights_By_org(pi_org_id in number) return sys_refcursor;
  ----------------------------------------------------------------------
  -- Выдаёт список коммутаторов, доступных для заданной организации
  function Get_List_Comm(pi_org_id  in number,
                         po_err_num out pls_integer,
                         po_err_msg out t_Err_Msg) return sys_refcursor;
  --------------------------------------------------------------------------
  function Get_Orgs_Tree_By_Level(pi_RootOrgs       in Num_Tab := Num_Tab(), --организации, от котрой строится дерево
                                  pi_org_type       in num_tab := Num_Tab(), --типы организаций, возвращаемых в результате
                                  pi_block_org      in pls_integer := 0, -- показывать ли заблокированные организации
                                  pi_block_dog      in pls_integer := 0, -- показывать ли организации с заблокированными договрами
                                  pi_right_id       in number, --   Право, по которому будут оганичиваться организации
                                  pi_include_Parent in number, -- Признак, включать-ли родителей оргций переданых в pi_RootOrgs
                                  pi_dop_org_type   in num_tab := Num_Tab(), -- Типы организаций, выбранные в фильтрах.
                                  -- 70779
                                  pi_branch    in number, -- Какие ветки возвращать: 0 - РТК, 1 - РТ-Мобайл, 2 - обе
                                  pi_show_rtk  in number, -- 1 - показывать детей, принадлежещих РТК
                                  pi_worker_id in number, -- пользователь
                                  po_err_num   out pls_integer,
                                  po_err_msg   out varchar2)

   return sys_refcursor;
  -----------------------------------------------------------------------------
  -- Возвращает ID ФЭСа по ID региона.
  -----------------------------------------------------------------------------
  Function Get_FES_Bi_Id_Region(Pi_Region_Id in Number,
                                pi_is_rtm    number,
                                po_err_num   out pls_integer,
                                po_err_msg   out t_Err_Msg) return Number;
  -----------------------------------------------------------------------------
  -- ** ЗАДЧА №11710 **
  -- Добавление/редактирование доверенности по договору
  procedure Add_Warrant(pi_dog_id   in number,
                        pi_org_id   in number,
                        pi_org_list in num_tab, -- список организаций, которым выдаётся доверенность
                        po_err_num  out pls_integer,
                        po_err_msg  out t_Err_Msg);
  -----------------------------------------------------------------------------
  -- ** ЗАДЧА №11710 **
  -- возвращает список организаций, у которых есть доверенность по указанному договору
  function Get_Warrant_By_DogId(pi_dog_id  in number,
                                po_err_num out pls_integer,
                                po_err_msg out t_Err_Msg)
    return sys_refcursor;
  -------------------------------------------------
  -- по БИК банка возвращает список его отделений
  function Get_Bank_Department(pi_bik in t_bank_department.bik_bank%type)
    return sys_refcursor;
  ------------------------------------------------
  -- добавляет отделение, если его не существовало (pi_id_dep = null),
  -- обновляет название, если оно было изменено, либо ничего не делает=)
  procedure Add_Bank_Department(pi_bik      in t_bank_department.bik_bank%type,
                                pi_id_dep   in t_bank_department.id%type,
                                pi_name_dep in t_bank_department.name%type);
  ------------------------------------------------------------------------
  --возвращает 2 организации по id договора
  ------------------------------------------------------------------------
  function Get_Orgs_By_DogId(pi_dog_id in t_dogovor.dog_id%type)
    return sys_refcursor;
  --------------------------------------------------------------
  -- Проверка наличия договора с сайтом-партнером
  --------------------------------------------------------------
  function get_partner_site_dog(pi_worker_id in number,
                                pi_dog_id    in number,
                                pi_check_sum in varchar2,
                                po_err_num   out number,
                                po_err_msg   out varchar2)
    return sys_refcursor;
  --------------------------------------------------------------
  -- Получение списка договоров (и огранизаций) по классу договора
  --------------------------------------------------------------
  function get_dogovors_by_class(pi_dog_class in t_dic_dogovor_class.id%type,
                                 po_err_num   out number,
                                 po_err_msg   out varchar2)
    return sys_refcursor;
  -----------------------------------------------------------------------------------------
  function Get_Dic_Org_Type(po_err_num out number, po_err_msg out varchar2)
    return sys_refcursor;
  -----------------------------------------------------------------------------------------
  -- 51465 Получение МРФ по организации
  -----------------------------------------------------------------------------------------
  function get_mrf_by_org_id(pi_org_id  in t_organizations.org_id%type,
                             po_err_num out number,
                             po_err_msg out varchar2) return sys_refcursor;
  -----------------------------------------------------------------------------------------
  --51465 получение по региону реквизитов
  -----------------------------------------------------------------------------------------
  function Get_region_details(pi_region_id in number,
                              po_err_num   out number,
                              po_err_msg   out varchar2) return sys_refcursor;
  ---------------------------------------------------------------------------------------
  -- Загрузка печати и подписи для региона с веба
  ---------------------------------------------------------------------------------------
  procedure Add_Document_By_Region(pi_region_id    in t_region_documents.reg_id%type,
                                   pi_document     in t_region_documents.document%type,
                                   PI_RTM_DOCUMENT in number,
                                   pi_worker_id    in t_users.usr_id%type,
                                   po_err_num      out number,
                                   po_err_msg      out varchar2);
  -----------------------------------------------------------------------------------------
  -- Получение по региону МРФа
  -----------------------------------------------------------------------------------------
  function Get_mrf_by_region(pi_region_id in number,
                             po_err_num   out number,
                             po_err_msg   out varchar2) return sys_refcursor;
  ------------------------------------------------------------------------------------
  -- 50802
  ------------------------------------------------------------------------------------
  function Get_User_Orgs_Tab_By_Right_str(pi_worker_id           in T_USERS.USR_ID%type, -- пользователь
                                          pi_str_right_id        in T_RIGHTS.RIGHT_STRING_ID%type,
                                          pi_org_id              in T_ORGANIZATIONS.ORG_ID%type := null, -- организация с которой начинаем
                                          pi_self_include        in pls_integer := 1, -- включать pi_org_id
                                          pi_childrens_include   in pls_integer := 1, -- включать детей
                                          pi_curated_include     in pls_integer := 1, -- включать курируемых
                                          pi_curated_sub_include in pls_integer := 1, -- включать курируемых субдиллерами
                                          pi_curators_include    in pls_integer := 0) -- включать кураторов

   return num_tab;
  ------------------------------------------------------------------------------------
  -- 55378 Определяем, является ли организация филиалом
  ------------------------------------------------------------------------------------
  function is_org_filial(pi_org_id in t_organizations.org_id%type)
    return number;
  ------------------------------------------------------------------------------------
  -- 57954 Определяем, является ли организация стартапом
  ------------------------------------------------------------------------------------
  function is_org_startup(pi_org_id in t_organizations.org_id%type)
    return number;
  ------------------------------------------------------------------------------------
  -- 57857 Сохранение прав для абонки на организации
  ------------------------------------------------------------------------------------
  procedure save_ab_service_for_org(pi_org_id     in t_organizations.org_id%type,
                                    pi_ab_service in num_tab := num_tab(),
                                    pi_worker_id  in number,
                                    po_err_num    out pls_integer,
                                    po_err_msg    out t_Err_Msg);
  ------------------------------------------------------------------------------------
  -- 57857 Получение прав для абонки на организации
  ------------------------------------------------------------------------------------
  function get_ab_service_for_org(pi_org_id    in t_organizations.org_id%type,
                                  pi_worker_id in number,
                                  po_err_num   out pls_integer,
                                  po_err_msg   out t_Err_Msg)
    return sys_refcursor;
  ---------------------------------------------------------------
  -- 57857 Сохранение списка доверенных IP-адресов
  ---------------------------------------------------------------
  procedure save_trusted_ip(pi_ip_addresses in IP_ADDRESSE_TAB,
                            pi_org_id       in t_organizations.org_id%type,
                            pi_worker_id    in t_users.usr_id%type,
                            po_err_code     out number,
                            po_err_msg      out varchar2);
  ---------------------------------------------------------------
  -- 57857 Получение списка доверенных IP-адресов
  ---------------------------------------------------------------
  function get_trusted_ip(pi_org_id    in t_organizations.org_id%type,
                          pi_worker_id in t_users.usr_id%type,
                          po_err_code  out number,
                          po_err_msg   out varchar2) return sys_refcursor;
  ------------------------------------------------------------------------------------
  -- 55378 Определяем, является ли организация филиалом
  ------------------------------------------------------------------------------------
  function is_org_mrf_or_filial(pi_org_id   in t_organizations.org_id%type,
                                po_err_code out number,
                                po_err_msg  out varchar2) return number;
  ------------------------------------------------------------------------------------
  -- 61554 Возвращаем название головной организации
  ------------------------------------------------------------------------------------
  function Get_Head_Org_Name(pi_org_id in t_organizations.org_id%type)
    return sys_refcursor;
  ------------------------------------------------------------------------------------
  -- 61554 Возвращаем название головной организации
  ------------------------------------------------------------------------------------
  function Get_Head_Org_Name2(pi_org_id in t_organizations.org_id%type)
    return varchar2;
  ------------------------------------------------------------------------------------
  -- получение наименования организации с учетом организации
  ------------------------------------------------------------------------------------
  function get_org_name_tree(pi_org_id in t_organizations.org_id%type)
    return varchar2;
  -----------------------------------------------------------------------------------------
  -- Получение по региону МРФа
  -----------------------------------------------------------------------------------------
  function Get_mrf_by_region_kladr(pi_region_id in varchar2,
                                   po_err_num   out number,
                                   po_err_msg   out varchar2)
    return sys_refcursor;
  ----------------------------------------------------
  --Получение доп параметров по организации Телемаркетинга
  ----------------------------------------------------
  function get_org_dop_param_TM(pi_org_pid    in number,
                                pi_org_id     in number,
                                pi_date_start in date,
                                pi_date_end   in date,
                                pi_worker_id  in number,
                                po_err_num    out number,
                                po_err_msg    out varchar2)
    return sys_refcursor;
  -------------------------------------------------------------------------
  --редактирование доп парметров организации
  --------------------------------------------------------------------------
  procedure save_org_dop_param_TM(pi_param     in org_param_tm_tab,
                                  pi_worker_id in number,
                                  po_err_num   out number,
                                  po_err_msg   out varchar2);
  --------------------------------------------------------------------------
  function Get_Filials_by_worker(pi_worker_id in number,
                                 po_err_num   out pls_integer,
                                 po_err_msg   out t_Err_Msg)
    return sys_refcursor;
  --------------------------------------------------------------------------------------
  -- Список организацйи ограниченный структурой. типом связи и договорными пермишенами
  --------------------------------------------------------------------------------------
  function get_orgs_by_structure_and_prm(pi_worker_id      in number,
                                         pi_org_id         in number,
                                         pi_is_enabled_org in number,
                                         pi_is_enabled_dog in number,
                                         pi_is_cur_rtk     in number,
                                         pi_with_curated   in number,
                                         pi_with_self      in number,
                                         pi_rel_tab        in num_tab,
                                         pi_prm_tab        in num_tab,
                                         po_err_num        out number,
                                         po_err_msg        out varchar2)
    return num_tab;
  --------------------------------------------------------
  -- Проверка - есть ли у организации в выбранной структуре пермишен
  --------------------------------------------------------
  function is_org_have_prm(pi_org_id         in number,
                           pi_prm_tab        in num_tab,
                           pi_is_org_enabled in number,
                           pi_is_dog_enabled in number,
                           pi_structure      in number,
                           po_err_num        out number,
                           po_err_msg        out varchar2) return integer;
  -----------------------------------------------------------------
  -- Договоры между двумя организациями
  -----------------------------------------------------------------
  function get_dogovors_between_orgs(pi_org_id           in number,
                                     pi_org_pid          in number := null,
                                     pi_dogovor_class_id in num_tab,
                                     pi_worker_id        in number,
                                     po_err_num          out pls_integer,
                                     po_err_msg          out varchar2)
    return sys_refcursor;
  ----------------------------------------------------------------
  -- список организаций пользователя, ограниченный по пермишену
  ----------------------------------------------------------------
  function get_user_orgs_by_prm(pi_worker_id in number,
                                pi_rel_tab   in num_tab,
                                pi_prm_tab   in num_tab,
                                po_err_num   out number,
                                po_err_msg   out varchar2) return num_tab;
  -- Плюс учет заблокированности для отчетов типа остатков и движения
  function get_user_orgs_by_prm(pi_worker_id in number,
                                pi_rel_tab   in num_tab,
                                pi_prm_tab   in num_tab,
                                pi_block     in number,
                                po_err_num   out number,
                                po_err_msg   out varchar2) return num_tab;
  --получение наименований организации по списку
  function get_org_name_tab(pi_org_tab in num_tab) return sys_refcursor;
  ------------------------------------------------------------------------------------
  -- получение пути орг-ции до вершины
  ------------------------------------------------------------------------------------
  function get_path_org_to_top(pi_org_id    in t_organizations.org_id%type,
                               pi_worker_id in number) return sys_refcursor;
  ------------------------------------------------------------------------------------
  -- Получение организаций пользователя для построение дерева.
  ------------------------------------------------------------------------------------
  function get_path_org_to_top2(pi_worker_id in number) return sys_refcursor;
  ------------------------------------------------------------------------
  function Get_Root_Orgs_by_User(pi_worker_id in number,
                                 po_err_num   out number,
                                 po_err_msg   out varchar2)
    return sys_refcursor;
  ------------------------------------------------------------------------
  --получение дочерних орг-ций с определен. структурой
  ------------------------------------------------------------------------
  function Get_Child_Org_by_Type(pi_org_pid   in number,
                                 pi_org_type  in num_tab := Num_Tab(), --типы организаций, возвращаемых в результате
                                 pi_block_org in pls_integer := 0, -- показывать ли заблокированные организации
                                 pi_block_dog in pls_integer := 0, -- показывать ли организации с заблокированными договрами
                                 pi_branch    in number, -- Какие орг-ции возвращать: 0 - РТК, 1 - РТ-Мобайл, 2 - обе
                                 pi_worker_id in number,
                                 po_err_num   out number,
                                 po_err_msg   out varchar2)
    return sys_refcursor;
  ------------------------------------------------------------------------
  --получение списка договорных прав по орг-ции
  ------------------------------------------------------------------------
  function Get_dog_perm_by_ORG(pi_org_id    in number,
                               pi_worker_id in number,
                               po_err_num   out number,
                               po_err_msg   out varchar2)
    return sys_refcursor;
  ------------------------------------------------------------------------
  -- Получение ИД региона ЕИССД по ИД региона, возращаемому ПС
  ------------------------------------------------------------------------
  function getRegionByRegionPS(pi_reg_ps    in t_dic_regions_ps.ps_reg_id%type,
                               pi_worker_id in number,
                               po_err_num   out number,
                               po_err_msg   out varchar2)
    return sys_refcursor;
  ------------------------------------------------------------------------
  --получение справочника услуг ЦПО
  ------------------------------------------------------------------------
  function get_dic_org_ss_service(pi_worker_id in number,
                                  po_err_num   out number,
                                  po_err_msg   out varchar2)
    return sys_refcursor;
  ------------------------------------------------------------------------
  --получение списка ЦПО
  ------------------------------------------------------------------------
  function get_list_org_ss_center(pi_date_actual     in date,
                                  pi_reg_tab         in string_tab,
                                  pi_addr_obj_id     in t_address.ADDR_OBJ_ID%type, -- ID улицы (города) в ЕАК
                                  pi_house_obj_id    in t_address.ADDR_HOUSE_OBJ_ID%type, -- ID дома в ЕАК
                                  pi_addr_building   in t_address.addr_building%type,
                                  pi_addr_office     in t_address.addr_office%type, -- квартира
                                  pi_segment_service in number, -- сегмент обслуживания
                                  pi_num_page        in number, -- номер страницы
                                  pi_count_req       in number, -- кол-во записей на странице
                                  pi_column          in number, -- Номер колонки для сортировки
                                  pi_sorting         in number, -- 0-по возрастанияю, 1-по убыванию
                                  pi_worker_id       in number,
                                  po_timetable       out sys_refcursor,
                                  po_employee        out sys_refcursor,
                                  po_services        out sys_refcursor,
                                  po_all_count       out number, -- выводим общее кол-во записей, подходящих под условия
                                  po_err_num         out number,
                                  po_err_msg         out varchar2)
    return sys_refcursor;
  --------------------------------------------------------------------------------------
  -- Групповое редактирование ЦПО
  --------------------------------------------------------------------------------------
  PROCEDURE Update_Ssc_Info_Mass(PI_ORG_ID               IN ARRAY_NUM_2,
                                 PI_BLOCK                IN NUMBER,
                                 PI_ORG_RELATION         IN NUM_TAB,
                                 PI_SS_SERVICE           IN SS_SERVICE_TAB,
                                 PI_SSC_TIMETABLE        IN ORG_TIMETABLE_TAB,
                                 PI_UNRESERVED_TMC       IN array_num_2,
                                 PI_WORKER_ID            IN NUMBER,
                                 pi_cluster_id           in number,
                                 pi_ssc_employee         in org_employee_tab,
                                 pi_ssc_phone            in SSC_PHONES_tab,
                                 pi_is_pay_espp          in t_organizations.is_pay_espp%type,                               
                                 PO_ERR_NUM              OUT PLS_INTEGER,
                                 PO_ERR_MSG              OUT VARCHAR2);
  --------------------------------------------------------------------------------------
  --поиск орг-ций
  --------------------------------------------------------------------------------------
  function Get_Org_List(pi_org_name    in varchar2,
                        pi_inn         in t_organizations.org_inn%type,
                        pi_org_id      in t_organizations.org_id%type,
                        pi_dog_number  in varchar2,
                        pi_worker_id   in T_USERS.USR_ID%type,
                        pi_page_number in number,
                        pi_count_rows  in number,
                        pi_sort        in number, -- 1 по-возрастанию, 0 по убыванию
                        pi_column_sort in number,
                        po_count_all   out number,
                        po_err_num     out pls_integer,
                        po_err_msg     out varchar2) return sys_refcursor;
  --------------------------------------------------------------------------------------
  -- выгрузка справочника по протоколу ЕЛК
  --------------------------------------------------------------------------------------
  function get_org_dic(pi_worker_id in number,
                       pi_org_id    in t_organizations.org_id%type,
                       pi_hst_id    in t_organizations_hst.hst_id%type,
                       pi_kl_region in t_dic_region.kl_region%type,
                       po_err_num   out number,
                       po_err_msg   out varchar2) return sys_refcursor;
  --------------------------------------------------------------------------------------
  -- выгрузка справочника по протоколу ЕЛК
  --------------------------------------------------------------------------------------
  function get_org_hst(pi_hst_id  in t_organizations_hst.hst_id%type,
                       po_err_num out number,
                       po_err_msg out varchar2) return sys_refcursor;
  -----------------------------------------------------------------------------
  --Сохранение текущего состояния организации в историю
  -----------------------------------------------------------------------------
  function save_org_hst(pi_worker_id in number, pi_org_id in number)
    return number;
  -----------------------------------------------------------------------------
  -- Сохранение текущего состояния в историю
  -----------------------------------------------------------------------------
  function save_org_state(pi_worker_id in number, pi_org_id in number)
    return number;
  --смена канала продаж у организации при редактировании договора
  procedure change_org_channel_dog(pi_org_id    in number,
                                   pi_worker_id in number,
                                   po_err_num   out number,
                                   po_err_msg   out varchar2);
  ------------------------------------------------------------------------
  function get_list_dog_by_perm(pi_perm_id   in num_tab,
                                pi_org_id    in number,
                                pi_worker_id in number,
                                po_err_num   out pls_integer,
                                po_err_msg   out varchar2)
    return sys_refcursor;
  ------------------------------------------------------------------------
  -- Получение списка договоров Организации содержащих права
  ------------------------------------------------------------------------
  function get_dog_org_by_rigth(pi_rigth     in string_tab,
                                pi_org_id    in number,
                                pi_worker_id in number,
                                po_err_num   out number,
                                 po_err_msg   out varchar2,
                                 pi_dog_is_enabled NUMBER := 1
                                )
    return sys_refcursor;
  ------------------------------------------------------------------------
  procedure insert_TIMETABLE(PI_ORG_ID        IN T_ORGANIZATIONS.ORG_ID%type,
                             PI_SSC_TIMETABLE IN ORG_TIMETABLE_TAB,
                             pi_worker_id     in number,
                             po_err_num       out pls_integer,
                             po_err_msg       out varchar2);
  ------------------------------------------------------------------------
  function Get_Regions_for_mvno(pi_worker_id in number,
                                po_err_num   out number,
                                po_err_msg   out varchar2)
    return sys_refcursor;
  ------------------------------------------------------------------------
  --Получение списка организаций для общего отчета по заявкам
  ------------------------------------------------------------------------
  function Get_Orgs_structure(pi_org_tab           in num_tab,
                              pi_org_child_include in number,
                              pi_block             in number,
                              pi_org_relation      in num_tab,
                              pi_worker_id         in number,
                              po_err_num           out number,
                              po_err_msg           out varchar2)
    return sys_refcursor;
  ------------------------------------------------------------------------
  --Справочник Кластеров ЦПО
  ------------------------------------------------------------------------
  function get_dic_ssc_cluster(po_err_num out number,
                               po_err_msg out varchar2) return sys_refcursor;
  ------------------------------------------------------------------------
  --Справочник Должностей сотрудников организаций
  ------------------------------------------------------------------------
  function get_dic_org_employee(po_err_num out number,
                                po_err_msg out varchar2) return sys_refcursor;
  ------------------------------------------------------------------------
  --История изменения данных ЦПО
  ------------------------------------------------------------------------
  function get_org_ssc_history_change(pi_worker_id        in number,
                                      pi_regions          string_tab,
                                      pi_date_start       in date,
                                      pi_date_end         in date,
                                      pi_org_id           in num_tab,
                                      pi_is_child_include in number,

                                      pi_num_page  in number,
                                      pi_count_req in number,
                                      pi_column    in number,
                                      pi_sorting   in number,
                                      po_timetable out sys_refcursor,
                                      po_employee  out sys_refcursor,
                                      po_services  out sys_refcursor,
                                      po_all_count out number,
                                      po_err_num   out number,
                                      po_err_msg   out varchar2)
    return sys_refcursor;
  -------------------------------------------------
  --Получение списка организаций отображения при массовом редактировании ЦПО
  --------------------------------------------------
  function get_org_list_for_ssc_update(PI_ORG_ID       IN ARRAY_NUM_2,
                                       PI_BLOCK        IN NUMBER,
                                       PI_ORG_RELATION IN NUM_TAB,
                                       PI_WORKER_ID    IN NUMBER,
                                       PO_ERR_NUM      OUT PLS_INTEGER,
                                       PO_ERR_MSG      OUT VARCHAR2)
    return sys_refcursor;
  ----------------------------------------------------------------
  --Получение регионов пользователя
  ----------------------------------------------------------------
  function Get_Regions_User(pi_worker_id in number) return num_tab;

  -------------------------------------------------------------------
  --Определение является ли организация подчиненной
  -------------------------------------------------------------------
  function is_sub_org(pi_org_id        in number,
                      pi_parent_org_id in number,
                      po_err_num       out number,
                      po_err_msg       out varchar2) return number;

  -----------------------------------------------------------------------
  --Определение есть ли у организации указанный класс договора
  -----------------------------------------------------------------------
  function is_org_dog_class(pi_org_id       in number,
                            pi_dog_class_id in number,
                            po_err_num      out number,
                            po_err_msg      out varchar2) return number;

  ------------------------------------------------------------------
  -- Проверка, доступно ли редактирование договора
  ------------------------------------------------------------------
  procedure check_dog_m2m_type(pi_dog_id       in number,
                               pi_m2m_type     in number,
                               po_err_num      out number,
                               po_err_msg      out varchar2);

  FUNCTION get_all_available_mvno_regions RETURN SYS_REFCURSOR;

end ORGS;
/
