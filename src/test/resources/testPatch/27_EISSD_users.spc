CREATE OR REPLACE PACKAGE USERS is
  c_client_person     constant char := 'P';
  c_client_juristic   constant char := 'J';
  c_doc_type_passport constant pls_integer := 39;
  c_engeneer_role     constant number := 6959;
  -- Public type declarations
  subtype t_Err_Msg is varchar2(2000);
  subtype t_vc200 is varchar2(2000);
  subtype t_md5 is varchar2(32);
  subtype t_row is varchar2(10000);
  type double_id_type is record(
    ID1 pls_integer,
    ID2 pls_integer,
    ID3 pls_integer);
  type cur_ref_double_id_type is ref cursor return double_id_type;
  type rec_usr_info is record(
    USR_ID            T_USERS.USR_ID%type,
    USR_ORG_ID        T_ORGANIZATIONS.ORG_ID%type,
    USR_LOGIN         T_USERS.USR_LOGIN%type,
    PERSON_LASTNAME   T_PERSON.PERSON_LASTNAME%type,
    PERSON_FIRSTNAME  T_PERSON.PERSON_FIRSTNAME%type,
    PERSON_MIDDLENAME T_PERSON.PERSON_MIDDLENAME%type,
    PERSON_EMAIL      T_PERSON.PERSON_EMAIL%type,
    USR_STATUS        T_USERS.USR_STATUS%type);
  type cur_ref_usr_info is ref cursor return rec_usr_info;
  type rec_usr_info2 is record(
    USR_ID     T_USERS.USR_ID%type,
    USR_LOGIN  T_USERS.USR_LOGIN%type,
    ORG_ID     T_ORGANIZATIONS.ORG_ID%type,
    ORG_NAME   T_ORGANIZATIONS. ORG_NAME%type,
    LASTNAME   T_PERSON.PERSON_LASTNAME%type,
    FIRSTNAME  T_PERSON.PERSON_FIRSTNAME%type,
    MIDDLENAME T_PERSON.PERSON_MIDDLENAME%type,
    EMAIL      T_PERSON.PERSON_EMAIL%type,
    STATUS     T_USERS.USR_STATUS%type);
  type cur_ref_usr_info2 is ref cursor return rec_usr_info2;
  type cur_user is ref cursor;
  ----------------------------------------------------------------------------
  function Get_Org_Users_List(pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                              pi_worker_id in T_USERS.USR_ID%type,
                              po_err_num   out pls_integer,
                              po_err_msg   out t_Err_Msg)
    return sys_refcursor;
  ----------------------------------------------------------------------------
  function Get_User_By_Id(pi_user_id in T_USERS.USR_ID%type,
                          po_favor_menu out sys_refcursor)
    return sys_refcursor;  
  ----------------------------------------------------------------------------
  function Get_User_By_Id2(pi_user_id   in T_USERS.USR_ID%type,
                           pi_worker_id in T_USERS.USR_ID%type,
                           po_roles     out sys_refcursor,
                           po_err_num   out pls_integer,
                           po_err_msg   out t_Err_Msg) return sys_refcursor;
  ----------------------------------------------------------------------------
  function Get_User_By_Id2_for_mpz(pi_user_id   in T_USERS.USR_ID%type,
                                   pi_worker_id in T_USERS.USR_ID%type,
                                   po_favor_menu out sys_refcursor,
                                   po_err_num   out pls_integer,
                                   po_err_msg   out t_Err_Msg)
    return sys_refcursor;
  ----------------------------------------------------------------------------
  procedure Get_User_By_Id3(pi_worker_id in T_USERS.USR_ID%type,
                            po_orgs      out sys_refcursor,
                            po_roles     out sys_refcursor,
                            po_err_num   out pls_integer,
                            po_err_msg   out t_Err_Msg);
  ----------------------------------------------------------------------------
  function Get_Person_By_Name_And_Phone(pi_firstname  in T_PERSON.PERSON_FIRSTNAME%type,
                                        pi_lastname   in T_PERSON.PERSON_LASTNAME%type,
                                        pi_middlename in T_PERSON.PERSON_MIDDLENAME%type,
                                        pi_phone      in T_PERSON.PERSON_PHONE%type,
                                        pi_worker_id  in T_USERS.USR_ID%type,
                                        po_err_num    out pls_integer,
                                        po_err_msg    out t_Err_Msg)
    return sys_refcursor;
  ----------------------------------------------------------------------------
  
    
  function Ins_Address(pi_country     in T_ADDRESS.ADDR_COUNTRY%type,
                       pi_city        in T_ADDRESS.ADDR_CITY%type,
                       pi_index       in T_ADDRESS.ADDR_INDEX%type,
                       pi_street      in T_ADDRESS.ADDR_STREET%type,
                       pi_building    in T_ADDRESS.ADDR_BUILDING%type,
                       pi_office      in T_ADDRESS.ADDR_OFFICE%type,
                       pi_corp        in t_address.addr_corp%type,
                       pi_city_code   in T_ADDRESS.ADDR_CODE_CITY%type,
                       pi_street_code in T_ADDRESS.ADDR_CODE_STREET%type,
                       pi_region      in t_address.region_id%type,
                       -- Добавлены блок, строение и дробь
                       pi_addr_block     in t_address.ADDR_BLOCK%type,
                       pi_addr_structure in t_address.ADDR_STRUCTURE%type,
                       pi_addr_fraction  in t_address.ADDR_FRACTION%type,
                       pi_city_lvl       in t_address.addr_city_lvl%type,
                       pi_street_lvl     in t_address.addr_street_lvl%type,
                       pi_house_code     in t_address.addr_code_bld%type,
                       pi_addr_obj_id    in t_address.addr_obj_id%type,
                       pi_house_obj_id   in t_address.addr_house_obj_id%type)
    return T_ADDRESS.ADDR_ID%type;
  function Ins_Address(pi_country     in T_ADDRESS.ADDR_COUNTRY%type,
                       pi_city        in T_ADDRESS.ADDR_CITY%type,
                       pi_index       in T_ADDRESS.ADDR_INDEX%type,
                       pi_street      in T_ADDRESS.ADDR_STREET%type,
                       pi_building    in T_ADDRESS.ADDR_BUILDING%type,
                       pi_office      in T_ADDRESS.ADDR_OFFICE%type,
                       pi_corp        in t_address.addr_corp%type,
                       pi_city_code   in T_ADDRESS.ADDR_CODE_CITY%type,
                       pi_region      in T_DIC_REGION.KL_REGION%type,
                       -- Добавлены блок, строение и дробь
                       pi_addr_block     in t_address.ADDR_BLOCK%type,
                       pi_addr_structure in t_address.ADDR_STRUCTURE%type,
                       pi_addr_fraction  in t_address.ADDR_FRACTION%type,
                       pi_addr_obj_id    in t_address.addr_obj_id%type,
                       pi_house_obj_id   in t_address.addr_house_obj_id%type)
    return T_ADDRESS.ADDR_ID%type;
  ----------------------------------------------------------------------------
  function Ins_Document(pi_doc_ser  in T_DOCUMENTS.DOC_SERIES%type,
                        pi_doc_num  in T_DOCUMENTS.DOC_NUMBER%type,
                        pi_doc_date in T_DOCUMENTS.DOC_REGDATE%type,
                        pi_doc_info in T_DOCUMENTS.DOC_EXTRAINFO%type,
                        pi_doc_type in T_DOCUMENTS.DOC_TYPE%type)
    return T_DOCUMENTS.DOC_ID%type;
  ----------------------------------------------------------------------------
  function Ins_Person(pi_first_name            in T_PERSON.PERSON_FIRSTNAME%type,
                      pi_middle_name           in T_PERSON.PERSON_MIDDLENAME%type,
                      pi_last_name             in T_PERSON.PERSON_LASTNAME%type,
                      pi_sex                   in T_PERSON.PERSON_SEX%type,
                      pi_birth_day             in T_PERSON.PERSON_BIRTHDAY%type,
                      pi_inn                   in T_PERSON.PERSON_INN%type,
                      pi_email                 in T_PERSON.PERSON_EMAIL%type,
                      pi_phone                 in T_PERSON.PERSON_PHONE%type,
                      pi_reg_country           in T_ADDRESS.ADDR_COUNTRY%type,
                      pi_reg_city              in T_ADDRESS.ADDR_CITY%type,
                      pi_reg_post_index        in T_ADDRESS.ADDR_INDEX%type,
                      pi_reg_street            in T_ADDRESS.ADDR_STREET%type,
                      pi_reg_house             in T_ADDRESS.ADDR_BUILDING%type,
                      pi_reg_corp              in T_ADDRESS.Addr_Corp%type,
                      pi_reg_flat              in T_ADDRESS.ADDR_OFFICE%type,
                      pi_reg_code_city         in T_ADDRESS.ADDR_CODE_CITY%type,
                      pi_reg_code_street       in T_ADDRESS.ADDR_CODE_STREET%type,
                      pi_fact_country          in T_ADDRESS.ADDR_COUNTRY%type,
                      pi_fact_city             in T_ADDRESS.ADDR_CITY%type,
                      pi_fact_post_index       in T_ADDRESS.ADDR_INDEX%type,
                      pi_fact_street           in T_ADDRESS.ADDR_STREET%type,
                      pi_fact_house            in T_ADDRESS.ADDR_BUILDING%type,
                      pi_fact_corp             in T_ADDRESS.Addr_Corp%type,
                      pi_fact_flat             in T_ADDRESS.ADDR_OFFICE%type,
                      pi_fact_code_city        in T_ADDRESS.ADDR_CODE_CITY%type,
                      pi_fact_code_street      in T_ADDRESS.ADDR_CODE_STREET%type,
                      pi_passport_ser          in T_DOCUMENTS.DOC_SERIES%type,
                      pi_passport_num          in T_DOCUMENTS.DOC_NUMBER%type,
                      pi_passport_receive_date in T_DOCUMENTS.DOC_REGDATE%type,
                      pi_passport_receive_who  in T_DOCUMENTS.DOC_EXTRAINFO%type,
                      pi_sgs_id                in T_PERSON.SGS_ID%type,
                      pi_birthplace            in T_PERSON.BIRTHPLACE%TYPE,
                      pi_reg_region            in t_address.region_id%type,
                      pi_fact_region           in t_address.region_id%type,
                      pi_dlv_acc_type          in t_person.dlv_acc_type%type,
                      pi_HOME_PHONE            in t_person.person_home_phone%type,
                      pi_reg_city_lvl          in t_address.addr_city_lvl%type,
                      pi_reg_street_lvl        in t_address.addr_street_lvl%type,
                      pi_reg_house_code        in t_address.addr_code_bld%type,
                      pi_fact_city_lvl         in t_address.addr_city_lvl%type,
                      pi_fact_street_lvl       in t_address.addr_street_lvl%type,
                      pi_fact_house_code       in t_address.addr_code_bld%type,
                      pi_passport_type         in t_documents.doc_type%type,
                      pi_reg_addr_obj_id      in t_address.addr_obj_id%type,
                      pi_reg_house_obj_id     in t_address.addr_house_obj_id%type,
                      pi_fact_addr_obj_id      in t_address.addr_obj_id%type,
                      pi_fact_house_obj_id     in t_address.addr_house_obj_id%type)
    return T_PERSON.PERSON_ID%type;  
  ---------------------------------------------
  function Add_Physic_Client_short(pi_first_name   in T_PERSON.PERSON_FIRSTNAME%type,
                                   pi_middle_name  in T_PERSON.PERSON_MIDDLENAME%type,
                                   pi_last_name    in T_PERSON.PERSON_LASTNAME%type,
                                   pi_phone        in T_PERSON.PERSON_PHONE%type,
                                   pi_worker_id    in T_USERS.USR_ID%type,
                                   po_err_num      out pls_integer,
                                   po_err_msg      out t_Err_Msg)
    return T_CLIENTS.CLIENT_ID%type;
  ----------------------------------------------------------------------------
  function Add_Physic_Client(pi_first_name            in T_PERSON.PERSON_FIRSTNAME%type,
                             pi_middle_name           in T_PERSON.PERSON_MIDDLENAME%type,
                             pi_last_name             in T_PERSON.PERSON_LASTNAME%type,
                             pi_sex                   in T_PERSON.PERSON_SEX%type,
                             pi_birth_day             in T_PERSON.PERSON_BIRTHDAY%type,
                             pi_inn                   in T_PERSON.PERSON_INN%type,
                             pi_email                 in T_PERSON.PERSON_EMAIL%type,
                             pi_phone                 in T_PERSON.PERSON_PHONE%type,
                             pi_home_phone            in T_PERSON.PERSON_HOME_PHONE%type,
                             pi_reg_country           in T_ADDRESS.ADDR_COUNTRY%type,
                             pi_reg_city              in T_ADDRESS.ADDR_CITY%type,
                             pi_reg_post_index        in T_ADDRESS.ADDR_INDEX%type,
                             pi_reg_street            in T_ADDRESS.ADDR_STREET%type,
                             pi_reg_house             in T_ADDRESS.ADDR_BUILDING%type,
                             pi_reg_corp              in T_ADDRESS.ADDR_CORP%type,
                             pi_reg_flat              in T_ADDRESS.ADDR_OFFICE%type,
                             pi_reg_code_city         in T_ADDRESS.ADDR_CODE_CITY%type,
                             pi_reg_code_street       in T_ADDRESS.ADDR_CODE_STREET%type,
                             pi_fact_country          in T_ADDRESS.ADDR_COUNTRY%type,
                             pi_fact_city             in T_ADDRESS.ADDR_CITY%type,
                             pi_fact_post_index       in T_ADDRESS.ADDR_INDEX%type,
                             pi_fact_street           in T_ADDRESS.ADDR_STREET%type,
                             pi_fact_house            in T_ADDRESS.ADDR_BUILDING%type,
                             pi_fact_corp             in T_ADDRESS.ADDR_CORP%type,
                             pi_fact_flat             in T_ADDRESS.ADDR_OFFICE%type,
                             pi_fact_code_city        in T_ADDRESS.ADDR_CODE_CITY%type,
                             pi_fact_code_street      in T_ADDRESS.ADDR_CODE_STREET%type,
                             pi_passport_ser          in T_DOCUMENTS.DOC_SERIES%type,
                             pi_passport_num          in T_DOCUMENTS.DOC_NUMBER%type,
                             pi_passport_receive_date in T_DOCUMENTS.DOC_REGDATE%type,
                             pi_passport_receive_who  in T_DOCUMENTS.DOC_EXTRAINFO%type,
                             pi_person_id             in T_PERSON.PERSON_ID%type,
                             pi_sgs_id                in T_PERSON.SGS_ID%type,
                             pi_worker_id             in T_USERS.USR_ID%type,
                             pi_birthplace            in T_PERSON.BIRTHPLACE%type,
                             pi_reg_region            in t_address.region_id%type,
                             pi_fact_region           in t_address.region_id%type,
                             pi_is_resident           in t_clients.is_resident%type,
                             pi_reg_deadline          in t_clients.reg_deadline%type,
                             -- 55949
                             pi_dlv_acc_type    in t_person.dlv_acc_type%type,
                             pi_reg_city_lvl    in t_address.addr_city_lvl%type,
                             pi_reg_street_lvl  in t_address.addr_street_lvl%type,
                             pi_reg_house_code  in t_address.addr_code_bld%type,
                             pi_fact_city_lvl   in t_address.addr_city_lvl%type,
                             pi_fact_street_lvl in t_address.addr_street_lvl%type,
                             pi_fact_house_code in t_address.addr_code_bld%type,
                             pi_passport_type   in t_documents.doc_type%type,
                             pi_reg_addr_obj_id      in t_address.addr_obj_id%type,
                             pi_reg_house_obj_id     in t_address.addr_house_obj_id%type,
                             pi_fact_addr_obj_id      in t_address.addr_obj_id%type,
                             pi_fact_house_obj_id     in t_address.addr_house_obj_id%type,
                             po_err_num         out pls_integer,
                             po_err_msg         out t_Err_Msg)
    return T_CLIENTS.CLIENT_ID%type;  
  ----------------------------------------------------------------------------
    function Add_Juristic_Client(pi_name                       in T_JURISTIC.JUR_NAME%type,
                               pi_Name_Short                 in T_JURISTIC.Jur_Name_Short%type,
                               pi_ogrn                       in T_JURISTIC.JUR_OGRN%type,
                               pi_ogrn_Date                  in T_JURISTIC.JUR_OGRN_Date%type,
                               pi_ogrn_Place                 in T_JURISTIC.JUR_OGRN_Place%type,
                               pi_settl_acc                  in T_JURISTIC.JUR_SETTLEMENT_ACCOUNT%type,
                               pi_email                      in T_JURISTIC.JUR_EMAIL%type,
                               pi_bik                        in T_JURISTIC.JUR_BANK_BIK%type,
                               pi_country                    in T_ADDRESS.ADDR_COUNTRY%type,
                               pi_city                       in T_ADDRESS.ADDR_CITY%type,
                               pi_post_index                 in T_ADDRESS.ADDR_INDEX%type,
                               pi_street                     in T_ADDRESS.ADDR_STREET%type,
                               pi_house                      in T_ADDRESS.ADDR_BUILDING%type,
                               pi_corp                       in T_ADDRESS.Addr_Corp%type,
                               pi_flat                       in T_ADDRESS.ADDR_OFFICE%type,
                               pi_code_city                  in T_ADDRESS.ADDR_CODE_CITY%type,
                               pi_code_street                in T_ADDRESS.ADDR_CODE_STREET%type,
                               pi_resp_first_name            in T_PERSON.PERSON_FIRSTNAME%type,
                               pi_resp_middle_name           in T_PERSON.PERSON_MIDDLENAME%type,
                               pi_resp_last_name             in T_PERSON.PERSON_LASTNAME%type,
                               pi_resp_passport_ser          in T_DOCUMENTS.DOC_SERIES%type,
                               pi_resp_passport_num          in T_DOCUMENTS.DOC_NUMBER%type,
                               pi_resp_passport_receive_date in T_DOCUMENTS.DOC_REGDATE%type,
                               pi_resp_passport_receive_who  in T_DOCUMENTS.DOC_EXTRAINFO%type,
                               pi_resp_sex                   in T_PERSON.PERSON_SEX%type,
                               pi_resp_birth_day             in T_PERSON.PERSON_BIRTHDAY%type,
                               pi_resp_inn                   in T_PERSON.PERSON_INN%type,
                               pi_resp_email                 in T_PERSON.PERSON_EMAIL%type,
                               pi_resp_phone                 in T_PERSON.PERSON_PHONE%type,
                               pi_resp_doc_osn               in t_Documents.Doc_Type%type,
                               pi_resp_doc_osn_Date          in t_Documents.Doc_Regdate%type,
                               pi_resp_doc_osn_number        in t_Documents.Doc_Number%type,
                               pi_touch_first_name           in T_PERSON.PERSON_FIRSTNAME%type,
                               pi_touch_middle_name          in T_PERSON.PERSON_MIDDLENAME%type,
                               pi_touch_last_name            in T_PERSON.PERSON_LASTNAME%type,
                               pi_touch_sex                  in T_PERSON.PERSON_SEX%type,
                               pi_touch_email                in T_PERSON.PERSON_EMAIL%type,
                               pi_touch_phone                in T_PERSON.PERSON_PHONE%type,
                               pi_touch_birth_day            in T_PERSON.PERSON_BIRTHDAY%type,
                               pi_touch_inn                  in T_PERSON.PERSON_INN%type,
                               pi_boss_first_name            in T_PERSON.PERSON_FIRSTNAME%type,
                               pi_boss_middle_name           in T_PERSON.PERSON_MIDDLENAME%type,
                               pi_boss_last_name             in T_PERSON.PERSON_LASTNAME%type,
                               pi_boss_sex                   in T_PERSON.PERSON_SEX%type,
                               pi_boss_email                 in T_PERSON.PERSON_EMAIL%type,
                               pi_boss_phone                 in T_PERSON.PERSON_PHONE%type,
                               pi_boss_birth_day             in T_PERSON.PERSON_BIRTHDAY%type,
                               pi_boss_inn                   in T_PERSON.PERSON_INN%type,
                               pi_country2                   in T_ADDRESS.ADDR_COUNTRY%type,
                               pi_city2                      in T_ADDRESS.ADDR_CITY%type,
                               pi_post_index2                in T_ADDRESS.ADDR_INDEX%type,
                               pi_street2                    in T_ADDRESS.ADDR_STREET%type,
                               pi_house2                     in T_ADDRESS.ADDR_BUILDING%type,
                               pi_corp2                      in T_ADDRESS.Addr_Corp%type,
                               pi_flat2                      in T_ADDRESS.ADDR_OFFICE%type,
                               pi_code_city2                 in T_ADDRESS.ADDR_CODE_CITY%type,
                               pi_code_street2               in T_ADDRESS.ADDR_CODE_STREET%type,
                               pi_inn                        in T_JURISTIC.JUR_INN%type,
                               pi_kpp                        in T_JURISTIC.JUR_KPP%type,
                               pi_okpo                       in T_JURISTIC.JUR_OKPO%type,
                               pi_okved                      in T_JURISTIC.JUR_OKVED%type,
                               pi_threasury_name             in T_JURISTIC.JUR_THREASURY_NAME%type,
                               pi_threasury_acc              in T_JURISTIC.JUR_THREASURY_ACC%type,
                               Pi_jur_class                  in t_juristic.jur_class%type,
                               pi_jur_billing_group          in t_juristic.jur_billing_group%type,
                               --
                               pi_jur_billing_group_shpd in t_juristic.jur_billing_group_shpd%type,
                               pi_jur_funding_source     in t_juristic.jur_funding_source%type,
                               pi_jur_category           in t_juristic.jur_category%type,
                               pi_jur_person_manager     in t_juristic.jur_person_manager%type,
                               Pi_cat_id                 in t_juristic.jur_category%type,
                               pi_jur_accept_algorithm   in t_juristic.jur_accept_algorithm%type,
                               Pi_jur_without_accept     in t_juristic.jur_without_accept%type,
                               pi_corr_akk               in T_JURISTIC.JUR_CORR_AKK%type,
                               pi_jur_id                 in T_JURISTIC.JURISTIC_ID%type,
                               pi_sgs_id                 in T_PERSON.SGS_ID%type,
                               pi_worker_id              in T_USERS.USR_ID%type,
                               pi_jur_dlv_phone          in t_juristic.jur_dlv_phone%type,
                               pi_jur_dlv_email          in t_juristic.jur_dlv_email%type,
                               pi_jur_dlv_fax            in t_juristic.jur_dlv_fax%type,
                               pi_jur_dlv_telex          in t_juristic.jur_dlv_telex%type,
                               pi_jur_dlv_acc_type       in t_juristic.jur_dlv_acc_type%type,
                               pi_jur_bank_name          in t_juristic.jur_bank_name%type,
                               pi_jur_bank_department    in t_juristic.jur_bank_department%type,
                               pi_resp_birthplace        in T_PERSON.BIRTHPLACE%type,
                               pi_touch_birthplace       in T_PERSON.BIRTHPLACE%type,
                               pi_boss_birthplace        in T_PERSON.BIRTHPLACE%type,
                               pi_region1                in t_address.region_id%type,
                               pi_region2                in t_address.region_id%type,
                               pi_is_resident            in t_clients.is_resident%type,
                               pi_addr_obj_id1           in t_address.addr_obj_id%type,
                               pi_house_obj_id1          in t_address.addr_house_obj_id%type,
                               pi_addr_obj_id2           in t_address.addr_obj_id%type,
                               pi_house_obj_id2          in t_address.addr_house_obj_id%type,
                               po_err_num                out pls_integer,
                               po_err_msg                out t_Err_Msg)
    return T_CLIENTS.CLIENT_ID%type;
  ----------------------------------------------------------------------------
  function Get_Client_By_Id(pi_client_id in T_USERS.USR_ID%type,
                            pi_worker_id in T_USERS.USR_ID%type,
                            po_cl_type   out T_CLIENTS.CLIENT_TYPE%type,
                            po_err_num   out pls_integer,
                            po_err_msg   out t_Err_Msg) return cur_user;
  ----------------------------------------------------------------------------
  function Get_User_List_By_Org_Id(pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                                   pi_worker_id in T_USERS.USR_ID%type,
                                   po_err_num   out pls_integer,
                                   po_err_msg   out t_Err_Msg)
    return sys_refcursor;
  ----------------------------------------------------------------------------
  function Search_Users(pi_lname               in T_PERSON.PERSON_LASTNAME%type,
                        pi_fname               in T_PERSON.PERSON_FIRSTNAME%type,
                        pi_mname               in T_PERSON.PERSON_MIDDLENAME%type,
                        pi_id                  in T_USERS.USR_ID%type,
                        pi_login               in T_USERS.USR_LOGIN%type,
                        pi_all                 in pls_integer,
                        pi_ignore_blocked_user in pls_integer,
                        pi_worker_id           in T_USERS.USR_ID%type,
                        po_err_num             out pls_integer,
                        po_err_msg             out varchar2)
    return sys_refcursor;
  ----------------------------------------------------------------------------
  function Get_User_List(pi_org_id              in T_ORGANIZATIONS.ORG_ID%type,
                         pi_role_id             in t_roles.role_id%type,
                         pi_ignore_blocked_user in pls_integer,
                         pi_worker_id           in T_USERS.USR_ID%type,
                         po_err_num             out pls_integer,
                         po_err_msg             out varchar2)
    return sys_refcursor;
  ----------------------------------------------------------------------------
  function Get_User_List_By_page(pi_lname           in T_PERSON.PERSON_LASTNAME%type,
                                 pi_fname           in T_PERSON.PERSON_FIRSTNAME%type,
                                 pi_mname           in T_PERSON.PERSON_MIDDLENAME%type,
                                 pi_id              in T_USERS.USR_ID%type,
                                 pi_login           in t_users.usr_login%type,
                                 pi_login_tab       in string_tab,
                                 pi_org_id          in T_ORGANIZATIONS.ORG_ID%type,
                                 pi_curated_include in number,
                                 -- pi_all                 in pls_integer,
                                 pi_ignore_blocked_user in pls_integer,
                                 pi_worker_id           in T_USERS.USR_ID%type,
                                 pi_ORDER_NUM           in VARCHAR2, --Номер заявки САСП/SD
                                 pi_page_num            in number,
                                 pi_req_count           in number,
                                 pi_sort                in number, -- 1 по-возрастанию, 0 по убыванию
                                 pi_column              in number,
                                 pi_names               in STRING_TRIPLE_TAB, -- Список ФИО
                                 po_all_count           out number,
                                 po_err_num             out pls_integer,
                                 po_err_msg             out t_Err_Msg)
    return sys_refcursor;
  -- Перевызов.
  function Get_User_List_By_page(pi_lname               in T_PERSON.PERSON_LASTNAME%type,
                                 pi_fname               in T_PERSON.PERSON_FIRSTNAME%type,
                                 pi_mname               in T_PERSON.PERSON_MIDDLENAME%type,
                                 pi_id                  in T_USERS.USR_ID%type,
                                 pi_login               in t_users.usr_login%type,
                                 pi_login_tab           in string_tab,
                                 pi_org_id              in T_ORGANIZATIONS.ORG_ID%type,
                                 pi_curated_include     in number,
                                 pi_ignore_blocked_user in pls_integer,
                                 pi_worker_id           in T_USERS.USR_ID%type,
                                 pi_ORDER_NUM           in VARCHAR2, --Номер заявки САСП/SD
                                 pi_page_num            in number,
                                 pi_req_count           in number,
                                 pi_sort                in number, -- 1 по-возрастанию, 0 по убыванию
                                 pi_column              in number,
                                 po_all_count           out number,
                                 po_err_num             out pls_integer,
                                 po_err_msg             out t_Err_Msg)
    return sys_refcursor;
    
  ----------------------------------------------------------------------------
  function Get_Client_List(pi_cl_type   in T_CLIENTS.CLIENT_TYPE%type,
                           pi_all       in pls_integer,
                           pi_par1      in varchar2,
                           pi_par2      in varchar2,
                           pi_par3      in varchar2,
                           pi_par4      in varchar2,
                           pi_par5      in varchar2,
                           pi_par6      in varchar2,
                           pi_par7      in varchar2,
                           pi_par8      in varchar2,
                           pi_par9      in varchar2,
                           pi_par10     in varchar2,
                           pi_worker_id in T_USERS.USR_ID%type,
                           po_err_num   out pls_integer,
                           po_err_msg   out t_Err_Msg) return cur_user;
  ----------------------------------------------------------------------------
  function group_concat_user_roles(pi_worker_id in T_USER_ORG.USR_ID%type,
                                   pi_org_id    in T_USER_ORG.ORG_ID%type)
    return t_vc200;
  ------------------------------------------------------------------------
  procedure Block_User(pi_usr_id    in T_Users.Usr_Id%type,
                       pi_block     in pls_integer,
                       pi_order_num in t_users_hist.order_num%type,
                       -- 70937
                       pi_ip_address in t_users_hist.ip_address%type,
                       pi_worker_id  in T_USERS.USR_ID%type,
                       po_err_num    out pls_integer,
                       po_err_msg    out t_Err_Msg);
  ----------------------------------------------------------------------------
  function Get_Ed_Tar_By_Org(pi_org_id    in number,
                             pi_worker_id in number,
                             po_err_num   out pls_integer,
                             po_err_msg   out t_err_msg) return number;
  ----------------------------------------------------------------------------
  -- Поиск абонента по НШД или номеру телефона
  function Get_Abonent_By_Ab_Number(pi_ab_number      in varchar2,
                                    pi_worker_id      in T_USERS.USR_ID%type,
                                    pi_where          in number, -- поиск в ЕИССД - 0, СГС - 1
                                    po_tariff         out number,
                                    po_subs_number    out varchar2,
                                    po_imsi           out varchar2,
                                    po_fullname       out varchar2,
                                    po_birthday       out date,
                                    po_psp_series     out number,
                                    po_psp_number     out number,
                                    po_psp_issue_date out date,
                                    po_psp_issuer     out varchar2,
                                    po_reg_city       out varchar2,
                                    po_reg_street     out varchar2,
                                    po_reg_home       out number,
                                    po_reg_build      out number,
                                    po_reg_flat       out number,
                                    po_fact_city      out varchar2,
                                    po_fact_street    out varchar2,
                                    po_fact_home      out number,
                                    po_fact_build     out number,
                                    po_fact_flat      out number,
                                    po_err_num        out pls_integer,
                                    po_err_msg        out t_Err_Msg)
    return sys_refcursor;
  ----------------------------------------------------------------------------
  -- Получение адреса по идентификатору
  ----------------------------------------------------------------------------
  function getAddress(pi_address_id in t_address.addr_id%type)
    return sys_refcursor;
  ----------------------------------------------------------------------------
  --Аутентификация
  ----------------------------------------------------------------------------
  function Get_User_By_Login(pi_login          in T_USERS.USR_LOGIN%type,
                             pi_password_md5   in T_USERS.USR_PWD_MD5%type,
                             pi_hash_key       in T_SESSIONS.HASH_KEY%type,
                             pi_worker_ip      in T_SESSION_LOG.WORKER_IP%type := null,
                             pi_session_action in number)
    return sys_refcursor;
  ---------------------------------------------------------------
  --  регистрация под правами пользователя
  ---------------------------------------------------------------
  Function Create_User_Session_By_User_Id(pi_Worker_Id in T_SESSION_LOG.ID_WORKER%type,
                                          pi_worker_ip in T_SESSION_LOG.WORKER_IP%type := null,
                                          pi_hash_key  in T_SESSIONS.HASH_KEY%type)
    return sys_refcursor;
  ----------------------------------------------------------------------------
  /*Функционал для работы с полевыми инженерами!!!*/
  ----------------------------------------------------------------------------
  procedure add_user_qualifications(pi_user_id        in number,
                                    pi_qualifications in num_tab);
	----------------------------------------------------------------------------
    function Add_User(pi_org_id         in T_ORGANIZATIONS.ORG_ID%type,
                    pi_login          in T_USERS.USR_LOGIN%type,
                    pi_password_md5   in T_USERS.USR_PWD_MD5%type,
                    pi_firstname      in T_PERSON.PERSON_FIRSTNAME%type,
                    pi_middlename     in T_PERSON.PERSON_MIDDLENAME%type,
                    pi_lastname       in T_PERSON.PERSON_LASTNAME%type,
                    pi_email          in T_PERSON.PERSON_EMAIL%type,
                    pi_roles          in num_tab,
                    pi_worker_id      in T_USERS.USR_ID%type,
                    pi_qualifications in num_tab,
                    pi_scheds         in team_count_tab,
                    pi_system         in number,
                    -- 38664 Для прямого продавца
                    pi_person_phone  in t_person.person_phone%type, -- телефон
                    pi_date_login_to in t_users.date_login_to%type, --Дата действия учетной записи
                    pi_order_num     in t_users_hist.order_num%type,
                    -- 58633 Подписание договора по доверенности
                    pi_fio_dover             in t_users.fio_dover%type,
                    pi_position_dover        in t_users.position_dover%type,
                    pi_na_osnovanii_dover    in t_users.na_osnovanii_dover%type,
                    pi_address_dover         in t_users.address_dover%type,
                    pi_na_osnovanii_doc_type in t_users.na_osnovanii_doc_type%type,
                    pi_fio_dover_nominative  in t_users.fio_dover_nominative%type,
                    pi_boss_email            in t_users.boss_email%type,
                    -- 70937
                    pi_ip_address  in t_users_hist.ip_address%type,
                    -- 86340. Добавление Salt и HashAlgId.
                    pi_salt        in t_users.SALT%type,
                    pi_hash_alg_id in t_users.HASH_ALG_ID%type,
                    pi_employee_number t_users.employee_number%TYPE,
                    po_err_num    out pls_integer,
                    po_err_msg    out t_Err_Msg) return T_USERS.USR_ID%type;
  ----------------------------------------------------------------------------
  function Change_User(pi_user_id        in T_USER_ORG.USR_ID%type,
                       pi_login          in T_USERS.USR_LOGIN%type,
                       pi_password_md5   in T_USERS.USR_PWD_MD5%type,
                       pi_firstname      in T_PERSON.PERSON_FIRSTNAME%type,
                       pi_middlename     in T_PERSON.PERSON_MIDDLENAME%type,
                       pi_lastname       in T_PERSON.PERSON_LASTNAME%type,
                       pi_email          in T_PERSON.PERSON_EMAIL%type,
                       pi_roles          in num_tab,
                       pi_org_id         in T_USER_ORG.ORG_ID%type,
                       pi_worker_id      in T_USERS.USR_ID%type,
                       pi_qualifications in num_tab,
                       pi_scheds         in team_count_tab,
                       -- 38664 Для прямого продавца
                       pi_person_phone in t_person.person_phone%type, -- телефон
                       -- таймзона для пользователя
                       pi_tz_id         in t_users.tz_id%type,
                       pi_date_login_to in t_users.date_login_to%type,
                       pi_order_num     in t_users_hist.order_num%type,
                       -- 58633 Подписание договора по доверенности
                       pi_fio_dover             in t_users.fio_dover%type,
                       pi_position_dover        in t_users.position_dover%type,
                       pi_na_osnovanii_dover    in t_users.na_osnovanii_dover%type,
                       pi_address_dover         in t_users.address_dover%type,
                       pi_na_osnovanii_doc_type in t_users.na_osnovanii_doc_type%type,
                       pi_fio_dover_nominative  in t_users.fio_dover_nominative%type,
                       pi_boss_email            in t_users.boss_email%type,
                       -- 70937
                       pi_ip_address    in t_users_hist.ip_address%type,
                       -- 86340
                       pi_salt          in t_users.salt%type,
                       pi_hash_alg_id   in t_users.HASH_ALG_ID%type,
                       pi_employee_number t_users.employee_number%TYPE,
                       po_err_num    out pls_integer,
                       po_err_msg    out t_Err_Msg)
    return T_USERS.USR_ID%type;
  ----------------------------------------------------------------------------
  function Get_Timezones(po_err_num out pls_integer,
                         po_err_msg out t_Err_Msg) return sys_refcursor;
  ----------------------------------------------------------------------------
  function get_user_qualifications(pi_user_id in number) return sys_refcursor;
  ----------------------------------------------------------------------------
  function check_login_and_psswd(pi_login        in T_USERS.USR_LOGIN%type,
                                 pi_password_md5 in T_USERS.USR_PWD_MD5%type,
                                 pi_worker_id    in number,
                                 po_err_num      out pls_integer,
                                 po_err_msg      out varchar2) return number;
  function check_login(pi_login     in varchar2,
                       pi_org_id    in number,
                       pi_worker_id in number,
                       po_msg       out varchar2) return number;
  ----------------------------------------------------------------------------
  function get_org_by_user_id(pi_user_id   in number,
                              pi_worker_id in number,
                              po_err_num   out pls_integer,
                              po_err_msg   out varchar2) return number;
  ----------------------------------------------------------------------------
  function Get_Client_List_test(pi_cl_type   in T_CLIENTS.CLIENT_TYPE%type,
                                pi_all       in pls_integer,
                                pi_par1      in varchar2,
                                pi_par2      in varchar2,
                                pi_par3      in varchar2,
                                pi_par4      in varchar2,
                                pi_par5      in varchar2,
                                pi_par6      in varchar2,
                                pi_par7      in varchar2,
                                pi_par8      in varchar2,
                                pi_par9      in varchar2,
                                pi_par10     in varchar2,
                                pi_worker_id in T_USERS.USR_ID%type,
                                po_err_num   out pls_integer,
                                po_err_msg   out t_Err_Msg) return cur_user;
  -----------------------------------------------------------------------
  -- поиск пользователя по публичному ключу клиентского сертификата.
  -- Малышевский Задача № 32800
  -----------------------------------------------------------------------
  function get_user_by_cert(pi_cert      in varchar2,
                            pi_system_id in number,
                            po_system_id out number,
                            po_err_num   out pls_integer,
                            po_err_msg   out varchar2) return sys_refcursor;
  ---------------------------------------------------------------------------------------------
  -- 38664 Поиск прямых продавцов по организации
  ---------------------------------------------------------------------------------------------
  function Get_Direct_User_List_By_Org_Id(pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                                          pi_worker_id in T_USERS.USR_ID%type,
                                          po_err_num   out pls_integer,
                                          po_err_msg   out t_Err_Msg)
    return sys_refcursor;
  ---------------------------------------------------------------------------------------------
  --проверка принадлежности воркера организации
  ---------------------------------------------------------------------------------------------
  function Check_Worker_In_Org(pi_org_id            in t_organizations.org_id%type,
                               pi_childrens_include in pls_integer := 1, -- включать детей
                               pi_user_id           in t_users.usr_id%type,
                               pi_worker_id         in T_USERS.USR_ID%type,
                               po_err_num           out pls_integer,
                               po_err_msg           out varchar2)
    return number;
  ---------------------------------------------------------------------------------------------
  --получение данных справочника jur_billing_group
  ---------------------------------------------------------------------------------------------
  function get_jur_billing_group(po_err_num out pls_integer,
                                 po_err_msg out varchar2)
    return sys_refcursor;
  
  ---------------------------------------------------------------------------------------------
  -- Получение данных справочника типов подтверждающих документов V_DOC_TYPES
  ---------------------------------------------------------------------------------------------
  function get_v_doc_types(po_err_num out pls_integer,
                           po_err_msg out varchar2)
    return sys_refcursor;
    
  ---------------------------------------------------------------------------------------------
  -- 51553 Надо функцию, которая бы по воркеру возвращала, все ли выводить или только GSM
  --- 0- без ограничения
  --- 1- ограниченный функционал
  ---------------------------------------------------------------------------------------------
  function Check_Limit_for_User(pi_user_id in number,
                                po_err_num out number,
                                po_err_msg out varchar2) return number;
  ---------------------------------------------------------------------------------------------
  --52203 по пользователю определить, является ли он оператором или агентом
  ---------------------------------------------------------------------------------------------
  function Check_Is_Operator_Worker(pi_worker_id in number,
                                    po_is_head   out number,
                                    po_mrf       OUT SYS_REFCURSOR,
                                    po_err_num   out number,
                                    po_err_msg   out varchar2) return number;
  ---------------------------------------------------------------------------------------------
  -- Функция для изменения пользователем своего профиля
  ---------------------------------------------------------------------------------------------
  procedure Change_User_himself(pi_user_id      in T_USER_ORG.USR_ID%type,
                                pi_password_md5 in T_USERS.USR_PWD_MD5%type,
                                pi_email        in T_PERSON.PERSON_EMAIL%type,
                                pi_person_phone in t_person.person_phone%type, -- телефон
                                pi_tz_id        in t_users.tz_id%type,
                                -- 70937
                                pi_ip_address in t_users_hist.ip_address%type,
                                po_err_num    out number,
                                po_err_msg    out varchar2);
  --------------------------------------------------
  -- 57518 Функция, которая по воркеру возвращает список регионов, в которых он зарегистрирован
  --------------------------------------------------
  function Get_Region_by_Worker(pi_worker_id in number,
                                po_err_num   out number,
                                po_err_msg   out varchar2)
    return sys_refcursor;
  --------------------------------------------------
  --57416 получение истории изменений пользователя
  --------------------------------------------------
  function Get_User_hist(pi_user_id   in number,
                         pi_worker_id in number,
                         po_err_num   out number,
                         po_err_msg   out varchar2) return sys_refcursor;
  function is_user_usi(pi_user_id in number) return number;
  ----------------------------------------------------------------------------------------
  -- Принадлежность воркера организации
  ----------------------------------------------------------------------------------------
  function is_worker_in_org(pi_org_id    in t_organizations.org_id%type,
                            pi_worker_id in T_USERS.USR_ID%type,
                            po_err_num   out pls_integer,
                            po_err_msg   out varchar2) return number;
  ----------------------------------------------------------------------------------------
  -- Сохранение канала по умолчанию для пользователя
  -- %param pi_worker_id Иднетификатор пользователя
  -- %param pi_channel_id Идентификатор канала
  -- %param po_err_num Код ошибки
  -- %param po_err_msg Сообщение об ошибке
  ----------------------------------------------------------------------------------------
  procedure save_channel_user(pi_worker_id  in number,
                              pi_channel_id in number,
                              po_err_num    out number,
                              po_err_msg    out varchar2);
  -------------------------------------------------------------------
  --Сохранение параметров профиля для пользователя
  --%param pi_worker_id пользователь
  --%param pi_params парамеры
  --%param po_err_num код ошибки
  --%param po_err_msg сообщение об ошибке
  -------------------------------------------------------------------
  procedure save_param_user(pi_worker_id in number,
                            pi_params    in REQUEST_PARAM_TAB,
                            po_err_num   out number,
                            po_err_msg   out varchar2);

  ----------------------------------------------------------------------------------------
  -- Смена пароля
  ----------------------------------------------------------------------------------------
  function change_user_passwd(pi_user_id      in T_USER_ORG.USR_ID%type,
                              pi_password_md5 in T_USERS.USR_PWD_MD5%type,
                              pi_org_id       in number,
                              pi_ip_address   in t_users_hist.ip_address%type,
                              pi_change_type  in number, -- 1 - смена, 2 восстановление                              
                              pi_worker_id    in T_USERS.USR_ID%type,
                              pi_salt         in t_users.salt%type,
                              pi_hash_alg_id  in t_users.HASH_ALG_ID%type,
                              po_err_num      out pls_integer,
                              po_err_msg      out t_Err_Msg)
    return T_USERS.USR_ID%type;
    
  --------------------------------------------------
  --Создание или редактирование пользователей списком
  --------------------------------------------------
  function change_user_tab(pi_worker_id in number,
                           pi_user_info in user_tab,
                           po_err_num   out number,
                           po_err_msg   out varchar2) return user_res_tab;
  -------------------------------------------------
  --Массовая блокировка/разблокировка пользователей
  -------------------------------------------------
  function block_user_tab(pi_worker_id       in number,
                          pi_user_block_info in rec_num_3_str_1_tab,
                          -- 70937
                          pi_ip_address in t_users_hist.ip_address%type,
                          po_err_num    out number,
                          po_err_msg    out varchar2) return user_res_tab;
  -------------------------------------------------
  function get_general_org_by_users(pi_usrers  in num_tab,
                                    po_err_num out number,
                                    po_err_msg out varchar2)
    return sys_refcursor;
  --------------------------------------
  --Сохранение адреса, с получение наименований города и улицы по кодам
  --------------------------------------------
  function ins_address_with_name(pi_country        in T_ADDRESS.ADDR_COUNTRY%type,
                                 pi_city           in T_ADDRESS.ADDR_CITY%type,
                                 pi_index          in T_ADDRESS.ADDR_INDEX%type,
                                 pi_street         in T_ADDRESS.ADDR_STREET%type,
                                 pi_building       in T_ADDRESS.ADDR_BUILDING%type,
                                 pi_office         in T_ADDRESS.ADDR_OFFICE%type,
                                 pi_corp           in t_address.addr_corp%type,
                                 pi_city_code      in T_ADDRESS.ADDR_CODE_CITY%type,
                                 pi_street_code    in T_ADDRESS.ADDR_CODE_STREET%type,
                                 pi_kl_region      in T_DIC_REGION.KL_REGION%type,
                                 pi_addr_block     in t_address.ADDR_BLOCK%type,
                                 pi_addr_structure in t_address.ADDR_STRUCTURE%type,
                                 pi_addr_fraction  in t_address.ADDR_FRACTION%type,
                                 pi_city_lvl       in t_address.addr_city_lvl%type,
                                 pi_street_lvl     in t_address.addr_street_lvl%type,
                                 pi_house_code     in t_address.addr_code_bld%type,
                                 pi_addr_obj_id    in T_ADDRESS_OBJECT.id%type,
                                 pi_house_obj_id  in T_ADDRESS_HOUSE_OBJ.ID%type,
                                 po_err_num        out number,
                                 po_err_msg        out varchar2)
    return sys_refcursor;
  -------------------------------------------------------------
  --Определение регионов пользователя
  -------------------------------------------------------------  
  function get_region_by_worker_right2(pi_worker_id in number,
                                       pi_right_str in string_tab,
                                       pi_org_id    in num_tab)
    return ARRAY_NUM_2;
  -------------------------------------------------------------
  --по воркеру определяет принадлежность его (организаций в каких у него есть роли) к Ростелекому или РТ-Мобайлу
  -------------------------------------------------------------
  function Get_User_belonging(pi_worker_id in number,
                              po_err_num   out number,
                              po_err_msg   out varchar2) return sys_refcursor;
  ----------------------------------------------------------------------------
  -- РТ-Мобайл
  -- 1.1.11. Изменения в работе пользователей, находящихся в нескольких организациях
  -- Поиск организаций пользователя с заданными догорными пермишенами
  ----------------------------------------------------------------------------
  function getOrgByUserAndPerm(pi_user_id   in number,
                               pi_dog_perm  in num_tab,
                               pi_worker_id in number,
                               po_err_num   out pls_integer,
                               po_err_msg   out varchar2)
    return sys_refcursor;
  ----------------------------------------------------------------
  -- получение структур по пользователю
  ----------------------------------------------------------------
  procedure get_user_structures(pi_worker_id in number,
                                po_is_rtk    out boolean,
                                po_is_rtm    out boolean,
                                po_err_num   out number,
                                po_err_msg   out varchar2);
  -----------------------------------------------------------------------------
  -- 70937 Функция сохранения ссылки для смены пароля
  -----------------------------------------------------------------------------
  procedure saveLinkChangePasswd(pi_usr_login          in t_users.usr_login%type,
                                 pi_link_change_passwd in t_users.link_change_passwd%type,
                                 pi_worker_id          in t_users.usr_id%type,
                                 po_err_num            out pls_integer,
                                 po_err_msg            out t_Err_Msg);
  -----------------------------------------------------------------------------
  -- 70937 Функция получения юзера по ссылке для смены пароля
  -----------------------------------------------------------------------------
  function getUserByLinkChangePasswd(pi_link_change_passwd in t_users.link_change_passwd%type,
                                     pi_worker_id          in t_users.usr_id%type,
                                     po_err_num            out pls_integer,
                                     po_err_msg            out t_Err_Msg)
    return sys_refcursor;
  ----------------------------------------------------------------------------------------
  -- Принадлежность воркера организации (для мультивыбора)
  ----------------------------------------------------------------------------------------
  function is_worker_in_org1(pi_org_id    in num_tab,
                             pi_worker_id in T_USERS.USR_ID%type,
                             po_err_num   out pls_integer,
                             po_err_msg   out varchar2) return number;
  function Get_Regions_User(pi_worker_id in number) return num_tab;
  ----------------------------------------------------------------------------------------
  function Get_Dog_Perm_by_Worker_Id(pi_worker_id in number)
    return sys_refcursor;
  --------------------------------------------------------------------------------------  
  -- Получение регионов пользователя по праву (со списком прав)
  --------------------------------------------------------------------------------------
  function get_region_by_worker_right(pi_worker_id in number,
                                      pi_right     in string_tab,
                                      po_err_num   out number,
                                      po_err_msg   out varchar2)
    return string_tab; 
    
  ---------------------------------------------------------------------------------------
  -- Список регионов пользователя для MVNO
  ---------------------------------------------------------------------------------------
  function getMVNORegionByWorker(pi_worker_id in number,
                                 po_err_num   out number,
                                 po_err_msg   out varchar2)
    return sys_refcursor;
  ---------------------------------------------
  --сохранение клиента
  ---------------------------------------------
  function Add_Client_Physic(pi_client    client_type,
                             pi_worker_id in T_USERS.USR_ID%type,
                             po_err_num   out pls_integer,
                             po_err_msg   out varchar2)
    return T_CLIENTS.CLIENT_ID%type;    
  ---------------------------------------------
  --вывод клиента
  ---------------------------------------------  
   function Get_Client_Physic(pi_CLIENT_ID in T_CLIENTS.CLIENT_ID%type,
                              pi_worker_id in T_USERS.USR_ID%type,
                              po_err_num   out pls_integer,
                              po_err_msg   out varchar2) return client_type;    
                              
  --------------------------------------------------------------------------------------
  -- Получение регионов пользователя по списку прав с учетом организаций
  --------------------------------------------------------------------------------------
  function get_region_by_worker_RightOrg(pi_worker_id in number,
                                         pi_right     in string_tab,
                                         pi_orgs      in num_tab,
                                         po_err_num   out number,
                                         po_err_msg   out varchar2)
    return string_tab;     
  --------------------------------------------------------------------------------------                      
  --Получение списка пользователей, которых пользователь может выбрать при создании заявки
  --------------------------------------------------------------------------------------
  function Get_List_User_by_users(pi_org_id    t_organizations.org_id%type,
                                  pi_rights    in string_tab,
                                  pi_worker_id in T_USERS.USR_ID%type,
                                  po_err_num   out number,
                                  po_err_msg   out varchar2)
    return sys_refcursor;
    
  --------------------------------------------------------
  --Определение организации пользователя по умолчанию в регионе
  ---------------------------------------------------------
  function get_default_org_by_region(pi_worker_id in number,
                                     pi_kl_region in varchar2,
                                     po_err_num   out number,
                                     po_err_msg   out varchar2)
    return sys_refcursor;    
    
  ----------------------------------------------------------------------------------------
  -- Сохранение склада по умолчанию для пользователя
  -- %param pi_worker_id Иднетификатор пользователя
  -- %param pi_agent_id Идентификатор агента выдачи (t_lira_agent_equipment)
  -- %param pi_return_warehouse склад возврата по умолчанию
  -- %param po_err_num Код ошибки
  -- %param po_err_msg Сообщение об ошибке
  ----------------------------------------------------------------------------------------
  procedure save_agent_equipment_user(pi_worker_id in number,
                                      pi_agent_id  in number,
                                      pi_return_warehouse in number,
                                      po_err_num   out number,
                                      po_err_msg   out varchar2);    
                                                                      
    
end USERS;
/
