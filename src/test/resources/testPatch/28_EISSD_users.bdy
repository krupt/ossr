CREATE OR REPLACE PACKAGE BODY USERS is
  c_package constant varchar2(30) := 'USERS.';
  client_not_found_ex exception;
  ex_invalid_org_id exception;
  ex_invalid_login exception;
  --  rec pkg_sgs_link.rec_link;
  ex_connect_timeout EXCEPTION;
  PRAGMA EXCEPTION_INIT(ex_connect_timeout, -12170);
  ----------------------------------------------------------------------------

  ---------------------------------------------------------
  -- Проверка, доступен ли пользователь для редактирования
  ---------------------------------------------------------
  function is_user_editable(pi_worker_id in number, -- кто пытается редактировать
                            pi_user_id   in number) -- кого пытается редактировать
   return boolean is
    l_is_worker_rtk  number;
    l_is_worker_rtm  number;
    l_is_user_rtk    number;
    l_is_user_rtm    number;
    l_err_num        number;
    l_err_msg        varchar2(4000);
    l_user_belonging sys_refcursor;
  begin
    l_user_belonging := security_pkg.Get_User_belonging(pi_worker_id,
                                           l_err_num,
                                           l_err_msg);
    fetch l_user_belonging
      into l_is_worker_rtk, l_is_worker_rtm;

    l_user_belonging := security_pkg.Get_User_belonging(pi_user_id, l_err_num, l_err_msg);
    fetch l_user_belonging
      into l_is_user_rtk, l_is_user_rtm;
    if (l_is_user_rtk = 1 and l_is_worker_rtk = 0) or
       (l_is_user_rtm = 1 and l_is_worker_rtm = 0) then
      return false;
    end if;
    return true;
  end;

  function is_user_usi(pi_user_id in number) return number is
    l_res number := 0;
  begin
    Select count(*)
      into l_res
      from t_user_org tuo
      join mv_org_tree m
        on m.org_id = tuo.org_id
       and m.root_reltype = 1002
       and m.org_reltype <> 1009
     where tuo.usr_id = pi_user_id
       and rownum <= 1
          --исключаем ветку РРС
       and not exists (select tor.org_id
              from mv_org_tree tor
             where tor.org_id = 3
               and tuo.org_id <> 3
            connect by prior tor.org_pid = tor.org_id
             start with tor.org_id = tuo.org_id);
    return l_res;
  end;
  ----------------------------------------------------------------------------
  function Get_Org_Users_List(pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                              pi_worker_id in T_USERS.USR_ID%type,
                              po_err_num   out pls_integer,
                              po_err_msg   out t_Err_Msg)
    return sys_refcursor is
    res sys_refcursor := null;
  begin
    if (not Security_pkg.Check_Rights_str((case
                                            when is_org_usi(pi_org_id) = 0 then
                                             'EISSD.WORKER.LIST'
                                            else
                                             'EISSD.WORKER_USI.LIST'
                                          end),
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg,
                                          null,
                                          null)) then
      return res;
    end if;

    open res for
      select distinct U.USR_ID,
                      U.USR_LOGIN,
                      U.USR_PWD_MD5,
                      P.PERSON_FIRSTNAME,
                      P.PERSON_MIDDLENAME,
                      P.PERSON_LASTNAME,
                      P.PERSON_EMAIL,
                      U.USR_STATUS,
                      group_concat_user_roles(UO.USR_ID, pi_org_id) roles,
                      U.IS_ENABLED,
                      u.org_id,
                      u.date_login_to,
                      u.is_temp_passwd
        from T_USERS U, T_PERSON P, T_USER_ORG UO
       where U.USR_PERSON_ID = P.PERSON_ID
         and U.USR_ID = UO.USR_ID
         and UO.ORG_ID = pi_org_id
         and not UO.ROLE_ID = 0
       order by P.PERSON_LASTNAME asc, P.PERSON_FIRSTNAME asc;
    return res;
  end Get_Org_Users_List;
  ----------------------------------------------------------------------------
  function Get_User_By_Id(pi_user_id    in T_USERS.USR_ID%type,
                          po_favor_menu out sys_refcursor)
    return sys_refcursor is
  begin
    return security_pkg. Get_User_By_Id(pi_user_id, po_favor_menu);
  end Get_User_By_Id;
  ----------------------------------------------------------------------------
  function Get_User_By_Id2(pi_user_id   in T_USERS.USR_ID%type,
                           pi_worker_id in T_USERS.USR_ID%type,
                           po_roles     out sys_refcursor,
                           po_err_num   out pls_integer,
                           po_err_msg   out t_Err_Msg) return sys_refcursor is
  begin
    return security_pkg.Get_User_By_Id2(pi_user_id,
                                        pi_worker_id,
                                        po_roles,
                                        po_err_num,
                                        po_err_msg);
  end Get_User_By_Id2;
  ----------------------------------------------------------------------------
  function Get_User_By_Id2_for_mpz(pi_user_id    in T_USERS.USR_ID%type,
                                   pi_worker_id  in T_USERS.USR_ID%type,
                                   po_favor_menu out sys_refcursor,
                                   po_err_num    out pls_integer,
                                   po_err_msg    out t_Err_Msg)
    return sys_refcursor is
  begin
    return security_pkg.Get_User_By_Id2_for_mpz(pi_user_id,
                                                pi_worker_id,
                                                po_favor_menu,
                                                po_err_num,
                                                po_err_msg);
  end;
  ----------------------------------------------------------------------------
  procedure Get_User_By_Id3(pi_worker_id in T_USERS.USR_ID%type,
                            po_orgs      out sys_refcursor,
                            po_roles     out sys_refcursor,
                            po_err_num   out pls_integer,
                            po_err_msg   out t_Err_Msg) is
  begin
    open po_orgs for
      select distinct O.ORG_ID, O.ORG_NAME
        from T_USER_ORG UO, T_ORGANIZATIONS O
       where UO.USR_ID = pi_worker_id
         and UO.ORG_ID = O.ORG_ID
       order by O.ORG_NAME;

    open po_roles for
      select distinct UO.ORG_ID, R.ROLE_ID, R.ROLE_NAME
        from T_USER_ORG UO, T_ROLES R
       where UO.USR_ID = pi_worker_id
         and UO.ROLE_ID = R.ROLE_ID
       order by R.ROLE_NAME;
    po_err_num := 0;
    po_err_msg := '';
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return;
  end Get_User_By_Id3;
  ----------------------------------------------------------------------------
  function Get_Person_By_Name_And_Phone(pi_firstname  in T_PERSON.PERSON_FIRSTNAME%type,
                                        pi_lastname   in T_PERSON.PERSON_LASTNAME%type,
                                        pi_middlename in T_PERSON.PERSON_MIDDLENAME%type,
                                        pi_phone      in T_PERSON.PERSON_PHONE%type,
                                        pi_worker_id  in T_USERS.USR_ID%type,
                                        po_err_num    out pls_integer,
                                        po_err_msg    out t_Err_Msg)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select C.CLIENT_ID,
             P.PERSON_ID,
             P.PERSON_LASTNAME,
             P.PERSON_FIRSTNAME,
             P.PERSON_MIDDLENAME,
             P.PERSON_EMAIL,
             P.PERSON_PHONE,
             P.PERSON_INN,
             P.PERSON_BIRTHDAY,
             P.PERSON_SEX,
             D.DOC_ID            doc_id,
             D.DOC_SERIES        doc_series,
             D.DOC_NUMBER        doc_number,
             D.DOC_REGDATE       doc_date,
             D.DOC_EXTRAINFO     doc_info,
             D.DOC_TYPE          doc_type,
             A1.ADDR_ID          adr1_id,
             A1.ADDR_COUNTRY     adr1_country,
             A1.ADDR_INDEX       adr1_index,
             A1.ADDR_CITY        adr1_city,
             A1.ADDR_STREET      adr1_street,
             A1.ADDR_BUILDING    adr1_building,
             A1.ADDR_CORP        adr1_corp,
             A1.ADDR_OFFICE      adr1_office,
             A1.ADDR_CODE_CITY   adr1_code_city,
             A1.ADDR_CODE_STREET adr1_code_street,
             A2.ADDR_ID          adr2_id,
             A2.ADDR_COUNTRY     adr2_country,
             A2.ADDR_INDEX       adr2_index,
             A2.ADDR_CITY        adr2_city,
             A2.ADDR_STREET      adr2_street,
             A2.ADDR_BUILDING    adr2_building,
             A2.ADDR_CORP        adr2_corp,
             A2.ADDR_OFFICE      adr2_office,
             A2.ADDR_CODE_CITY   adr2_code_city,
             A2.ADDR_CODE_STREET adr2_code_street,
             C.CLIENT_TYPE,
             P.SGS_ID,
             null                STATUS_PSTN,
             null                STATUS_GSM,
             null                KOL_GSM,
             null                ED_TARIFF_ID,
             null                BALANCE,
             p.birthplace,
             a1.addr_block       adr1_addr_block,
             a1.addr_structure   adr1_addr_structure,
             a1.addr_fraction    adr1_addr_fraction,
             a2.addr_block       adr2_addr_block,
             a2.addr_structure   adr2_addr_structure,
             a2.addr_fraction    adr2_addr_fraction
        from T_CLIENTS   C,
             T_PERSON    P,
             T_ADDRESS   A1,
             T_ADDRESS   A2,
             T_DOCUMENTS D
       where P.PERSON_PHONE = pi_phone
         and P.PERSON_LASTNAME = pi_lastname
         and P.PERSON_FIRSTNAME = pi_firstname
         and P.PERSON_MIDDLENAME = pi_middlename
         and D.DOC_ID = P.DOC_ID
         and C.FULLINFO_ID = P.PERSON_ID
         and C.CLIENT_TYPE = c_client_person
         and A1.ADDR_ID(+) = P.ADDR_ID_REG
         and A2.ADDR_ID(+) = P.ADDR_ID_FACT
         and C.VER = (select MAX(CCC.VER)
                        from T_CLIENTS CCC, T_PERSON PPP
                       where CCC.FULLINFO_ID = PPP.PERSON_ID
                         and CCC.FID_1VER = C.FID_1VER
                         and CCC.CLIENT_TYPE = c_client_person
                         and PPP.PERSON_PHONE = pi_phone
                         and PPP.PERSON_LASTNAME = pi_lastname
                         and PPP.PERSON_FIRSTNAME = pi_firstname
                         and PPP.PERSON_MIDDLENAME = pi_middlename);
    return res;
  Exception
    when Others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm;
  end Get_Person_By_Name_And_Phone;
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
                      pi_reg_addr_obj_id       in t_address.addr_obj_id%type,
                      pi_reg_house_obj_id      in t_address.addr_house_obj_id%type,
                      pi_fact_addr_obj_id      in t_address.addr_obj_id%type,
                      pi_fact_house_obj_id     in t_address.addr_house_obj_id%type)
    return T_PERSON.PERSON_ID%type is
    res         T_PERSON.PERSON_ID%type := null;
    l_doc_id    pls_integer := null;
    addr1_id    number;
    addr2_id    number;
    l_pers_hash t_md5;
    l_row_pers  t_row;
    ex_unique_constraint exception;
    pragma exception_init(ex_unique_constraint, -00001);
    l_hash_vers t_person.hash_version%type := 2;
  begin
    l_row_pers := upper(trim(pi_last_name)) || ';' ||
                  upper(trim(pi_first_name)) || ';' ||
                  upper(trim(pi_middle_name)) || ';' ||
                  upper(trim(pi_email)) || ';' || upper(trim(pi_phone)) || ';' ||
                  upper(trim(pi_inn)) || ';' || upper(trim(pi_birth_day)) || ';' ||
                  upper(trim(pi_sex)) || ';' ||
                  upper(trim(pi_passport_ser)) || ';' ||
                  upper(trim(pi_passport_num)) || ';' ||
                  upper(trim(pi_passport_receive_date)) || ';' ||
                  upper(trim(pi_passport_receive_who)) || ';' ||
                  upper(trim(nvl(pi_passport_type, c_doc_type_passport))) || ';' ||
                  upper(nvl(trim(pi_reg_country), 'Россия')) || ';' ||
                  upper(trim(pi_reg_post_index)) || ';' ||
                  upper(trim(pi_reg_city)) || ';' ||
                  upper(trim(pi_reg_street)) || ';' ||
                  upper(trim(pi_reg_house)) || ';' ||
                  upper(trim(pi_reg_flat)) || ';' ||
                  upper(trim(pi_reg_code_city)) || ';' ||
                  upper(trim(pi_reg_code_street)) || ';' ||
                  upper(nvl(trim(pi_fact_country), 'Россия')) || ';' ||
                  upper(trim(pi_fact_post_index)) || ';' ||
                  upper(trim(pi_fact_city)) || ';' ||
                  upper(trim(pi_fact_street)) || ';' ||
                  upper(trim(pi_fact_house)) || ';' ||
                  upper(trim(pi_fact_flat)) || ';' ||
                  upper(trim(pi_fact_code_city)) || ';' ||
                  upper(trim(pi_fact_code_street)) || ';' ||
                  upper(trim(c_client_person)) || ';' ||
                  upper(trim(pi_reg_corp)) || ';' ||
                  upper(trim(pi_fact_corp)) || ';' ||
                  upper(trim(pi_birthplace)) || ';' ||
                  upper(trim(pi_home_phone)) || ';' ||
                  upper(trim(pi_reg_region)) || ';' ||
                  upper(trim(pi_fact_region)) || ';' ||
                  upper(trim(pi_sgs_id)) || ';' ||
                  upper(trim(pi_dlv_acc_type)) || ';' ||
                  trim(to_char(pi_reg_addr_obj_id)) || ';' ||
                  trim(to_char(pi_reg_house_obj_id)) || ';' ||
                  trim(to_char(pi_fact_addr_obj_id)) || ';' ||
                  trim(to_char(pi_fact_house_obj_id));
    --костыль для тех у кого ничего нет
    if (l_row_pers =
       upper(trim(c_doc_type_passport)) || upper(trim(c_client_person))) then
      return null;
    end if;

    if (l_row_pers is not null) then
      l_pers_hash := Compute_MD5Hash(l_row_pers);
      -- insert person
      begin
        select max(nvl(P.PERSON_ID, 0))
          into res
          from T_PERSON P
         where p.person_hash is not null
           and l_pers_hash = p.person_hash
           and p.hash_version = l_hash_vers;

      exception
        when others then
          res := 0;
      end;
      if nvl(res, 0) = 0 then
        -- add reg address
        addr1_id := Ins_Address(pi_reg_country,
                                pi_reg_city,
                                pi_reg_post_index,
                                pi_reg_street,
                                pi_reg_house,
                                pi_reg_flat,
                                pi_reg_corp,
                                pi_reg_code_city,
                                pi_reg_code_street,
                                pi_reg_region,
                                null,
                                null,
                                null,
                                pi_reg_city_lvl,
                                pi_reg_street_lvl,
                                pi_reg_house_code,
                                pi_reg_addr_obj_id,
                                pi_reg_house_obj_id);
        -- add fact address
        addr2_id := Ins_Address(pi_fact_country,
                                pi_fact_city,
                                pi_fact_post_index,
                                pi_fact_street,
                                pi_fact_house,
                                pi_fact_flat,
                                pi_fact_corp,
                                pi_fact_code_city,
                                pi_fact_code_street,
                                pi_fact_region,
                                null,
                                null,
                                null,
                                pi_fact_city_lvl,
                                pi_fact_street_lvl,
                                pi_fact_house_code,
                                pi_fact_addr_obj_id,
                                pi_fact_house_obj_id);
        -- insert passport data
        l_doc_id := Ins_Document(pi_passport_ser,
                                 pi_passport_num,
                                 pi_passport_receive_date,
                                 pi_passport_receive_who,
                                 nvl(pi_passport_type, c_doc_type_passport));

        begin
          insert into T_PERSON
            (DOC_ID,
             PERSON_PHONE,
             PERSON_EMAIL,
             PERSON_LASTNAME,
             PERSON_FIRSTNAME,
             PERSON_MIDDLENAME,
             PERSON_INN,
             PERSON_BIRTHDAY,
             PERSON_SEX,
             ADDR_ID_REG,
             ADDR_ID_FACT,
             SGS_ID,
             PERSON_HASH,
             BIRTHPLACE,
             PERSON_HOME_PHONE,
             DLV_ACC_TYPE,
             hash_version)
          values
            (l_doc_id,
             SUBSTR(pi_phone, 0, 16),
             SUBSTR(pi_email, 0, 255),
             SUBSTR(pi_last_name,0,30),
             SUBSTR(pi_first_name,0,30),
             SUBSTR(pi_middle_name,0,30),
             SUBSTR(pi_inn, 0, 30),
             pi_birth_day,
             pi_sex,
             addr1_id,
             addr2_id,
             pi_sgs_id,
             l_pers_hash,
             pi_birthplace,
             pi_home_phone,
             pi_dlv_acc_type,
             l_hash_vers)
          returning PERSON_ID into res;
        exception
          when ex_unique_constraint then
            select P.PERSON_ID
              into res
              from T_PERSON P
             where p.person_hash is not null
               and p.person_hash = l_pers_hash
               and p.hash_version = l_hash_vers;
        end;
      end if;
    end if;
    return res;
  end Ins_Person;
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
    return T_ADDRESS.ADDR_ID%type is
  begin
    return addresse_pkg.Ins_Address(pi_country,
                                   pi_city,
                                   pi_index,
                                   pi_street,
                                   pi_building,
                                   pi_office,
                                   pi_corp,
                                   pi_city_code,
                                   pi_city_lvl,
                                   pi_street_code,
                                   pi_street_lvl,
                                   pi_house_code,
                                   null,
                                   pi_region,
                                   pi_addr_block,
                                   pi_addr_structure,
                                   pi_addr_fraction,
                                   pi_addr_obj_id,
                                   pi_house_obj_id);
  end Ins_Address;

  function Ins_Address(pi_country   in T_ADDRESS.ADDR_COUNTRY%type,
                       pi_city      in T_ADDRESS.ADDR_CITY%type,
                       pi_index     in T_ADDRESS.ADDR_INDEX%type,
                       pi_street    in T_ADDRESS.ADDR_STREET%type,
                       pi_building  in T_ADDRESS.ADDR_BUILDING%type,
                       pi_office    in T_ADDRESS.ADDR_OFFICE%type,
                       pi_corp      in t_address.addr_corp%type,
                       pi_city_code in T_ADDRESS.ADDR_CODE_CITY%type,
                       pi_region    in T_DIC_REGION.KL_REGION%type,
                       -- Добавлены блок, строение и дробь
                       pi_addr_block     in t_address.ADDR_BLOCK%type,
                       pi_addr_structure in t_address.ADDR_STRUCTURE%type,
                       pi_addr_fraction  in t_address.ADDR_FRACTION%type,
                       pi_addr_obj_id    in t_address.addr_obj_id%type,
                       pi_house_obj_id   in t_address.addr_house_obj_id%type)
    return T_ADDRESS.ADDR_ID%type is
    l_region number;
  begin

    if pi_region is not null then
      SELECT dr.reg_id
        INTO l_region
        FROM T_DIC_REGION dr
       WHERE dr.KL_REGION = pi_region;
    end if;

    return addresse_pkg.Ins_Address(pi_country        => pi_country,
                                   pi_city           => pi_city,
                                   pi_index          => pi_index,
                                   pi_street         => pi_street,
                                   pi_building       => pi_building,
                                   pi_office         => pi_office,
                                   pi_corp           => pi_corp,
                                   pi_city_code      => pi_city_code,
                                   pi_city_lvl       => null,
                                   pi_street_code    => null,
                                   pi_street_lvl     => null,
                                   pi_house_code     => null,
                                   pi_addr_Oth       => null,
                                   pi_region         => l_region,
                                   pi_addr_block     => pi_addr_block,
                                   pi_addr_structure => pi_addr_structure,
                                   pi_addr_fraction  => pi_addr_fraction,
                                   pi_addr_obj_id    => pi_addr_obj_id,
                                   pi_house_obj_id   => pi_house_obj_id);
  end Ins_Address;
  ----------------------------------------------------------------------------
  function Ins_Document(pi_doc_ser  in T_DOCUMENTS.DOC_SERIES%type,
                        pi_doc_num  in T_DOCUMENTS.DOC_NUMBER%type,
                        pi_doc_date in T_DOCUMENTS.DOC_REGDATE%type,
                        pi_doc_info in T_DOCUMENTS.DOC_EXTRAINFO%type,
                        pi_doc_type in T_DOCUMENTS.DOC_TYPE%type)
    return T_DOCUMENTS.DOC_ID%type is
    res        T_DOCUMENTS.DOC_ID%type := null;
    ex_unique_constraint exception;
    pragma exception_init(ex_unique_constraint, -00001);
    l_err_num number;
    l_err_msg varchar2(4000);
  begin     
    res := client_pkg.Ins_Document(pi_docum   => doc_type(DOC_TYPE      =>pi_doc_type,
                                                          DOC_SERIES    =>pi_doc_ser,
                                                          DOC_NUMBER    =>pi_doc_num,
                                                          DOC_REGDATE   =>pi_doc_date,
                                                          DOC_EXTRAINFO =>pi_doc_info),
                                   po_err_num => l_err_num,
                                   po_err_msg => l_err_msg);
    return res;
  end Ins_Document;
  ---------------------------------------------
  function Add_Physic_Client_short(pi_first_name  in T_PERSON.PERSON_FIRSTNAME%type,
                                   pi_middle_name in T_PERSON.PERSON_MIDDLENAME%type,
                                   pi_last_name   in T_PERSON.PERSON_LASTNAME%type,
                                   pi_phone       in T_PERSON.PERSON_PHONE%type,
                                   pi_worker_id   in T_USERS.USR_ID%type,
                                   po_err_num     out pls_integer,
                                   po_err_msg     out t_Err_Msg)
    return T_CLIENTS.CLIENT_ID%type is
  begin
    return client_pkg.save_client_phys(pi_worker_id => pi_worker_id,
                                       pi_client    => client_type(CLIENT_ID     => null,
                                                                   first_name    => pi_first_name,
                                                                   last_name     => pi_last_name,
                                                                   middle_name   => pi_middle_name,
                                                                   sex           => null,
                                                                   is_resident   => null,
                                                                   birth_day     => null,
                                                                   birthplace    => null,
                                                                   email         => null,
                                                                   phone         => pi_phone,
                                                                   document_type => null,
                                                                   reg_address   => null,
                                                                   fact_address  => null,
                                                                   DLV_ACC_TYPE  => null,
                                                                   phone_home    => null),
                                       po_err_num   => po_err_num,
                                       po_err_msg   => po_err_msg);
    /*return users.Add_Physic_Client(pi_first_name            => pi_first_name,
    pi_middle_name           => pi_middle_name,
    pi_last_name             => pi_last_name,
    pi_sex                   => null,
    pi_birth_day             => null,
    pi_inn                   => null,
    pi_email                 => null,
    pi_phone                 => pi_phone,
    pi_home_phone            => null,
    pi_reg_country           => null,
    pi_reg_city              => null,
    pi_reg_post_index        => null,
    pi_reg_street            => null,
    pi_reg_house             => null,
    pi_reg_corp              => null,
    pi_reg_flat              => null,
    pi_reg_code_city         => null,
    pi_reg_code_street       => null,
    pi_fact_country          => null,
    pi_fact_city             => null,
    pi_fact_post_index       => null,
    pi_fact_street           => null,
    pi_fact_house            => null,
    pi_fact_corp             => null,
    pi_fact_flat             => null,
    pi_fact_code_city        => null,
    pi_fact_code_street      => null,
    pi_passport_ser          => null,
    pi_passport_num          => null,
    pi_passport_receive_date => null,
    pi_passport_receive_who  => null,
    pi_person_id             => null,
    pi_sgs_id                => null,
    pi_worker_id             => pi_worker_id,
    pi_birthplace            => null,
    pi_reg_region            => null,
    pi_fact_region           => null,
    pi_is_resident           => null,
    pi_reg_deadline          => null,
    pi_dlv_acc_type          => null,
    
    pi_reg_city_lvl    => null,
    pi_reg_street_lvl  => null,
    pi_reg_house_code  => null,
    pi_fact_city_lvl   => null,
    pi_fact_street_lvl => null,
    pi_fact_house_code => null,
    pi_passport_type   => null,
    
    pi_reg_addr_obj_id   => null,
    pi_reg_house_obj_id  => null,
    pi_fact_addr_obj_id  => null,
    pi_fact_house_obj_id => null,
    
    po_err_num => po_err_num,
    po_err_msg => po_err_msg);*/
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg,
                        'users.Add_Physic_Client');
      return - 1;
  end;
  ---------------------------------------------
  --вывод клиента
  ---------------------------------------------
  function Get_Client_Physic(pi_CLIENT_ID in T_CLIENTS.CLIENT_ID%type,
                             pi_worker_id in T_USERS.USR_ID%type,
                             po_err_num   out pls_integer,
                             po_err_msg   out varchar2) return client_type is
    c_pr_name constant varchar2(100) := c_package || '.Get_Client_Physic';
    res            client_type;
    l_reg_address  ADDRESS_TYPE;
    l_fact_address ADDRESS_TYPE;
    l_reg_addrId   number;
    l_fact_addrId  number;
  begin
    select p.addr_id_reg, p.addr_id_fact
      into l_reg_addrId, l_fact_addrId
      from t_clients t
      join t_person p
        on p.person_id = t.fullinfo_id
     where t.client_id = pi_CLIENT_ID;
    l_reg_address  := ADDRESS_TYPE(null, null, null, null, null, null); /*addresse_pkg.get_address_type(pi_addr_id => l_reg_addrId,
                                                       po_err_num => po_err_num,
                                                       po_err_msg => po_err_msg);*/
    l_fact_address := ADDRESS_TYPE(null, null, null, null, null, null); /*addresse_pkg.get_address_type(pi_addr_id => l_fact_addrId,
                                                       po_err_num => po_err_num,
                                                       po_err_msg => po_err_msg);*/
    select client_type(CLIENT_ID     => NULL,
                       first_name    => p.person_firstname,
                       last_name     => p.person_lastname,
                       middle_name   => p.person_middlename,
                       sex           => p.person_sex,
                       is_resident   => t.is_resident,
                       birth_day     => p.person_birthday,
                       birthplace    => p.birthplace,
                       email         => p.person_email,
                       phone         => p.person_phone,
                       document_type => doc_type(DOC_TYPE      => d.doc_type,
                                                 DOC_SERIES    => d.doc_series,
                                                 DOC_NUMBER    => d.doc_number,
                                                 DOC_REGDATE   => d.doc_regdate,
                                                 DOC_EXTRAINFO => d.doc_extrainfo),
                       reg_address   => l_reg_address,
                       fact_address  => l_fact_address,
                       DLV_ACC_TYPE  => p.dlv_acc_type,
                       phone_home    => p.person_home_phone)
      into res
      from t_clients t
      join t_person p
        on p.person_id = t.fullinfo_id
      left join t_documents d
        on d.doc_id = p.doc_id
      left join t_address a1
        on a1.addr_id = p.addr_id_fact
      left join t_address_object ao1
        on ao1.id = a1.addr_obj_id
      left join t_address a2
        on a2.addr_id = p.addr_id_reg
      left join t_address_object ao2
        on ao2.id = a2.addr_obj_id
     where t.client_id = pi_CLIENT_ID;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg, c_pr_name);
      return null;
  end;
  ---------------------------------------------
  --сохранение клиента
  ---------------------------------------------
  function Add_Client_Physic(pi_client    client_type,
                             pi_worker_id in T_USERS.USR_ID%type,
                             po_err_num   out pls_integer,
                             po_err_msg   out varchar2)
    return T_CLIENTS.CLIENT_ID%type is
    c_pr_name constant varchar2(100) := c_package || '.Add_Client_Physic';
  begin 
    return client_pkg.save_client_phys(pi_worker_id => pi_worker_id,
                                       pi_client    => pi_client,
                                       po_err_num   => po_err_num,
                                       po_err_msg   => po_err_msg);
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg, c_pr_name);
      return - 1;
  end;
  ---------------------------------------------
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
                             pi_dlv_acc_type          in t_person.dlv_acc_type%type,
                             pi_reg_city_lvl          in t_address.addr_city_lvl%type,
                             pi_reg_street_lvl        in t_address.addr_street_lvl%type,
                             pi_reg_house_code        in t_address.addr_code_bld%type,
                             pi_fact_city_lvl         in t_address.addr_city_lvl%type,
                             pi_fact_street_lvl       in t_address.addr_street_lvl%type,
                             pi_fact_house_code       in t_address.addr_code_bld%type,
                             pi_passport_type         in t_documents.doc_type%type,
                             pi_reg_addr_obj_id       in t_address.addr_obj_id%type,
                             pi_reg_house_obj_id      in t_address.addr_house_obj_id%type,
                             pi_fact_addr_obj_id      in t_address.addr_obj_id%type,
                             pi_fact_house_obj_id     in t_address.addr_house_obj_id%type,
                             po_err_num               out pls_integer,
                             po_err_msg               out t_Err_Msg)
    return T_CLIENTS.CLIENT_ID%type is
    l_person_id    T_PERSON.PERSON_ID%type;
    l_person_hash  t_md5;
    res            T_CLIENTS.CLIENT_ID%type := -1;
    version_id     pls_integer := 1;
    l_oldpers_id   T_PERSON.PERSON_ID%type;
    l_hash_version t_clients.hash_version%type := 2;
  begin
    logging_pkg.info('
    pi_first_name= ' || pi_first_name || '
    pi_middle_name= ' || pi_middle_name || '
    pi_last_name= ' || pi_last_name || '
    pi_sex= ' || pi_sex || '
    pi_birth_day= ' || pi_birth_day || '
    pi_inn= ' || pi_inn || '
    pi_email= ' || pi_email || '
    pi_phone= ' || pi_phone || '
    pi_reg_country= ' || pi_reg_country || '
    pi_reg_city= ' || pi_reg_city || '
    pi_reg_post_index= ' || pi_reg_post_index || '
    pi_reg_street= ' || pi_reg_street || '
    pi_reg_house= ' || pi_reg_house || '
    pi_reg_corp= ' || pi_reg_corp || '
    pi_reg_flat= ' || pi_reg_flat || '
    pi_reg_code_city= ' || pi_reg_code_city || '
    pi_reg_code_street= ' || pi_reg_code_street || '
    pi_passport_ser= ' || pi_passport_ser || '
    pi_passport_num= ' || pi_passport_num || '
    pi_passport_receive_date= ' ||
                     pi_passport_receive_date || '
    pi_passport_receive_who= ' ||
                     pi_passport_receive_who || '
    pi_person_id= ' || pi_person_id || '
    pi_sgs_id= ' || pi_sgs_id || '
    pi_worker_id= ' || pi_worker_id || '
    pi_reg_region= ' || pi_reg_region || '
    pi_fact_region= ' || pi_fact_region,
                     'Add_Physic_Client');

    savepoint save_point;
    logging_pkg.info('psp_who=' || pi_passport_receive_who,
                     c_package || 'Add_Physic_Client');

    l_person_id := Ins_Person(pi_first_name            => pi_first_name,
                              pi_middle_name           => pi_middle_name,
                              pi_last_name             => pi_last_name,
                              pi_sex                   => pi_sex,
                              pi_birth_day             => pi_birth_day,
                              pi_inn                   => pi_inn,
                              pi_email                 => pi_email,
                              pi_phone                 => pi_phone,
                              pi_reg_country           => pi_reg_country,
                              pi_reg_city              => pi_reg_city,
                              pi_reg_post_index        => pi_reg_post_index,
                              pi_reg_street            => pi_reg_street,
                              pi_reg_house             => pi_reg_house,
                              pi_reg_corp              => pi_reg_corp,
                              pi_reg_flat              => pi_reg_flat,
                              pi_reg_code_city         => pi_reg_code_city,
                              pi_reg_code_street       => pi_reg_code_street,
                              pi_fact_country          => pi_fact_country,
                              pi_fact_city             => pi_fact_city,
                              pi_fact_post_index       => pi_fact_post_index,
                              pi_fact_street           => pi_fact_street,
                              pi_fact_house            => pi_fact_house,
                              pi_fact_corp             => pi_fact_corp,
                              pi_fact_flat             => pi_fact_flat,
                              pi_fact_code_city        => pi_fact_code_city,
                              pi_fact_code_street      => pi_fact_code_street,
                              pi_passport_ser          => pi_passport_ser,
                              pi_passport_num          => pi_passport_num,
                              pi_passport_receive_date => pi_passport_receive_date,
                              pi_passport_receive_who  => pi_passport_receive_who,
                              pi_sgs_id                => pi_sgs_id,
                              pi_birthplace            => pi_birthplace,
                              pi_reg_region            => pi_reg_region,
                              pi_fact_region           => pi_fact_region,
                              pi_dlv_acc_type          => pi_dlv_acc_type,
                              pi_home_phone            => pi_home_phone,
                              pi_reg_city_lvl          => pi_reg_city_lvl,
                              pi_reg_street_lvl        => pi_reg_street_lvl,
                              pi_reg_house_code        => pi_reg_house_code,
                              pi_fact_city_lvl         => pi_fact_city_lvl,
                              pi_fact_street_lvl       => pi_fact_street_lvl,
                              pi_fact_house_code       => pi_fact_house_code,
                              pi_passport_type         => pi_passport_type,
                              pi_reg_addr_obj_id       => pi_reg_addr_obj_id,
                              pi_reg_house_obj_id      => pi_reg_house_obj_id,
                              pi_fact_addr_obj_id      => pi_fact_addr_obj_id,
                              pi_fact_house_obj_id     => pi_fact_house_obj_id);
    select p.person_hash
      into l_person_hash
      from t_person p
     where p.person_id = l_person_id;

    Begin
      select C.CLIENT_ID
        into res
        from T_CLIENTS C
       where C.FULLINFO_ID = l_person_id
         and C.CLIENT_TYPE = c_client_person;
    Exception
      when others then
        if (pi_person_id is null) then
          version_id   := 1;
          l_oldpers_id := l_person_id;
        elsif (not (pi_person_id = l_person_id)) then
          begin
            select MAX(C.VER)
              into version_id
              from T_CLIENTS C
             where C.CLIENT_TYPE = c_client_person
               and C.FID_1VER =
                   (select CCC.FID_1VER
                      from T_CLIENTS CCC
                     where CCC.CLIENT_TYPE = c_client_person
                       and CCC.FULLINFO_ID = pi_person_id);
            version_id := version_id + 1;

            select C.FID_1VER
              into l_oldpers_id
              from T_CLIENTS C
             where pi_person_id = C.FULLINFO_ID
               and C.CLIENT_TYPE = c_client_person;
          exception
            when others then
              version_id   := 1;
              l_oldpers_id := pi_person_id;
          end;
        else
          l_oldpers_id := l_person_id;
        end if;

        insert into T_CLIENTS
          (CLIENT_TYPE,
           FULLINFO_ID,
           VER,
           HASH,
           FID_1VER,
           IS_RESIDENT,
           REG_DEADLINE,
           hash_version)
        values
          (c_client_person,
           l_person_id,
           version_id,
           l_person_hash,
           l_oldpers_id,
           nvl(pi_IS_RESIDENT, 1),
           pi_REG_DEADLINE,
           l_hash_version)
        returning CLIENT_ID into res;
    end;
    return res;
  exception
    when others then
      rollback to save_point;
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg,
                        'users.Add_Physic_Client');
      return - 1;
  end Add_Physic_Client;
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
                               pi_jur_billing_group_shpd     in t_juristic.jur_billing_group_shpd%type,
                               pi_jur_funding_source         in t_juristic.jur_funding_source%type,
                               pi_jur_category               in t_juristic.jur_category%type,
                               pi_jur_person_manager         in t_juristic.jur_person_manager%type,
                               Pi_cat_id                     in t_juristic.jur_category%type,
                               pi_jur_accept_algorithm       in t_juristic.jur_accept_algorithm%type,
                               Pi_jur_without_accept         in t_juristic.jur_without_accept%type,
                               pi_corr_akk                   in T_JURISTIC.JUR_CORR_AKK%type,
                               pi_jur_id                     in T_JURISTIC.JURISTIC_ID%type,
                               pi_sgs_id                     in T_PERSON.SGS_ID%type,
                               pi_worker_id                  in T_USERS.USR_ID%type,
                               pi_jur_dlv_phone              in t_juristic.jur_dlv_phone%type,
                               pi_jur_dlv_email              in t_juristic.jur_dlv_email%type,
                               pi_jur_dlv_fax                in t_juristic.jur_dlv_fax%type,
                               pi_jur_dlv_telex              in t_juristic.jur_dlv_telex%type,
                               pi_jur_dlv_acc_type           in t_juristic.jur_dlv_acc_type%type,
                               pi_jur_bank_name              in t_juristic.jur_bank_name%type,
                               pi_jur_bank_department        in t_juristic.jur_bank_department%type,
                               pi_resp_birthplace            in T_PERSON.BIRTHPLACE%type,
                               pi_touch_birthplace           in T_PERSON.BIRTHPLACE%type,
                               pi_boss_birthplace            in T_PERSON.BIRTHPLACE%type,
                               pi_region1                    in t_address.region_id%type,
                               pi_region2                    in t_address.region_id%type,
                               pi_is_resident                in t_clients.is_resident%type,
                               pi_addr_obj_id1               in t_address.addr_obj_id%type,
                               pi_house_obj_id1              in t_address.addr_house_obj_id%type,
                               pi_addr_obj_id2               in t_address.addr_obj_id%type,
                               pi_house_obj_id2              in t_address.addr_house_obj_id%type,
                               po_err_num                    out pls_integer,
                               po_err_msg                    out t_Err_Msg)
    return T_CLIENTS.CLIENT_ID%type is
    addr1_id    number;
    addr2_id    number;
    resp_id     number;
    touch_id    number;
    boss_id     number;
    l_jur_id    T_JURISTIC.JURISTIC_ID%type;
    l_jur_hash  t_md5;
    version_id  pls_integer := 1;
    res         T_CLIENTS.CLIENT_ID%type := -1;
    found_one   pls_integer := 0;
    l_row_jur   t_row;
    l_oldjur_id T_JURISTIC.JURISTIC_ID%type;
    l_dov_id    number;
    ex_unique_constraint exception;
    pragma exception_init(ex_unique_constraint, -00001);
    l_hash_vers t_juristic.hash_version%type := 2;
  begin
    savepoint save_point;
    l_row_jur := upper(trim(pi_name)) || ';' || upper(trim(pi_Name_Short)) || ';' ||
                 upper(trim(pi_ogrn)) || ';' || upper(trim(pi_ogrn_Date)) || ';' ||
                 upper(trim(pi_ogrn_Place)) || ';' || upper(trim(pi_email)) || ';' ||
                 upper(trim(pi_settl_acc)) || ';' || upper(trim(pi_bik)) || ';' ||
                 upper(trim(pi_inn)) || ';' || upper(trim(pi_kpp)) || ';' ||
                 upper(trim(pi_okpo)) || ';' || upper(trim(pi_okved)) || ';' ||
                 upper(trim(pi_corr_akk)) || ';' ||
                 upper(trim(pi_threasury_name)) || ';' ||
                 upper(trim(pi_threasury_acc)) || ';' ||
                 upper(nvl(trim(pi_country), 'Россия')) || ';' ||
                 upper(trim(pi_post_index)) || ';' || upper(trim(pi_city)) || ';' ||
                 upper(trim(pi_street)) || ';' || upper(trim(pi_house)) || ';' ||
                 upper(trim(pi_flat)) || ';' || upper(trim(pi_code_city)) || ';' ||
                 upper(trim(pi_code_street)) || ';' ||
                 upper(nvl(trim(pi_country2), 'Россия')) || ';' ||
                 upper(trim(pi_post_index2)) || ';' ||
                 upper(trim(pi_city2)) || ';' || upper(trim(pi_street2)) || ';' ||
                 upper(trim(pi_house2)) || ';' || upper(trim(pi_flat2)) || ';' ||
                 upper(trim(pi_code_city2)) || ';' ||
                 upper(trim(pi_code_street2)) || ';' ||
                 upper(trim(pi_resp_phone)) || ';' ||
                 upper(trim(pi_resp_email)) || ';' ||
                 upper(trim(pi_resp_last_name)) || ';' ||
                 upper(trim(pi_resp_first_name)) || ';' ||
                 upper(trim(pi_resp_middle_name)) || ';' ||
                 upper(trim(pi_resp_inn)) || ';' ||
                 upper(trim(pi_resp_birth_day)) || ';' ||
                 upper(trim(pi_resp_sex)) || ';' ||
                 upper(trim(pi_resp_passport_ser)) || ';' ||
                 upper(trim(pi_resp_passport_num)) || ';' ||
                 upper(trim(pi_resp_passport_receive_date)) || ';' ||
                 upper(trim(pi_resp_passport_receive_who)) || ';' ||
                 upper(trim(pi_resp_doc_osn)) || ';' ||
                 upper(trim(pi_resp_doc_osn_Date)) || ';' ||
                 upper(trim(pi_resp_doc_osn_number)) || ';' || --
                 upper(trim(c_doc_type_passport)) || ';' ||
                 upper(trim(pi_touch_phone)) || ';' ||
                 upper(trim(pi_touch_email)) || ';' ||
                 upper(trim(pi_touch_last_name)) || ';' ||
                 upper(trim(pi_touch_first_name)) || ';' ||
                 upper(trim(pi_touch_middle_name)) || ';' ||
                 upper(trim(pi_touch_sex)) || ';' ||
                 upper(trim(pi_touch_birth_day)) || ';' ||
                 upper(trim(pi_touch_inn)) || ';' ||
                 upper(trim(c_doc_type_passport)) || ';' ||
                 upper(trim(pi_boss_phone)) || ';' ||
                 upper(trim(pi_boss_email)) || ';' ||
                 upper(trim(pi_boss_last_name)) || ';' ||
                 upper(trim(pi_boss_first_name)) || ';' ||
                 upper(trim(pi_boss_middle_name)) || ';' || '' || ';' || '' || ';' ||
                 upper(trim(pi_boss_sex)) || ';' ||
                 upper(trim(pi_boss_birth_day)) || ';' ||
                 upper(trim(pi_boss_inn)) || ';' ||
                 upper(trim(c_doc_type_passport)) || ';' ||
                 upper(trim(Pi_jur_class)) || ';' ||
                 upper(trim(pi_jur_billing_group)) || ';' ||
                 upper(trim(pi_jur_billing_group_shpd)) || ';' ||
                 upper(trim(pi_jur_funding_source)) || ';' ||
                 upper(trim(pi_jur_category)) || ';' ||
                 upper(trim(pi_jur_person_manager)) || ';' ||
                 upper(trim(2)) || ';' ||
                 upper(trim(pi_jur_accept_algorithm)) || ';' ||
                 upper(trim(Pi_jur_without_accept)) || ';' ||
                 upper(trim(c_client_juristic)) || ';' ||
                 upper(trim(pi_jur_dlv_phone)) || ';' ||
                 upper(trim(pi_jur_dlv_email)) || ';' ||
                 upper(trim(pi_jur_dlv_fax)) || ';' ||
                 upper(trim(pi_jur_dlv_telex)) || ';' ||
                 upper(trim(pi_jur_dlv_acc_type)) || ';' ||
                 upper(trim(pi_jur_bank_name)) || ';' ||
                 upper(trim(pi_jur_bank_department)) || ';' ||
                 upper(trim(pi_resp_birthplace)) || ';' ||
                 upper(trim(pi_touch_birthplace)) || ';' ||
                 upper(trim(pi_boss_birthplace)) || ';' ||
                 upper(trim(pi_region1)) || ';' || upper(trim(pi_region2)) || ';' ||
                 trim(to_char(pi_addr_obj_id1)) || ';' ||
                 trim(to_char(pi_house_obj_id1)) || ';' ||
                 trim(to_char(pi_addr_obj_id2)) || ';' ||
                 trim(to_char(pi_house_obj_id2));
  
    if (l_row_jur is not null) then
      l_jur_hash := Compute_MD5Hash(l_row_jur);
    
      begin
        select J.JURISTIC_ID
          into l_jur_id
          from T_JURISTIC J
         where j.jur_hash is not null
           and l_jur_hash = j.jur_hash
           and j.hash_version = l_hash_vers;
        found_one := 1;
      exception
        when others then
          -- адрес регистрации
          addr1_id := Ins_Address(pi_country        => pi_country,
                                  pi_city           => pi_city,
                                  pi_index          => pi_post_index,
                                  pi_street         => pi_street,
                                  pi_building       => pi_house,
                                  pi_office         => pi_flat,
                                  pi_corp           => pi_corp,
                                  pi_city_code      => pi_code_city,
                                  pi_street_code    => pi_code_street,
                                  pi_region         => pi_region1,
                                  pi_addr_block     => null,
                                  pi_addr_structure => null,
                                  pi_addr_fraction  => null,
                                  pi_city_lvl       => null,
                                  pi_street_lvl     => null,
                                  pi_house_code     => null,
                                  pi_addr_obj_id    => pi_addr_obj_id1,
                                  pi_house_obj_id   => pi_house_obj_id1);
          -- адрес фактический
          addr2_id := Ins_Address(pi_country        => pi_country2,
                                  pi_city           => pi_city2,
                                  pi_index          => pi_post_index2,
                                  pi_street         => pi_street2,
                                  pi_building       => pi_house2,
                                  pi_office         => pi_flat2,
                                  pi_corp           => pi_corp2,
                                  pi_city_code      => pi_code_city2,
                                  pi_street_code    => pi_code_street2,
                                  pi_region         => pi_region2,
                                  pi_addr_block     => null,
                                  pi_addr_structure => null,
                                  pi_addr_fraction  => null,
                                  pi_city_lvl       => null,
                                  pi_street_lvl     => null,
                                  pi_house_code     => null,
                                  pi_addr_obj_id    => pi_addr_obj_id2,
                                  pi_house_obj_id   => pi_house_obj_id2);
        
          -- ответственное лицо
          resp_id := Ins_Person(pi_first_name            => pi_resp_first_name,
                                pi_middle_name           => pi_resp_middle_name,
                                pi_last_name             => pi_resp_last_name,
                                pi_sex                   => pi_resp_sex,
                                pi_birth_day             => pi_resp_birth_day,
                                pi_inn                   => pi_resp_inn,
                                pi_email                 => pi_resp_email,
                                pi_phone                 => pi_resp_phone,
                                pi_reg_country           => '',
                                pi_reg_city              => '',
                                pi_reg_post_index        => '',
                                pi_reg_street            => '',
                                pi_reg_house             => '',
                                pi_reg_corp              => '',
                                pi_reg_flat              => '',
                                pi_reg_code_city         => '',
                                pi_reg_code_street       => '',
                                pi_fact_country          => '',
                                pi_fact_city             => '',
                                pi_fact_post_index       => '',
                                pi_fact_street           => '',
                                pi_fact_house            => '',
                                pi_fact_corp             => '',
                                pi_fact_flat             => '',
                                pi_fact_code_city        => '',
                                pi_fact_code_street      => '',
                                pi_passport_ser          => pi_resp_passport_ser,
                                pi_passport_num          => pi_resp_passport_num,
                                pi_passport_receive_date => pi_resp_passport_receive_date,
                                pi_passport_receive_who  => pi_resp_passport_receive_who,
                                pi_sgs_id                => '',
                                pi_birthplace            => pi_resp_birthplace,
                                pi_reg_region            => null,
                                pi_fact_region           => null,
                                pi_dlv_acc_type          => null,
                                pi_HOME_PHONE            => null,
                                pi_reg_city_lvl          => null,
                                pi_reg_street_lvl        => null,
                                pi_reg_house_code        => null,
                                pi_fact_city_lvl         => null,
                                pi_fact_street_lvl       => null,
                                pi_fact_house_code       => null,
                                pi_passport_type         => c_doc_type_passport,
                                pi_reg_addr_obj_id       => null,
                                pi_reg_house_obj_id      => null,
                                pi_fact_addr_obj_id      => null,
                                pi_fact_house_obj_id     => null);
        
          if pi_resp_doc_osn_number is not null then
            l_dov_id := Ins_Document(null,
                                     pi_resp_doc_osn_number,
                                     pi_resp_doc_osn_Date,
                                     null,
                                     pi_resp_doc_osn);
            update t_person p
               set p.dov_id = l_dov_id
             where p.person_id = resp_id;
          end if;
          -- контактное лицо
          touch_id := Ins_Person(pi_first_name            => pi_touch_first_name,
                                 pi_middle_name           => pi_touch_middle_name,
                                 pi_last_name             => pi_touch_last_name,
                                 pi_sex                   => pi_touch_sex,
                                 pi_birth_day             => pi_touch_birth_day,
                                 pi_inn                   => pi_touch_inn,
                                 pi_email                 => pi_touch_email,
                                 pi_phone                 => pi_touch_phone,
                                 pi_reg_country           => '',
                                 pi_reg_city              => '',
                                 pi_reg_post_index        => '',
                                 pi_reg_street            => '',
                                 pi_reg_house             => '',
                                 pi_reg_corp              => '',
                                 pi_reg_flat              => '',
                                 pi_reg_code_city         => '',
                                 pi_reg_code_street       => '',
                                 pi_fact_country          => '',
                                 pi_fact_city             => '',
                                 pi_fact_post_index       => '',
                                 pi_fact_street           => '',
                                 pi_fact_house            => '',
                                 pi_fact_corp             => '',
                                 pi_fact_flat             => '',
                                 pi_fact_code_city        => '',
                                 pi_fact_code_street      => '',
                                 pi_passport_ser          => '',
                                 pi_passport_num          => '',
                                 pi_passport_receive_date => '',
                                 pi_passport_receive_who  => '',
                                 pi_sgs_id                => '',
                                 pi_birthplace            => pi_touch_birthplace,
                                 pi_reg_region            => null,
                                 pi_fact_region           => null,
                                 pi_dlv_acc_type          => null,
                                 pi_HOME_PHONE            => null,
                                 pi_reg_city_lvl          => null,
                                 pi_reg_street_lvl        => null,
                                 pi_reg_house_code        => null,
                                 pi_fact_city_lvl         => null,
                                 pi_fact_street_lvl       => null,
                                 pi_fact_house_code       => null,
                                 pi_passport_type         => null,
                                 pi_reg_addr_obj_id       => null,
                                 pi_reg_house_obj_id      => null,
                                 pi_fact_addr_obj_id      => null,
                                 pi_fact_house_obj_id     => null);
        
          -- руководитель
          boss_id := Ins_Person(pi_first_name            => pi_boss_first_name,
                                pi_middle_name           => pi_boss_middle_name,
                                pi_last_name             => pi_boss_last_name,
                                pi_sex                   => pi_boss_sex,
                                pi_birth_day             => pi_touch_birth_day,
                                pi_inn                   => pi_touch_inn,
                                pi_email                 => pi_boss_email,
                                pi_phone                 => pi_boss_phone,
                                pi_reg_country           => '',
                                pi_reg_city              => '',
                                pi_reg_post_index        => '',
                                pi_reg_street            => '',
                                pi_reg_house             => '',
                                pi_reg_corp              => '',
                                pi_reg_flat              => '',
                                pi_reg_code_city         => '',
                                pi_reg_code_street       => '',
                                pi_fact_country          => '',
                                pi_fact_city             => '',
                                pi_fact_post_index       => '',
                                pi_fact_street           => '',
                                pi_fact_house            => '',
                                pi_fact_corp             => '',
                                pi_fact_flat             => '',
                                pi_fact_code_city        => '',
                                pi_fact_code_street      => '',
                                pi_passport_ser          => '',
                                pi_passport_num          => '',
                                pi_passport_receive_date => '',
                                pi_passport_receive_who  => '',
                                pi_sgs_id                => '',
                                pi_birthplace            => pi_boss_birthplace,
                                pi_reg_region            => null,
                                pi_fact_region           => null,
                                pi_dlv_acc_type          => null,
                                pi_HOME_PHONE            => null,
                                pi_reg_city_lvl          => null,
                                pi_reg_street_lvl        => null,
                                pi_reg_house_code        => null,
                                pi_fact_city_lvl         => null,
                                pi_fact_street_lvl       => null,
                                pi_fact_house_code       => null,
                                pi_passport_type         => null,
                                pi_reg_addr_obj_id       => null,
                                pi_reg_house_obj_id      => null,
                                pi_fact_addr_obj_id      => null,
                                pi_fact_house_obj_id     => null);
        
          begin
            insert into T_JURISTIC
              (JUR_NAME,
               JUR_NAME_Short,
               JUR_OGRN,
               JUR_OGRN_PLACE,
               JUR_OGRN_DATE,
               JUR_SETTLEMENT_ACCOUNT,
               JUR_EMAIL,
               JUR_ADDRESS_ID,
               JUR_FACT_ADDRESS_ID,
               JUR_BANK_BIK,
               JUR_INN,
               JUR_KPP,
               JUR_OKPO,
               JUR_OKVED,
               JUR_CORR_AKK,
               JUR_THREASURY_NAME,
               JUR_THREASURY_ACC,
               JUR_RESP_ID,
               JUR_TOUCH_ID,
               JUR_BOSS_ID,
               SGS_ID,
               jur_class,
               jur_billing_group,
               jur_funding_source,
               jur_category,
               jur_person_manager,
               jur_cat_id,
               jur_accept_algorithm,
               jur_without_accept,
               jur_dlv_phone,
               jur_dlv_email,
               jur_dlv_fax,
               jur_dlv_telex,
               jur_dlv_acc_type,
               JUR_HASH,
               jur_bank_name,
               jur_bank_department,
               jur_billing_group_shpd,
               hash_version)
            values
              (SUBSTR(pi_name, 0, 512),
               SUBSTR(pi_Name_Short, 0, 64),
               SUBSTR(pi_ogrn, 0, 16),
               SUBSTR(pi_ogrn_Place, 0, 50),
               pi_ogrn_Date,
               SUBSTR(pi_settl_acc, 0, 22),
               SUBSTR(pi_email, 0, 50),
               addr1_id,
               addr2_id,
               SUBSTR(pi_bik, 0, 16),
               SUBSTR(pi_inn, 0, 16),
               SUBSTR(pi_kpp, 0, 16),
               SUBSTR(pi_okpo, 0, 16),
               SUBSTR(pi_okved, 0, 16),
               SUBSTR(pi_corr_akk, 0, 22),
               SUBSTR(pi_threasury_name, 0, 16),
               SUBSTR(pi_threasury_acc, 0, 16),
               resp_id,
               touch_id,
               boss_id,
               pi_sgs_id,
               pi_jur_class,
               pi_jur_billing_group,
               pi_jur_funding_source,
               pi_jur_category,
               pi_jur_person_manager,
               2,
               pi_jur_accept_algorithm,
               pi_jur_without_accept,
               pi_jur_dlv_phone,
               pi_jur_dlv_email,
               pi_jur_dlv_fax,
               pi_jur_dlv_telex,
               pi_jur_dlv_acc_type,
               l_jur_hash,
               pi_jur_bank_name,
               pi_jur_bank_department,
               pi_jur_billing_group_shpd,
               l_hash_vers)
            returning JURISTIC_ID into l_jur_id;
          exception
            when ex_unique_constraint then
              select J.JURISTIC_ID
                into l_jur_id
                from T_JURISTIC J
               where j.jur_hash is not null
                 and l_jur_hash = j.jur_hash
                 and j.hash_version = l_hash_vers;
            
              found_one := 1;
            
          end;
      end;
    
      if (found_one = 0) then
        if (pi_jur_id is null) then
          version_id  := 1;
          l_oldjur_id := l_jur_id;
        elsif (not (pi_jur_id = l_jur_id)) then
          begin
            select max(C.VER)
              into version_id
              from T_CLIENTS C
             where C.CLIENT_TYPE = c_client_juristic
               and C.FID_1VER =
                   (select CCC.FID_1VER
                      from T_CLIENTS CCC
                     where CCC.CLIENT_TYPE = c_client_juristic
                       and CCC.FULLINFO_ID = pi_jur_id);
            version_id := version_id + 1;
          
            select C.FID_1VER
              into l_oldjur_id
              from T_CLIENTS C
             where pi_jur_id = C.FULLINFO_ID
               and C.CLIENT_TYPE = c_client_juristic;
          exception
            when others then
              version_id  := 1;
              l_oldjur_id := pi_jur_id;
          end;
        else
          l_oldjur_id := l_jur_id;
        end if;
      
        insert into T_CLIENTS
          (CLIENT_TYPE,
           FULLINFO_ID,
           VER,
           HASH,
           FID_1VER,
           is_resident,
           hash_version)
        values
          (c_client_juristic,
           l_jur_id,
           version_id,
           l_jur_hash,
           l_oldjur_id,
           nvl(pi_is_resident, 1),
           l_hash_vers)
        returning CLIENT_ID into res;
      else
        select C.CLIENT_ID
          into res
          from T_CLIENTS C
         where C.FULLINFO_ID = l_jur_id
           and C.CLIENT_TYPE = c_client_juristic;
      end if;
    end if;
    return res;
  exception
    when others then
      rollback to save_point;
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg,
                        'users.Add_Juristic_Client');
      return - 1;
  end Add_Juristic_Client;
  ----------------------------------------------------------------------------
  function Get_Client_By_Id(pi_client_id in T_USERS.USR_ID%type,
                            pi_worker_id in T_USERS.USR_ID%type,
                            po_cl_type   out T_CLIENTS.CLIENT_TYPE%type,
                            po_err_num   out pls_integer,
                            po_err_msg   out t_Err_Msg) return cur_user is
    res         cur_user := null;
    client_type T_CLIENTS.CLIENT_TYPE%type;
  begin
    savepoint sp_Get_Client_By_Id;
    begin
      select C.CLIENT_TYPE
        into client_type
        from T_CLIENTS C
       where C.CLIENT_ID = pi_client_id;
    exception
      when others then
        raise client_not_found_ex;
    end;

    if (client_type = c_client_person) then
      open res for
        select C.CLIENT_ID,
               P.PERSON_ID,
               P.PERSON_LASTNAME,
               P.PERSON_FIRSTNAME,
               P.PERSON_MIDDLENAME,
               P.PERSON_EMAIL,
               P.PERSON_PHONE,
               P.PERSON_INN,
               P.PERSON_BIRTHDAY,
               P.PERSON_SEX,
               D.DOC_ID            doc_id,
               D.DOC_SERIES        doc_series,
               D.DOC_NUMBER        doc_number,
               D.DOC_REGDATE       doc_date,
               D.DOC_EXTRAINFO     doc_info,
               D.DOC_TYPE          doc_type,
               A1.ADDR_ID          adr1_id,
               A1.ADDR_COUNTRY     adr1_country,
               A1.ADDR_INDEX       adr1_index,
               A1.ADDR_CITY        adr1_city,
               A1.ADDR_STREET      adr1_street,
               A1.ADDR_BUILDING    adr1_building,
               A1.ADDR_CORP        adr1_corp,
               A1.ADDR_OFFICE      adr1_office,
               A1.ADDR_CODE_CITY   adr1_code_city,
               A1.ADDR_CODE_STREET adr1_code_street,
               a1.region_id        adr1_region_id,
               P.Birthplace,
               A2.ADDR_ID          adr2_id,
               A2.ADDR_COUNTRY     adr2_country,
               A2.ADDR_INDEX       adr2_index,
               A2.ADDR_CITY        adr2_city,
               A2.ADDR_STREET      adr2_street,
               A2.ADDR_BUILDING    adr2_building,
               A2.ADDR_CORP        adr2_corp,
               A2.ADDR_OFFICE      adr2_office,
               A2.ADDR_CODE_CITY   adr2_code_city,
               A2.ADDR_CODE_STREET adr2_code_street,
               a2.region_id        adr2_region_id,
               C.CLIENT_TYPE,
               P.SGS_ID,
               0                   BALANCE,
               null                STATUS_PSTN,
               null                STATUS_GSM,
               null                KOL_GSM,
               null                ED_TARIFF_ID,
               -- 55494
               p.dlv_acc_type,
               a1.addr_block     adr1_addr_block,
               a1.addr_structure adr1_addr_structure,
               a1.addr_fraction  adr1_addr_fraction,
               a2.addr_block     adr2_addr_block,
               a2.addr_structure adr2_addr_structure,
               a2.addr_fraction  adr2_addr_fraction
          from T_CLIENTS   C,
               T_PERSON    P,
               T_ADDRESS   A1,
               T_ADDRESS   A2,
               T_DOCUMENTS D
         where C.FULLINFO_ID = P.PERSON_ID
           and C.CLIENT_TYPE = c_client_person
           and C.CLIENT_ID = pi_client_id
           and D.DOC_ID(+) = P.DOC_ID
           and A1.ADDR_ID(+) = P.ADDR_ID_REG
           and A2.ADDR_ID(+) = P.ADDR_ID_FACT;
    elsif (client_type = c_client_juristic) then
      open res for
        select C.CLIENT_ID,
               J.JURISTIC_ID,
               J.JUR_NAME,
               j.jur_name_short,
               J.JUR_OGRN,
               j.jur_ogrn_date,
               j.jur_ogrn_place,
               j.jur_billing_group,
               J.JUR_EMAIL,
               J.JUR_SETTLEMENT_ACCOUNT,
               J.JUR_BANK_BIK,
               J.JUR_INN,
               J.JUR_KPP,
               J.JUR_OKPO,
               J.JUR_OKVED,
               J.JUR_CORR_AKK,
               J.JUR_THREASURY_NAME,
               J.JUR_THREASURY_ACC,
               J.SGS_ID,
               A.ADDR_ID                adr_id,
               A.ADDR_COUNTRY           adr_country,
               A.ADDR_INDEX             adr_index,
               A.ADDR_CITY              adr_city,
               A.ADDR_STREET            adr_street,
               A.ADDR_BUILDING          adr_building,
               A.ADDR_OFFICE            adr_office,
               A.ADDR_CORP              adr1_corp,
               A.ADDR_CODE_CITY         adr_code_city,
               A.ADDR_CODE_STREET       adr_code_street,
               a.region_id              adr_region_id,
               A2.ADDR_ID               adr2_id,
               A2.ADDR_COUNTRY          adr2_country,
               A2.ADDR_INDEX            adr2_index,
               A2.ADDR_CITY             adr2_city,
               A2.ADDR_STREET           adr2_street,
               A2.ADDR_BUILDING         adr2_building,
               A2.ADDR_OFFICE           adr2_office,
               A2.ADDR_CORP             adr2_corp,
               A2.ADDR_CODE_CITY        adr2_code_city,
               A2.ADDR_CODE_STREET      adr2_code_street,
               a2.region_id             adr2_region_id,
               RESP.PERSON_ID           resp_id,
               RESP.PERSON_PHONE        resp_phone,
               RESP.PERSON_EMAIL        resp_email,
               RESP.PERSON_LASTNAME     resp_lastname,
               RESP.PERSON_FIRSTNAME    resp_firstname,
               RESP.PERSON_MIDDLENAME   resp_middlename,
               RESP.PERSON_INN          resp_inn,
               RESP.PERSON_BIRTHDAY     resp_birthday,
               resp.birthplace          resp_birthplace,
               RESP.PERSON_SEX          resp_sex,
               -- 54890
               d4.doc_type              resp_doc_osn,
               d4.doc_regdate           resp_doc_osn_Date,
               d4.doc_number            resp_doc_osn_number, --
               D1.DOC_ID                resp_doc_id,
               D1.DOC_SERIES            resp_doc_series,
               D1.DOC_NUMBER            resp_doc_number,
               D1.DOC_REGDATE           resp_doc_date,
               D1.DOC_EXTRAINFO         resp_doc_info,
               D1.DOC_TYPE              resp_doc_type,
               TOUCH.PERSON_ID          touch_id,
               TOUCH.PERSON_PHONE       touch_phone,
               TOUCH.PERSON_EMAIL       touch_email,
               TOUCH.PERSON_LASTNAME    touch_lastname,
               TOUCH.PERSON_FIRSTNAME   touch_firstname,
               TOUCH.PERSON_MIDDLENAME  touch_middlename,
               TOUCH.PERSON_INN         touch_inn,
               TOUCH.PERSON_BIRTHDAY    touch_birthday,
               TOUCH.BIRTHPLACE         touch_birthplace,
               TOUCH.PERSON_SEX         touch_sex,
               D2.DOC_ID                touch_doc_id,
               D2.DOC_SERIES            touch_doc_series,
               D2.DOC_NUMBER            touch_doc_number,
               D2.DOC_REGDATE           touch_doc_date,
               D2.DOC_EXTRAINFO         touch_doc_info,
               D2.DOC_TYPE              touch_doc_type,
               BOSS.PERSON_ID           boss_id,
               BOSS.PERSON_PHONE        boss_phone,
               BOSS.PERSON_EMAIL        boss_email,
               BOSS.PERSON_LASTNAME     boss_lastname,
               BOSS.PERSON_FIRSTNAME    boss_firstname,
               BOSS.PERSON_MIDDLENAME   boss_middlename,
               BOSS.PERSON_INN          boss_inn,
               BOSS.PERSON_BIRTHDAY     boss_birthday,
               boss.birthplace          boss_birthplace,
               BOSS.PERSON_SEX          boss_sex,
               D3.DOC_ID                boss_doc_id,
               D3.DOC_SERIES            boss_doc_series,
               D3.DOC_NUMBER            boss_doc_number,
               D3.DOC_REGDATE           boss_doc_date,
               D3.DOC_EXTRAINFO         boss_doc_info,
               D3.DOC_TYPE              boss_doc_type,
               C.CLIENT_TYPE,
               j.jur_class, -- New
               j.jur_billing_group, -- New
               j.jur_billing_group_shpd, --50477
               j.jur_funding_source, -- New
               j.jur_category, -- New
               j.jur_person_manager, -- New
               j.jur_cat_id, -- New
               j.jur_accept_algorithm, -- New
               j.jur_without_accept, -- New
               J.SGS_ID,
               0 /*PKG_SGS_LINK.GetBallancePClientFromSGS(l_ser,l_num)*/                        BALANCE, -- alla
               j.jur_dlv_phone,
               j.jur_dlv_email,
               j.jur_dlv_fax,
               j.jur_dlv_telex,
               j.jur_dlv_acc_type,
               j.jur_bank_name,
               j.jur_bank_department,
               a.addr_block             adr_addr_block,
               a.addr_structure         adr_addr_structure,
               a.addr_fraction          adr_addr_fraction,
               a2.addr_block            adr2_addr_block,
               a2.addr_structure        adr2_addr_structure,
               a2.addr_fraction         adr2_addr_fraction
          from T_CLIENTS   C,
               T_JURISTIC  J,
               T_ADDRESS   A,
               T_ADDRESS   A2,
               T_PERSON    RESP,
               T_PERSON    TOUCH,
               T_PERSON    BOSS,
               T_DOCUMENTS D1,
               T_DOCUMENTS D2,
               T_DOCUMENTS D3,
               T_DOCUMENTS D4
         where C.CLIENT_ID = pi_client_id
           and C.FULLINFO_ID = J.JURISTIC_ID
           and C.CLIENT_TYPE = c_client_juristic
           and A.ADDR_ID(+) = J.JUR_ADDRESS_ID
           and A2.ADDR_ID(+) = J.JUR_FACT_ADDRESS_ID
           and RESP.PERSON_ID(+) = J.JUR_RESP_ID
           and TOUCH.PERSON_ID(+) = J.JUR_TOUCH_ID
           and BOSS.PERSON_ID(+) = J.JUR_BOSS_ID
           and D1.DOC_ID(+) = RESP.DOC_ID
           and D2.DOC_ID(+) = TOUCH.DOC_ID
           and D3.DOC_ID(+) = BOSS.DOC_ID
           and d4.doc_id(+) = resp.dov_id;
    end if;
    po_cl_type := client_type;
    return res;
  exception
    when client_not_found_ex then
      ROLLBACK to sp_Get_Client_By_Id;
      po_cl_type := ' ';
      po_err_num := 1001;
      po_err_msg := 'Клиент не найден (clientId= ' || pi_client_id || ').';
      return null;
    when others then
      ROLLBACK to sp_Get_Client_By_Id;
      po_cl_type := ' ';
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Client_By_Id;
  ----------------------------------------------------------------------------
  function Get_User_List_By_Org_Id(pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                                   pi_worker_id in T_USERS.USR_ID%type,
                                   po_err_num   out pls_integer,
                                   po_err_msg   out t_Err_Msg)
    return sys_refcursor is
    res sys_refcursor;
  begin
    if (not Security_pkg.Check_Rights_str((case
                                            when (is_org_usi(pi_org_id) > 0 or is_user_usi(pi_worker_id) > 0) then
                                             'EISSD.WORKER_USI.LIST'
                                            else
                                             'EISSD.WORKER.LIST'
                                          end),
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      return null;
    end if;
    open res for
      select distinct U.USR_ID,
                      USR_LOGIN,
                      PERSON_LASTNAME,
                      PERSON_FIRSTNAME,
                      PERSON_MIDDLENAME,
                      PERSON_EMAIL,
                      USR_STATUS,
                      group_concat_user_roles(uo.usr_id, UO.ORG_ID) roles,
                      U.IS_ENABLED,
                      u.org_id,
                      u.date_login_to,
                      u.date_pswd_to,
                      u.is_temp_passwd
        from T_USERS U, T_USER_ORG UO, T_PERSON P
       where UO.ORG_ID = pi_org_id
         and U.USR_ID = UO.USR_ID
         and USR_PERSON_ID = PERSON_ID
         and not UO.ROLE_ID = 0
       order by P.PERSON_LASTNAME asc, P.PERSON_FIRSTNAME asc;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_User_List_By_Org_Id;
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
    return sys_refcursor is
    res       sys_refcursor;
    l_org_tab num_Tab;
  begin
    If not Security_pkg.Check_User_Right_str('EISSD.WORKER.SEARCH',
                                             pi_worker_id,
                                             po_err_num,
                                             po_err_msg) -- Поиск сотрудника
     then
      return null;
    Else
      po_err_num := Null;
      po_err_msg := Null;
    End If;
    l_org_tab := get_user_orgs_tab(pi_worker_id);
    open res for
      select distinct *
        from (select U.USR_ID,
                     U.USR_LOGIN,
                     U.USR_PWD_MD5,
                     P.PERSON_FIRSTNAME,
                     P.PERSON_MIDDLENAME,
                     P.PERSON_LASTNAME,
                     P.PERSON_EMAIL,
                     U.USR_STATUS,
                     U.IS_ENABLED,
                     O.ORG_ID,
                     O.ORG_NAME,
                     u.date_login_to,
                     u.date_pswd_to,
                     u.is_temp_passwd
                from T_USERS U
                join T_PERSON P
                  on U.USR_PERSON_ID = P.PERSON_ID
                join T_USER_ORG UO
                  on U.USR_ID = UO.USR_ID
                 and not UO.ROLE_ID = 0
                 and UO.ORG_ID in (select * from TABLE(l_org_tab))
                join T_ORGANIZATIONS O
                  on O.ORG_ID = UO.ORG_ID
               where ((not (pi_lname is null)) or (not (pi_fname is null)) or
                     (not (pi_mname is null)) or (not (pi_id is null)) or
                     (not (pi_login is null)))
                 and ( -- сходство д.б. во всех полях
                      ((pi_all = 1) and
                      ((pi_lname is null) or
                      (lower(P.PERSON_LASTNAME) like lower(pi_lname) || '%')) and
                      ((pi_fname is null) or (lower(P.PERSON_FIRSTNAME) like
                      lower(pi_fname) || '%')) and
                      ((pi_mname is null) or (lower(P.PERSON_MIDDLENAME) like
                      lower(pi_mname) || '%')) and
                      ((pi_id is null) or (pi_id = U.USR_ID)) and
                      ((pi_login is null) or
                      (lower(U.USR_LOGIN) like lower(pi_login) || '%'))) or
                     --сходство д.б. хотя бы по 1 полю
                      ((pi_all = 0) and
                      (((lower(P.PERSON_LASTNAME) like
                      lower(pi_lname) || '%') and not pi_lname is null) or
                      ((lower(P.PERSON_FIRSTNAME) like
                      lower(pi_fname) || '%') and not pi_fname is null) or
                      ((lower(P.PERSON_MIDDLENAME) like
                      lower(pi_mname) || '%') and not pi_mname is null) or
                      (pi_id = U.USR_ID) or
                      ((lower(U.USR_LOGIN) like lower(pi_login) || '%') and
                      not pi_login is null))))
                 and (pi_ignore_blocked_user = 0 or
                     (pi_ignore_blocked_user = 1 and u.is_enabled = 1)))
       order by PERSON_LASTNAME asc, PERSON_FIRSTNAME asc;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  ----------------------------------------------------------------------------
  -- 59657 Добавлен параметр pi_ignore_blocked_org
  ----------------------------------------------------------------------------
  function Get_User_List(pi_org_id              in T_ORGANIZATIONS.ORG_ID%type,
                         pi_role_id             in t_roles.role_id%type,
                         pi_ignore_blocked_user in pls_integer,
                         pi_worker_id           in T_USERS.USR_ID%type,
                         po_err_num             out pls_integer,
                         po_err_msg             out varchar2)
    return sys_refcursor is
    res       sys_refcursor;
    l_org_tab num_Tab;
  begin
    If not Security_pkg.Check_User_Right_str((case
                                               when is_user_usi(pi_worker_id) = 0 then
                                                'EISSD.WORKER.LIST'
                                               when is_user_usi(pi_worker_id) = 1 and is_org_usi(pi_org_id) = 0 then
                                                'EISSD.WORKER.LIST'
                                               else
                                                'EISSD.WORKER_USI.LIST'
                                             end),
                                             pi_worker_id,
                                             po_err_num,
                                             po_err_msg) -- Список сотрудников
     then
      return null;
    Else
      po_err_num := Null;
      po_err_msg := Null;
    End If;
    l_org_tab := get_user_orgs_tab(pi_worker_id);
    open res for
      select /*distinct 32301*/
       U.USR_ID,
       U.USR_LOGIN,
       U.USR_PWD_MD5,
       P.PERSON_FIRSTNAME,
       P.PERSON_MIDDLENAME,
       P.PERSON_LASTNAME,
       P.PERSON_EMAIL,
       U.USR_STATUS,
       USERS.group_concat_user_roles(UO.USR_ID, pi_org_id) roles,
       U.IS_ENABLED,
       O.ORG_ID,
       O.ORG_NAME,
       to_clob(listagg(T.ROLE_NAME, ',') within group(order by T.ROLE_NAME)) ROLE_NAME,
       u.date_login_to,
       u.date_pswd_to,
       u.is_temp_passwd
        from T_USERS U
        join T_PERSON P
          on U.USR_PERSON_ID = P.PERSON_ID
        join T_USER_ORG UO
          on U.USR_ID = UO.USR_ID
         and not UO.ROLE_ID = 0
         and UO.ORG_ID in (select * from TABLE(l_org_tab))
        join T_ORGANIZATIONS O
          on O.ORG_ID = UO.ORG_ID
        join T_ROLES T
          on T.ROLE_ID = UO.ROLE_ID
       where (pi_ignore_blocked_user = 0 or
             (pi_ignore_blocked_user = 1 and u.is_enabled = 1))
         and pi_org_id = uo.org_id
         and (pi_role_id is null or uo.role_id = pi_role_id)
       GROUP BY U.USR_ID,
                U.USR_LOGIN,
                U.USR_PWD_MD5,
                P.PERSON_FIRSTNAME,
                P.PERSON_MIDDLENAME,
                P.PERSON_LASTNAME,
                P.PERSON_EMAIL,
                U.USR_STATUS,
                U.IS_ENABLED,
                O.ORG_ID,
                UO.USR_ID,
                O.ORG_NAME,
                u.date_login_to,
                u.date_pswd_to,
                u.is_temp_passwd
       order by P.PERSON_LASTNAME asc, P.PERSON_FIRSTNAME asc;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;

  ----------------------------------------------------------------------------
  -- Постраничный поиск сотрудника. Перевызов.
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
                                 po_all_count           out number,
                                 po_err_num             out pls_integer,
                                 po_err_msg             out t_Err_Msg)
    return sys_refcursor is
  begin
    return Get_User_List_By_page(pi_lname               => pi_lname,
                                 pi_fname               => pi_fname,
                                 pi_mname               => pi_mname,
                                 pi_id                  => pi_id,
                                 pi_login               => pi_login,
                                 pi_login_tab           => pi_login_tab,
                                 pi_org_id              => pi_org_id,
                                 pi_curated_include     => pi_curated_include,
                                 pi_ignore_blocked_user => pi_ignore_blocked_user,
                                 pi_worker_id           => pi_worker_id,
                                 pi_order_num           => pi_order_num,
                                 pi_page_num            => pi_page_num,
                                 pi_req_count           => pi_req_count,
                                 pi_sort                => pi_sort,
                                 pi_column              => pi_column,
                                 po_all_count           => po_all_count,
                                 pi_names               => null,
                                 po_err_num             => po_err_num,
                                 po_err_msg             => po_err_msg);
  end;

  ----------------------------------------------------------------------------
  -- Постраничный поиск сотрудника
  ----------------------------------------------------------------------------
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
                                 pi_names               in STRING_TRIPLE_TAB, -- Список ФИО
                                 po_all_count           out number,
                                 po_err_num             out pls_integer,
                                 po_err_msg             out t_Err_Msg)
    return sys_refcursor is
    res            sys_refcursor;
    l_org_tab      num_Tab;
    User_Orgs      num_tab;
    l_user_tab     request_Order_Tab;
    l_max_num_page number;
    l_num_page     number;
    l_order_asc    number;
    l_order_desc   number;
    l_max_usr_id   number;
    l_login        t_users.usr_login%type;
    l_names_search number := 1;

    -----------------
    FUNCTION GET_STR_BY_STRING_TRIPLE_TAB(pi_tab in string_triple_tab)
      RETURN VARCHAR2 IS
      res varchar2(2000);
    BEGIN
      select substr(listagg('string_triple( ''' || str1 || ''',''' || str2 ||
                            ''',''' || str3 || ''' )',
                            ', ') within group(order by str1),
                    1,
                    3000)
        into res
        from table(pi_tab);

      return res;
    EXCEPTION
      WHEN OTHERS THEN
        RETURN '...TOO LONG STRING... [count: ' || pi_tab.count || ']';
    end;
    -------------------

  begin
    logging_pkg.debug('pi_lname := ' || pi_lname || ' (' ||
                      length(pi_lname) || ')' || chr(10) || 'pi_fname := ' ||
                      pi_fname || ' (' || length(pi_fname) || ')' ||
                      chr(10) || 'pi_mname := ' || pi_mname || ' (' ||
                      length(pi_mname) || ')' || chr(10) || 'pi_id := ' ||
                      pi_id || chr(10) || 'pi_login := ' || pi_login || ' (' ||
                      length(pi_login) || ')' || chr(10) ||
                      'pi_login_tab := ' ||
                      get_str_by_string_tab(pi_login_tab) || chr(10) ||
                      'pi_org_id := ' || pi_org_id || chr(10) ||
                      'pi_curated_include := ' || pi_curated_include ||
                      chr(10) || 'pi_ignore_blocked_user := ' ||
                      pi_ignore_blocked_user || chr(10) ||
                      'pi_worker_id := ' || pi_worker_id || chr(10) ||
                      'pi_ORDER_NUM := ' || pi_ORDER_NUM || ' (' ||
                      length(pi_ORDER_NUM) || ')' || chr(10) ||
                      'pi_page_num := ' || pi_page_num || chr(10) ||
                      'pi_req_count := ' || pi_req_count || chr(10) ||
                      'pi_sort := ' || pi_sort || chr(10) ||
                      'pi_column := ' || pi_column || chr(10) ||
                      'pi_names := ' ||
                      GET_STR_BY_STRING_TRIPLE_TAB(pi_names),
                      'users.Get_User_List_By_page');

    select length(max(u.usr_id)) + 1 into l_max_usr_id from t_users u;
    if pi_login is not null then
      l_login := replace(pi_login, '\', '\\');
      l_login := replace(l_login, '%', '\%');
      l_login := replace(l_login, '_', '\_');
    end if;

    If (not (Security_pkg.Check_User_Right_str('EISSD.WORKER.SEARCH',
                                               pi_worker_id,
                                               po_err_num,
                                               po_err_msg) -- Поиск сотрудника
        or Security_pkg.Check_User_Right_str((case
                                                    when is_user_usi(pi_worker_id) = 0 then
                                                     'EISSD.WORKER.LIST'
                                                    else
                                                     'EISSD.WORKER_USI.LIST'
                                                  end),
                                                  pi_worker_id,
                                                  po_err_num,
                                                  po_err_msg) -- Список сотрудников
        or Security_pkg.Check_User_Right_str('EISSD.WORKER.MANAGEMENT', --Персонал: управление учетными записями пользователей
                                                  pi_worker_id,
                                                  po_err_num,
                                                  po_err_msg))) then
      return null;
    Else
      po_err_num := Null;
      po_err_msg := Null;
    End If;
    l_org_tab := get_user_orgs_tab(pi_worker_id => pi_worker_id,pi_ignore_blocked_org => 0);

    if pi_org_id is null then
      select column_value bulk collect
        into User_Orgs
        from table(l_org_tab);
    elsif nvl(pi_curated_include, 0) = 0 then
      select column_value bulk collect
        into User_Orgs
        from (select column_value
                from table(num_tab(nvl(pi_org_id, 0)))
              intersect
              select column_value from table(l_org_tab));
    else
      select org_id bulk collect
        into User_Orgs
        from ((select tor.org_id
                 from t_org_relations tor
               connect by prior tor.org_id = tor.org_pid
                start with tor.org_id = nvl(pi_org_id, 0)
               union
               select column_value
                 from table(num_tab(pi_org_id))
                where column_value = 0) intersect
               select column_value from table(l_org_tab));

    end if;

    If pi_sort = 1 then
      l_order_asc := NVL(pi_column, 1);
    else
      l_order_desc := NVL(pi_column, 1);
    end If;

    if pi_names is null or pi_names.count = 0 then
      l_names_search := 0;
    end if;

    select request_Order_Type(USR_ID, rownum) bulk collect
      into l_user_tab
      from (select distinct U.USR_ID,
                            U.USR_LOGIN,
                            U.USR_PWD_MD5,
                            P.PERSON_FIRSTNAME,
                            P.PERSON_MIDDLENAME,
                            P.PERSON_LASTNAME,
                            P.PERSON_EMAIL,
                            p.person_phone,
                            u.boss_email,
                            orgs.get_org_name_tree(nvl(o.org_id, 0)) org_name_tree
              from T_USERS U
              JOIN T_PERSON P
                ON U.USR_PERSON_ID = P.PERSON_ID
              JOIN T_USER_ORG UO
                ON U.USR_ID = UO.USR_ID
              JOIN T_ORGANIZATIONS O
                ON O.ORG_ID = U.ORG_ID

              join t_organizations o_rol
                on o_rol.org_id = uo.org_id
              join T_ROLES T
                on T.ROLE_ID = UO.ROLE_ID

              LEFT JOIN t_users_hist h
                ON h.user_id = u.usr_id

              LEFT JOIN TABLE(pi_names) n
                ON lower(P.PERSON_LASTNAME) IS NOT NULL
               AND lower(P.PERSON_LASTNAME) = lower(n.str1)
               AND lower(P.PERSON_FIRSTNAME) IS NOT NULL
               AND lower(P.PERSON_FIRSTNAME) = lower(n.str2)
               AND (lower(P.PERSON_MIDDLENAME) IS NOT NULL AND
                    lower(P.PERSON_MIDDLENAME) = lower(n.str3) OR
                    n.str3 IS NULL AND P.PERSON_MIDDLENAME IS NULL)

             where ((pi_lname is null) or
                   (lower(P.PERSON_LASTNAME) like lower(pi_lname) || '%'))
               and ((pi_fname is null) or
                   (lower(P.PERSON_FIRSTNAME) like lower(pi_fname) || '%'))
               and ((pi_mname is null) or
                   (lower(P.PERSON_MIDDLENAME) like lower(pi_mname) || '%'))
               and ((pi_id is null) or (pi_id = U.USR_ID))
               and (pi_login is null or
                   (lower(U.USR_LOGIN) like lower(l_login) || '%' escape '\'))
               and (pi_ORDER_NUM is null or
                   lower(h.order_num) like
                   '%' || lower(pi_ORDER_NUM) || '%')
               and (pi_login_tab is null or
                   lower(u.usr_login) in
                   (select lower(column_value) from table(pi_login_tab)))
               and UO.ORG_ID in (select * from TABLE(User_Orgs))
               and not UO.ROLE_ID = 0
               and (pi_ignore_blocked_user = 0 or
                   (pi_ignore_blocked_user = 1 and u.is_enabled = 1))
               and (l_names_search = 0 OR n.str1 is not null)
             order by decode(l_order_asc,
                             null,
                             null,
                             1,
                             lpad(USR_ID, l_max_usr_id, '0'),
                             2,
                             USR_LOGIN,
                             3,
                             PERSON_LASTNAME,
                             4,
                             PERSON_FIRSTNAME,
                             5,
                             PERSON_MIDDLENAME,
                             6,
                             PERSON_EMAIL,
                             7,
                             boss_email,
                             8,
                             person_phone,
                             9,
                             org_name_tree,
                             null) asc,
                      decode(l_order_desc,
                             null,
                             null,
                             1,
                             lpad(USR_ID, l_max_usr_id, '0'),
                             2,
                             USR_LOGIN,
                             3,
                             PERSON_LASTNAME,
                             4,
                             PERSON_FIRSTNAME,
                             5,
                             PERSON_MIDDLENAME,
                             6,
                             PERSON_EMAIL,
                             7,
                             boss_email,
                             8,
                             person_phone,
                             9,
                             org_name_tree,
                             null) desc);
    po_all_count := l_user_tab.count;

    l_max_num_page := round(po_all_count / nvl(pi_req_count, 1));

    if (pi_page_num > l_max_num_page and l_max_num_page <> 0) then
      l_num_page := l_max_num_page + 1;
    else
      l_num_page := nvl(pi_page_num, 1);
    end if;

    open res for
      select U.USR_ID,
             U.USR_LOGIN,
             U.USR_PWD_MD5,
             P.PERSON_FIRSTNAME,
             P.PERSON_MIDDLENAME,
             P.PERSON_LASTNAME,
             P.PERSON_EMAIL,
             U.USR_STATUS,
             USERS.group_concat_user_roles(UO.USR_ID, pi_org_id) roles,
             U.IS_ENABLED,
             O.ORG_ID,
             O.ORG_NAME,
             max(orgs.get_org_name_tree(nvl(o.org_id, 0))) org_name_tree,
             cast(collect(T.ROLE_NAME || ' (' || o_rol.org_name || ')') as string_tab) ROLE_NAME,
             u.date_login_to,
             u.date_pswd_to,
             u.is_temp_passwd,
             p.person_phone,
             u.boss_email,
             u.employee_number
        from (Select request_id, rn
                from table(l_user_tab)
               where rn between (l_num_page - 1) * pi_req_count + 1 and
                     (l_num_page) * pi_req_count) tmp
        join T_USERS U
          on u.usr_id = tmp.request_id
        join T_PERSON P
          on U.USR_PERSON_ID = P.PERSON_ID
        join T_USER_ORG UO
          on U.USR_ID = UO.USR_ID
         and not UO.ROLE_ID = 0
        join T_ORGANIZATIONS O
          on O.ORG_ID = U.ORG_ID
        join t_organizations o_rol
          on o_rol.org_id = uo.org_id
        join T_ROLES T
          on T.ROLE_ID = UO.ROLE_ID
       GROUP BY U.USR_ID,
                U.USR_LOGIN,
                U.USR_PWD_MD5,
                P.PERSON_FIRSTNAME,
                P.PERSON_MIDDLENAME,
                P.PERSON_LASTNAME,
                P.PERSON_EMAIL,
                U.USR_STATUS,
                U.IS_ENABLED,
                O.ORG_ID,
                UO.USR_ID,
                O.ORG_NAME,
                u.date_login_to,
                u.date_pswd_to,
                u.is_temp_passwd,
                p.person_phone,
                u.boss_email,
                u.employee_number,
                tmp.rn
       order by tmp.rn;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;

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
                           po_err_msg   out t_Err_Msg) return cur_user is
    res cur_user;
  begin
    if (not Security_pkg.Check_Rights_Orgs_str('EISSD.ABONENT.SEARCH',
                                               Get_User_Orgs_Tab(pi_worker_id),
                                               pi_worker_id,
                                               po_err_num,
                                               po_err_msg)) then
      return null;
    end if;
    if (pi_cl_type = c_client_person) then
      open res for
        select C.CLIENT_ID,
               P.PERSON_ID,
               P.PERSON_LASTNAME,
               P.PERSON_FIRSTNAME,
               P.PERSON_MIDDLENAME,
               P.PERSON_EMAIL,
               P.PERSON_PHONE,
               P.PERSON_INN,
               P.PERSON_BIRTHDAY,
               p.birthplace,
               P.PERSON_SEX,
               D.DOC_ID            doc_id,
               D.DOC_SERIES        doc_series,
               D.DOC_NUMBER        doc_number,
               D.DOC_REGDATE       doc_date,
               D.DOC_EXTRAINFO     doc_info,
               D.DOC_TYPE          doc_type,
               A1.ADDR_ID          adr1_id,
               A1.ADDR_COUNTRY     adr1_country,
               A1.ADDR_INDEX       adr1_index,
               A1.ADDR_CITY        adr1_city,
               A1.ADDR_STREET      adr1_street,
               A1.ADDR_BUILDING    adr1_building,
               A1.ADDR_OFFICE      adr1_office,
               A1.ADDR_CORP        adr1_corp,
               A1.ADDR_CODE_CITY   adr1_code_city,
               A1.ADDR_CODE_STREET adr1_code_street,
               a1.region_id        adr1_region_id,
               A2.ADDR_ID          adr2_id,
               A2.ADDR_COUNTRY     adr2_country,
               A2.ADDR_INDEX       adr2_index,
               A2.ADDR_CITY        adr2_city,
               A2.ADDR_STREET      adr2_street,
               A2.ADDR_BUILDING    adr2_building,
               A2.ADDR_OFFICE      adr2_office,
               A2.ADDR_CORP        adr2_corp,
               A2.ADDR_CODE_CITY   adr2_code_city,
               A2.ADDR_CODE_STREET adr2_code_street,
               a2.region_id        adr2_region_id,
               C.CLIENT_TYPE,
               P.SGS_ID,
               0 /*PKG_SGS_LINK.GetBallancePClientFromSGS(l_ser,l_num)*/                   BALANCE, -- alla
               null                STATUS_PSTN,
               null                STATUS_GSM,
               null                KOL_GSM,
               null                ED_TARIFF_ID,
               a1.addr_block       adr1_addr_block,
               a1.addr_structure   adr1_addr_structure,
               a1.addr_fraction    adr1_addr_fraction,
               a2.addr_block       adr2_addr_block,
               a2.addr_structure   adr2_addr_structure,
               a2.addr_fraction    adr2_addr_fraction
          from T_CLIENTS   C,
               T_PERSON    P,
               T_ADDRESS   A1,
               T_ADDRESS   A2,
               T_DOCUMENTS D
         where C.FULLINFO_ID = P.PERSON_ID(+)
           and C.CLIENT_TYPE = c_client_person
           and D.DOC_TYPE = c_doc_type_passport
           and P.DOC_ID = D.DOC_ID(+)
           and P.ADDR_ID_REG = A1.ADDR_ID(+)
           and P.ADDR_ID_FACT = A2.ADDR_ID(+)
           and ((not (pi_par1 is null)) or (not (pi_par2 is null)) or
               (not (pi_par3 is null)) or (not (pi_par4 is null)) or
               (not (pi_par5 is null)) or (not (pi_par6 is null)) or
               (not (pi_par7 is null)))
           and (((pi_all = 1) and
               ((pi_par1 is null) or (pi_par1 = D.DOC_SERIES)) and
               ((pi_par2 is null) or (pi_par2 = D.DOC_NUMBER)) and
               ((pi_par3 is null) or
               (lower(pi_par3) = lower(P.PERSON_FIRSTNAME))) and
               ((pi_par4 is null) or
               (lower(pi_par4) = lower(P.PERSON_LASTNAME))) and
               ((pi_par5 is null) or
               (lower(pi_par5) = lower(P.PERSON_MIDDLENAME))) and
               ((pi_par6 is null) or
               (lower(pi_par6) = lower(P.PERSON_INN))) and
               ((pi_par7 is null) or
               (lower(pi_par7) = lower(P.PERSON_PHONE)))) or
               ((pi_all = 0) and
               ((pi_par1 = D.DOC_SERIES) or (pi_par2 = D.DOC_NUMBER) or
               (lower(pi_par3) = lower(P.PERSON_FIRSTNAME)) or
               (lower(pi_par4) = lower(P.PERSON_LASTNAME)) or
               (lower(pi_par5) = lower(P.PERSON_MIDDLENAME)) or
               (lower(pi_par6) = lower(P.PERSON_INN)) or
               (lower(pi_par7) = lower(P.PERSON_PHONE)))))
         order by CLIENT_ID asc;
    elsif (pi_cl_type = c_client_juristic) then
      open res for
        select C.CLIENT_ID,
               J.JURISTIC_ID,
               J.JUR_NAME,
               J.JUR_OGRN,
               J.JUR_EMAIL,
               J.JUR_SETTLEMENT_ACCOUNT,
               J.JUR_BANK_BIK,
               J.JUR_INN,
               J.JUR_KPP,
               J.JUR_OKPO,
               J.JUR_OKVED,
               J.JUR_CORR_AKK,
               J.JUR_THREASURY_NAME,
               J.JUR_THREASURY_ACC,
               A.ADDR_ID                adr_id,
               A.ADDR_COUNTRY           adr_country,
               A.ADDR_INDEX             adr_index,
               A.ADDR_CITY              adr_city,
               A.ADDR_STREET            adr_street,
               A.ADDR_BUILDING          adr_building,
               A.ADDR_OFFICE            adr_office,
               A.ADDR_CORP              adr_corp,
               A.ADDR_CODE_CITY         adr_code_city,
               A.ADDR_CODE_STREET       adr_code_street,
               A2.ADDR_ID               adr2_id,
               A2.ADDR_COUNTRY          adr2_country,
               A2.ADDR_INDEX            adr2_index,
               A2.ADDR_CITY             adr2_city,
               A2.ADDR_STREET           adr2_street,
               A2.ADDR_BUILDING         adr2_building,
               A2.ADDR_OFFICE           adr2_office,
               A2.ADDR_CORP             adr2_corp,
               A2.ADDR_CODE_CITY        adr2_code_city,
               A2.ADDR_CODE_STREET      adr2_code_street,
               RESP.PERSON_ID           resp_id,
               RESP.PERSON_PHONE        resp_phone,
               RESP.PERSON_EMAIL        resp_email,
               RESP.PERSON_LASTNAME     resp_lastname,
               RESP.PERSON_FIRSTNAME    resp_firstname,
               RESP.PERSON_MIDDLENAME   resp_middlename,
               RESP.PERSON_INN          resp_inn,
               RESP.PERSON_BIRTHDAY     resp_birthday,
               RESP.PERSON_SEX          resp_sex,
               D1.DOC_ID                resp_doc_id,
               D1.DOC_SERIES            resp_doc_series,
               D1.DOC_NUMBER            resp_doc_number,
               D1.DOC_REGDATE           resp_doc_date,
               D1.DOC_EXTRAINFO         resp_doc_info,
               D1.DOC_TYPE              resp_doc_type,
               TOUCH.PERSON_ID          touch_id,
               TOUCH.PERSON_PHONE       touch_phone,
               TOUCH.PERSON_EMAIL       touch_email,
               TOUCH.PERSON_LASTNAME    touch_lastname,
               TOUCH.PERSON_FIRSTNAME   touch_firstname,
               TOUCH.PERSON_MIDDLENAME  touch_middlename,
               TOUCH.PERSON_INN         touch_inn,
               TOUCH.PERSON_BIRTHDAY    touch_birthday,
               TOUCH.PERSON_SEX         touch_sex,
               D2.DOC_ID                touch_doc_id,
               D2.DOC_SERIES            touch_doc_series,
               D2.DOC_NUMBER            touch_doc_number,
               D2.DOC_REGDATE           touch_doc_date,
               D2.DOC_EXTRAINFO         touch_doc_info,
               D2.DOC_TYPE              touch_doc_type,
               BOSS.PERSON_ID           boss_id,
               BOSS.PERSON_PHONE        boss_phone,
               BOSS.PERSON_EMAIL        boss_email,
               BOSS.PERSON_LASTNAME     boss_lastname,
               BOSS.PERSON_FIRSTNAME    boss_firstname,
               BOSS.PERSON_MIDDLENAME   boss_middlename,
               BOSS.PERSON_INN          boss_inn,
               BOSS.PERSON_BIRTHDAY     boss_birthday,
               BOSS.PERSON_SEX          boss_sex,
               D3.DOC_ID                boss_doc_id,
               D3.DOC_SERIES            boss_doc_series,
               D3.DOC_NUMBER            boss_doc_number,
               D3.DOC_REGDATE           boss_doc_date,
               D3.DOC_EXTRAINFO         boss_doc_info,
               D3.DOC_TYPE              boss_doc_type,
               C.CLIENT_TYPE,
               J.SGS_ID,
               a.addr_block             adr_addr_block,
               a.addr_structure         adr_addr_structure,
               a.addr_fraction          adr_addr_fraction,
               a2.addr_block            adr2_addr_block,
               a2.addr_structure        adr2_addr_structure,
               a2.addr_fraction         adr2_addr_fraction
          from T_CLIENTS   C,
               T_JURISTIC  J,
               T_ADDRESS   A,
               T_ADDRESS   A2,
               T_PERSON    RESP,
               T_PERSON    TOUCH,
               T_PERSON    BOSS,
               T_DOCUMENTS D1,
               T_DOCUMENTS D2,
               T_DOCUMENTS D3
         where C.FULLINFO_ID = J.JURISTIC_ID
           and C.CLIENT_TYPE = c_client_juristic
           and A.ADDR_ID(+) = J.JUR_ADDRESS_ID
           and A2.ADDR_ID(+) = J.JUR_FACT_ADDRESS_ID
           and RESP.PERSON_ID(+) = J.JUR_RESP_ID
           and TOUCH.PERSON_ID(+) = J.JUR_TOUCH_ID
           and BOSS.PERSON_ID(+) = J.JUR_BOSS_ID
           and D1.DOC_ID(+) = RESP.DOC_ID
           and D2.DOC_ID(+) = TOUCH.DOC_ID
           and D3.DOC_ID(+) = BOSS.DOC_ID
           and ((not (pi_par1 is null)) or (not (pi_par2 is null)) or
               (not (pi_par3 is null)) or (not (pi_par4 is null)) or
               (not (pi_par5 is null)) or (not (pi_par6 is null)) or
               (not (pi_par7 is null)) or (not (pi_par8 is null)) or
               (not (pi_par9 is null)) or (not (pi_par10 is null)))
           and (((pi_all = 1) and
               ((pi_par1 is null) or (pi_par1 = J.JUR_OGRN)) and
               ((pi_par2 is null) or (lower(pi_par2) = lower(J.JUR_NAME))) and
               ((pi_par3 is null) or (pi_par3 = J.JUR_INN)) and
               ((pi_par4 is null) or
               (lower(pi_par4) = lower(RESP.PERSON_LASTNAME)) or
               (lower(pi_par4) = lower(TOUCH.PERSON_LASTNAME)) or
               (lower(pi_par4) = lower(BOSS.PERSON_LASTNAME))) and
               ((pi_par5 is null) or
               (lower(pi_par5) = lower(RESP.PERSON_FIRSTNAME)) or
               (lower(pi_par5) = lower(TOUCH.PERSON_FIRSTNAME)) or
               (lower(pi_par5) = lower(BOSS.PERSON_FIRSTNAME))) and
               ((pi_par6 is null) or
               (lower(pi_par6) = lower(RESP.PERSON_MIDDLENAME)) or
               (lower(pi_par6) = lower(TOUCH.PERSON_MIDDLENAME)) or
               (lower(pi_par6) = lower(BOSS.PERSON_MIDDLENAME))) and
               ((pi_par7 is null) or
               (lower(pi_par7) = lower(RESP.PERSON_PHONE)) or
               (lower(pi_par7) = lower(TOUCH.PERSON_PHONE)) or
               (lower(pi_par7) = lower(BOSS.PERSON_PHONE))) and
               ((pi_par8 is null) or
               (lower(pi_par8) = lower(RESP.PERSON_INN)) or
               (lower(pi_par8) = lower(TOUCH.PERSON_INN)) or
               (lower(pi_par8) = lower(BOSS.PERSON_INN))) and
               ((pi_par9 is null) or (pi_par9 = D1.DOC_SERIES) or
               (pi_par9 = D2.DOC_SERIES) or (pi_par9 = D3.DOC_SERIES)) and
               ((pi_par10 is null) or (pi_par10 = D1.DOC_NUMBER) or
               (pi_par10 = D2.DOC_NUMBER) or (pi_par10 = D3.DOC_NUMBER))) or
               ((pi_all = 0) and
               ((pi_par1 = J.JUR_OGRN) or
               (lower(pi_par2) = lower(J.JUR_NAME)) or
               (pi_par3 = J.JUR_INN) or
               (lower(pi_par4) = lower(RESP.PERSON_LASTNAME)) or
               (lower(pi_par4) = lower(TOUCH.PERSON_LASTNAME)) or
               (lower(pi_par4) = lower(BOSS.PERSON_LASTNAME)) or
               (lower(pi_par5) = lower(RESP.PERSON_FIRSTNAME)) or
               (lower(pi_par5) = lower(TOUCH.PERSON_FIRSTNAME)) or
               (lower(pi_par5) = lower(BOSS.PERSON_FIRSTNAME)) or
               (lower(pi_par6) = lower(RESP.PERSON_MIDDLENAME)) or
               (lower(pi_par6) = lower(TOUCH.PERSON_MIDDLENAME)) or
               (lower(pi_par6) = lower(BOSS.PERSON_MIDDLENAME)) or
               (lower(pi_par7) = lower(RESP.PERSON_PHONE)) or
               (lower(pi_par7) = lower(TOUCH.PERSON_PHONE)) or
               (lower(pi_par7) = lower(BOSS.PERSON_PHONE)) or
               (lower(pi_par8) = lower(RESP.PERSON_INN)) or
               (lower(pi_par8) = lower(TOUCH.PERSON_INN)) or
               (lower(pi_par8) = lower(BOSS.PERSON_INN)) or
               (pi_par9 = D1.DOC_SERIES) or (pi_par9 = D2.DOC_SERIES) or
               (pi_par9 = D3.DOC_SERIES) or (pi_par10 = D1.DOC_NUMBER) or
               (pi_par10 = D2.DOC_NUMBER) or (pi_par10 = D3.DOC_NUMBER))))
         order by CLIENT_ID asc;
    end if;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Client_List;
  ------------------------------------------------------------------
  function group_concat_user_roles(pi_worker_id in T_USER_ORG.USR_ID%type,
                                   pi_org_id    in T_USER_ORG.ORG_ID%type)
    return t_vc200 is
    res     t_vc200 := '';
    tmp_rec double_id_type;
    cur1    cur_ref_double_id_type;
    count_0 pls_integer := 0;
  begin
    open cur1 for
      select UO.ROLE_ID, UO.ORG_ID, r.is_privilege
        from T_USER_ORG UO
        join t_roles r
          on r.role_id = uo.role_id
       where UO.USR_ID = pi_worker_id;
    loop
      fetch cur1
        into tmp_rec;
      exit when cur1%notfound;
      if (count_0 > 0) then
        res := Substr(res || ',', 1, 2000);
      end if;
      res     := Substr(res || tmp_rec.id1 || ',' || tmp_rec.id2 || ',' ||
                        tmp_rec.id3,
                        1,
                        2000);
      count_0 := count_0 + 1;
    end loop;
    close cur1;
    return res;
  end group_concat_user_roles;
  ------------------------------------------------------------------------
  procedure Block_User(pi_usr_id    in T_Users.Usr_Id%type,
                       pi_block     in pls_integer,
                       pi_order_num in t_users_hist.order_num%type,
                       -- 70937
                       pi_ip_address in t_users_hist.ip_address%type,
                       pi_worker_id  in T_USERS.USR_ID%type,
                       po_err_num    out pls_integer,
                       po_err_msg    out t_Err_Msg) is
    l_max_hist_id number;
    l_su_id       number;
    l_sa_id       number;
    l_dog_id      number;
  begin
    if (not Security_pkg.Check_User_Right2((case
                                             when is_user_usi(pi_usr_id) = 0 then
                                              5305
                                             else
                                              5310
                                           end),
                                           pi_worker_id,
                                           po_err_num,
                                           po_err_msg)) then
      return;
    end if;

    select nvl(max(t.hist_id), 0)
      into l_max_hist_id
      from t_users_hist t
     where t.user_id = pi_usr_id;

    update T_USERS U
       set U.IS_ENABLED =
           (1 - pi_block),
           u.worker_id  = pi_worker_id
     where U.USR_ID = pi_usr_id;

    if (pi_block = 1) then
      update T_SESSION_LOG SL
         set SL.END_TIME = sysdate, SL.END_BY_TIMEOUT = 0
       where SL.ID_SESSION in
             (select S.ID from T_SESSIONS S where S.ID_WORKER = pi_usr_id);
      delete from T_SESSIONS S where S.ID_WORKER = pi_usr_id;
    end if;
    update t_users_hist t
       set t.order_num = pi_order_num, t.ip_address = pi_ip_address
     where t.user_id = pi_usr_id
       and t.hist_id > l_max_hist_id;
    -- найдем связанных супервайзера и активника и заблокируем/разблокируем их
    begin
      select max(su.su_id)
        into l_su_id
        from t_supervisor su
       where su.su_user_id = pi_usr_id;

      l_su_id    := supervizor.blockSupervisor(pi_su_id     => l_su_id,
                                               pi_op_id     => 1 - pi_block,
                                               pi_worker_id => pi_worker_id,
                                               po_err_num   => po_err_num,
                                               po_err_msg   => po_err_msg);
      po_err_num := 0;
      po_err_msg := null;
    exception
      when no_data_found then
        null;
    end;
    begin
      select sa.sa_id
        into l_sa_id
        from t_seller_active sa
       where sa.sa_user_id = pi_usr_id;
         --and sa.sa_is_block = 0;Задача № 110906

      l_sa_id    := supervizor.blockSellerActive(pi_sa_id     => l_sa_id,
                                                 pi_op_id     => 1 - pi_block,
                                                 pi_worker_id => pi_worker_id,
                                                 po_err_num   => po_err_num,
                                                 po_err_msg   => po_err_msg);
      po_err_num := 0;
      po_err_msg := null;
    exception
      when no_data_found then
        null;
    end;
    l_dog_id := supervizor.isUserGPH(pi_usr_id, po_err_num, po_err_msg);
    if nvl(l_dog_id, -1) > 0 then
      orgs.Block_Dogovor(pi_dogovor_id => l_dog_id,
                         pi_block      => pi_block,
                         pi_worker_id  => pi_worker_id,
                         po_err_num    => po_err_num,
                         po_err_msg    => po_err_msg);
    end if;
    if (pi_block <> 1) then
      notifications.set_notifications_to_user(pi_usr_id,
                                              po_err_num,
                                              po_err_msg);
    end if;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
  end Block_User;
  ---------------------------- Поиск Единого ТП -----------------------------
  ----------------------------------------------------------------------------
  function Get_Ed_Tar_By_Org(pi_org_id    in number,
                             pi_worker_id in number,
                             po_err_num   out pls_integer,
                             po_err_msg   out t_err_msg) return number is
    l_ed_tar_id number;
    l_region    number;
  begin
    select o.region_id
      into l_region
      from T_ORGANIZATIONS o
     where o.org_id = pi_org_id;

    select e.tar_id
      into l_ed_tar_id
      from T_EDTAR_REGION e
     where e.region_id = l_region;

    return l_ed_tar_id;
  exception
    when no_data_found then
      po_err_num := -1;
      po_err_msg := 'Данные не найдены';
      return 0;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return 0;
  end Get_Ed_Tar_By_Org;

  ----------------------------------------------------------------------------
  -- Поиск абонента по НШД или номеру телефона
  ----------------------------------------------------------------------------
  function Get_Abonent_By_Ab_Number(pi_ab_number in varchar2,
                                    pi_worker_id in T_USERS.USR_ID%type,
                                    pi_where     in number, -- поиск в ЕИССД - 0, СГС - 1
                                    /*pi_where теперь всегда 0, т.к. от СГС отказались*/
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
    return sys_refcursor is
    res      sys_refcursor;
    l_is_usi number;
  begin
    po_tariff         := Null;
    po_subs_number    := Null;
    po_imsi           := Null;
    po_fullname       := Null;
    po_birthday       := Null;
    po_psp_series     := Null;
    po_psp_number     := Null;
    po_psp_issue_date := Null;
    po_psp_issuer     := Null;
    po_reg_city       := Null;
    po_reg_street     := Null;
    po_reg_home       := Null;
    po_reg_build      := Null;
    po_reg_flat       := Null;
    po_fact_city      := Null;
    po_fact_street    := Null;
    po_fact_home      := Null;
    po_fact_build     := Null;
    po_fact_flat      := Null;
    po_err_num        := Null;
    po_err_msg        := Null;
    if (not Security_pkg.Check_Rights_Orgs_str('EISSD.ABONENT.SEARCH',
                                               Get_User_Orgs_Tab(pi_worker_id),
                                               pi_worker_id,
                                               po_err_num,
                                               po_err_msg)) then
      return null;
    end if;
    l_is_usi := 0;
    for i in (select * from table(get_user_orgs_tab(pi_worker_id))) loop
      if (is_org_usi(i.column_value) = 1) then
        l_is_usi := 1;
      end if;
    end loop;
    open res for
      select distinct tarif,
                      ab_number,
                      imsi,
                      person_lastname,
                      person_firstname,
                      person_middlename,
                      person_birthday,
                      doc_series,
                      doc_number,
                      doc_regdate,
                      doc_extrainfo,
                      reg_addr_country,
                      reg_addr_index,
                      reg_addr_city,
                      reg_addr_street,
                      reg_addr_building,
                      reg_addr_office,
                      reg_addr_corp,
                      reg_region_id,
                      fact_addr_country,
                      fact_addr_index,
                      fact_addr_city,
                      fact_addr_street,
                      fact_addr_building,
                      fact_addr_office,
                      fact_addr_corp,
                      fact_region_id,
                      tmc_type,
                      is_canceled,
                      cancel_date,
                      reg_addr_block,
                      reg_addr_structure,
                      reg_addr_fraction,
                      fact_addr_block,
                      fact_addr_structure,
                      fact_addr_fraction
        from (select /*distinct*/
               tar.title tarif,
               to_char(ts.sim_callsign) ab_number,
               ts.sim_imsi imsi,
               p.person_lastname,
               p.person_firstname,
               p.person_middlename,
               p.person_birthday,
               td.doc_series,
               td.doc_number,
               td.doc_regdate,
               td.doc_extrainfo,
               ta_reg.addr_country reg_addr_country,
               ta_reg.addr_index reg_addr_index,
               ta_reg.addr_city reg_addr_city,
               ta_reg.addr_street reg_addr_street,
               ta_reg.addr_building reg_addr_building,
               ta_reg.addr_office reg_addr_office,
               ta_reg.addr_corp reg_addr_corp,
               ta_reg.region_id reg_region_id,
               ta_fact.addr_country fact_addr_country,
               ta_fact.addr_index fact_addr_index,
               ta_fact.addr_city fact_addr_city,
               ta_fact.addr_street fact_addr_street,
               ta_fact.addr_building fact_addr_building,
               ta_fact.addr_office fact_addr_office,
               ta_fact.addr_corp fact_addr_corp,
               ta_fact.region_id fact_region_id,
               t.tmc_type,
               a.is_canceled is_canceled,
               a.cancel_date cancel_date,
               ta_reg.addr_block reg_addr_block,
               ta_reg.addr_structure reg_addr_structure,
               ta_reg.addr_fraction reg_addr_fraction,
               ta_fact.addr_block fact_addr_block,
               ta_fact.addr_structure fact_addr_structure,
               ta_fact.addr_fraction fact_addr_fraction
                from t_abonent        a,
                     t_tmc            t,
                     t_tmc_sim        ts,
                     t_tarif_by_at_id tar,
                     t_clients        tc,
                     t_person         p,
                     t_documents      td,
                     t_address        ta_reg,
                     t_address        ta_fact
               where a.ab_tmc_id = t.tmc_id
                 and t.tmc_type = 8
                 and ts.tmc_id = t.tmc_id
                 and ts.sim_callsign = pi_ab_number
                 and tar.at_id = ts.tar_id
                 and a.client_id = tc.client_id
                 and tc.fullinfo_id = p.person_id
                 and p.doc_id = td.doc_id
                 and ta_reg.addr_id = p.addr_id_reg
                 and ta_fact.addr_id = p.addr_id_reg
              union all
              select /*distinct*/
               sl.name tarif,
               to_char(ac.nsd) ab_number,
               ac.imsi,
               p.person_lastname,
               p.person_firstname,
               p.person_middlename,
               p.person_birthday,
               td.doc_series,
               td.doc_number,
               td.doc_regdate,
               td.doc_extrainfo,
               ta_reg.addr_country reg_addr_country,
               ta_reg.addr_index reg_addr_index,
               ta_reg.addr_city reg_addr_city,
               ta_reg.addr_street reg_addr_street,
               ta_reg.addr_building reg_addr_building,
               ta_reg.addr_office reg_addr_office,
               ta_reg.addr_corp reg_addr_corp,
               ta_reg.region_id reg_region_id,
               ta_fact.addr_country fact_addr_country,
               ta_fact.addr_index fact_addr_index,
               ta_fact.addr_city fact_addr_city,
               ta_fact.addr_street fact_addr_street,
               ta_fact.addr_building fact_addr_building,
               ta_fact.addr_office fact_addr_office,
               ta_fact.addr_corp fact_addr_corp,
               ta_fact.region_id fact_region_id,
               t.tmc_type,
               null is_canceled,
               null cancel_date,
               ta_reg.addr_block reg_addr_block,
               ta_reg.addr_structure reg_addr_structure,
               ta_reg.addr_fraction reg_addr_fraction,
               ta_fact.addr_block fact_addr_block,
               ta_fact.addr_structure fact_addr_structure,
               ta_fact.addr_fraction fact_addr_fraction
                from t_contract_usluga     cu,
                     t_contract_usluga_tmc cut,
                     t_contract_product    cp,
                     t_contract_client     cc,
                     t_tmc_adsl_card       ac,
                     t_tmc                 t,
                     t_clients             tc,
                     t_person              p,
                     t_documents           td,
                     t_address             ta_reg,
                     t_address             ta_fact,
                     t_lira_service        sl,
                     t_lira_rate_plan_pack lrpp
               where cu.usl_id = cut.usl_id
                 and cut.tmc_id = t.tmc_id
                 and cu.id_product = cp.id_product
                 and cp.contract_id = cc.contract_id
                 and t.tmc_type = 7000
                 and t.tmc_id = ac.tmc_id
                 and ac.nsd = pi_ab_number
                 and cc.client_id = tc.client_id
                 and tc.fullinfo_id = p.person_id
                 and p.doc_id = td.doc_id
                 and ta_reg.addr_id = p.addr_id_reg
                 and ta_fact.addr_id = p.addr_id_reg
                 and lrpp.pack_id = cp.pack_id
                 and nvl(lrpp.service_code, 0) = nvl(sl.id, 0));

    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Abonent_By_Ab_Number;
  ----------------------------------------------------------------------------
  -- Получение адреса по идентификатору
  ----------------------------------------------------------------------------
  function getAddress(pi_address_id in t_address.addr_id%type)
    return sys_refcursor is
  begin
    return addresse_pkg.getAddress_by_id_for_eissd(pi_address_id);   
  end getAddress;
  ----------------------------------------------------------------------------
  --Аутентификация
  ----------------------------------------------------------------------------
  function Get_User_By_Login(pi_login          in T_USERS.USR_LOGIN%type,
                             pi_password_md5   in T_USERS.USR_PWD_MD5%type,
                             pi_hash_key       in T_SESSIONS.HASH_KEY%type,
                             pi_worker_ip      in T_SESSION_LOG.WORKER_IP%type := null,
                             pi_session_action in number)
    return sys_refcursor is
    res sys_refcursor := null;
    type t_cur is ref cursor;
    lIdUser    pls_integer := -1;
    lIsEnabled pls_integer := 0;

    cur           t_cur;
    session_count number;
    po_err_num    pls_integer;
    po_err_msg    varchar2(255);
    str_hash      t_Sessions.Hash_Key%type;
  begin
    select NVL2(UU.USR_ID,
                -- Логин был найден
                (case
                -- Пароль пользователя подходит
                  when UPPER(UU.USR_PWD_MD5) = UPPER(pi_password_md5) then
                   UU.USR_ID
                -- Ошибка в пароле
                  else
                   -333
                end),
                -- Ошибка в логине
                -222),
           NVL(UU.IS_ENABLED, 0)
      into lIdUser, lIsEnabled
      from (select lower(pi_login) login from dual) D, T_USERS UU
     where D.login = lower(UU.USR_LOGIN(+));

    Security.Set_User_Session(lIdUser, pi_hash_key, pi_worker_ip);

    select count(id_worker)
      into session_count
      from t_sessions
     where id_worker = lIdUser;

    open res for
      select distinct (case
                      -- Логин или пароль не верны
                        when lIdUser < 0 then
                         lIdUser
                      -- Организации в которых работает пользователь заблокированы
                      -- или отсутствуют
                        when (select count(*)
                                from T_USER_ORG UO, T_ORGANIZATIONS O
                               where UO.USR_ID = lIdUser
                                 and Uo.Org_Id = O.ORG_ID
                                 and O.IS_ENABLED = 1) = 0 then
                         -444
                      -- Пользователь заблокирован
                        when lIdUser >= 0 and lIsEnabled = 0 then
                         -111
                      -- Пользователь может работать
                        else
                         lIdUser
                      end) USR_ID,
                      pi_login USR_LOGIN,
                      P.PERSON_LASTNAME,
                      P.PERSON_FIRSTNAME,
                      P.PERSON_MIDDLENAME,
                      P.PERSON_EMAIL,
                      NVL(U.USR_STATUS, 'N') USR_STATUS,
                      NVL2(UO.ORG_ID,
                           group_concat_user_roles(lIdUser, UO.ORG_ID),
                           '') ROLES,
                      lIsEnabled IS_ENABLED,
                      u.org_id,
                      session_count as session_count,
                      u.system_id,
                      u.date_login_to,
                      u.date_pswd_to,
                      u.is_temp_passwd
        from T_USERS U, T_USER_ORG UO, T_PERSON P
       where lIdUser = U.USR_ID(+)
         and U.USR_ID = UO.USR_ID(+)
         and U.USR_PERSON_ID = P.PERSON_ID(+);
    --проверка сессий
    if (not
        Security_pkg.Check_Rights(5814, 0, lIdUser, po_err_num, po_err_msg)) and
       lIdUser not in (1008) then
      if (session_count > 0) and (pi_session_action = 0) then
        return res;
      end if;
      if (session_count > 0) and (pi_session_action = 1) then
        open cur for
          select t.hash_key from t_sessions t where t.id_worker = lIdUser;
        loop
          fetch cur
            into str_hash;
          exit when cur%notfound;
          security.Kill_User_Session(str_hash);
        end loop;
        close cur;
      end if;
    end if;

    session_count := 0;

    if (lIdUser >= 0 and lIsEnabled > 0) then
      Security.Set_User_Session(lIdUser, pi_hash_key, pi_worker_ip);
    end if;
    close res;
    open res for
      select distinct (case
                      -- Логин или пароль не верны
                        when lIdUser < 0 then
                         lIdUser
                      -- Организации в которых работает пользователь заблокированы
                      -- или отсутствуют
                        when (select count(*)
                                from T_USER_ORG UO, T_ORGANIZATIONS O
                               where UO.USR_ID = lIdUser
                                 and Uo.Org_Id = O.ORG_ID
                                 and O.IS_ENABLED = 1) = 0 then
                         -444
                      -- Пользователь заблокирован
                        when lIdUser >= 0 and lIsEnabled = 0 then
                         -111
                      -- Пользователь может работать
                        else
                         lIdUser
                      end) USR_ID,
                      pi_login USR_LOGIN,
                      P.PERSON_LASTNAME,
                      P.PERSON_FIRSTNAME,
                      P.PERSON_MIDDLENAME,
                      P.PERSON_EMAIL,
                      NVL(U.USR_STATUS, 'N') USR_STATUS,
                      NVL2(UO.ORG_ID,
                           group_concat_user_roles(lIdUser, UO.ORG_ID),
                           '') ROLES,
                      lIsEnabled IS_ENABLED,
                      u.org_id,
                      session_count as session_count,
                      u.system_id,
                      u.date_login_to,
                      u.date_pswd_to,
                      u.is_temp_passwd
        from T_USERS U, T_USER_ORG UO, T_PERSON P
       where lIdUser = U.USR_ID(+)
         and U.USR_ID = UO.USR_ID(+)
         and U.USR_PERSON_ID = P.PERSON_ID(+);
    return res;
  end Get_User_By_Login;
  ---------------------------------------------------------------
  --  регистрация под правами пользователя
  ---------------------------------------------------------------
  Function Create_User_Session_By_User_Id(pi_Worker_Id in T_SESSION_LOG.ID_WORKER%type,
                                          pi_worker_ip in T_SESSION_LOG.WORKER_IP%type := null,
                                          pi_hash_key  in T_SESSIONS.HASH_KEY%type)
    return sys_refcursor is
    res              sys_refcursor := null;
    l_login          T_USERS.USR_LOGIN%type;
    l_password_md5   T_USERS.USR_PWD_MD5%type;
    l_session_action number := 1;
  Begin
    Select t.usr_login, t.usr_pwd_md5
      into l_login, l_password_md5
      from t_Users t
     where t.usr_id = pi_Worker_Id;
    res := Get_User_By_Login(l_login,
                             l_password_md5,
                             pi_hash_key,
                             pi_worker_ip,
                             l_session_action);
    return res;
  End;
  ----------------------------------------------------------------------------
  /*Функционал для работы с полевыми инженерами!!!*/
  ----------------------------------------------------------------------------
  procedure add_user_qualifications(pi_user_id        in number,
                                    pi_qualifications in num_tab) is
    pragma autonomous_transaction;
  begin
    savepoint sp_add_user_qualifications;
    update t_user_qualifications uq
       set uq.is_actual = 0
     where uq.id_user = pi_user_id;

    insert into t_user_qualifications
      (qualification, id_user, date_set, is_actual)
      select a.*, pi_user_id, sysdate, 1 from table(pi_qualifications) a;
    commit;
  exception
    when others then
      rollback to sp_add_user_qualifications;
  end;
  ----------------------------------------------------------------------------
  procedure add_user_scheds(pi_user_id in number,
                            pi_scheds  in team_count_tab) is
    pragma autonomous_transaction;
  begin
    savepoint sp_add_user_scheds;
    update t_user_scheds us
       set us.is_actual = 0
     where us.id_user = pi_user_id;
    insert into t_user_scheds
      (id_sched_team, count_team, id_user, date_set, is_actual)
      select a.sched_team, a.count_team, pi_user_id, sysdate, 1
        from table(pi_scheds) a;
    commit;
  exception
    when others then
      rollback to sp_add_user_scheds;
  end;
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
                    pi_ip_address in t_users_hist.ip_address%type,
                    -- 86340. Добавление Salt и HashAlgId.
                    pi_salt        in t_users.SALT%type,
                    pi_hash_alg_id in t_users.HASH_ALG_ID%type,
                    pi_employee_number t_users.employee_number%TYPE,
                    po_err_num     out pls_integer,
                    po_err_msg     out t_Err_Msg)
    return T_USERS.USR_ID%type is
    l_user_id     T_USERS.USR_ID%type := null;
    l_person_id   T_PERSON.PERSON_ID%type;
    l_login_count pls_integer := 0;
    cnt           number;
    ex_role_exc exception; --взаимоисключающие роли
    l_count           number;
    l_block_org_count number;
    ex_block_org exception;
  begin
    savepoint sp_Add_User;
    -- checking access for operation for specified user
    if (not Security_pkg.Check_Rights_str((case
                                            when is_org_usi(pi_org_id) = 0 then
                                             'EISSD.WORKER.REGISTER'
                                            else
                                             'EISSD.WORKER_USI.REGISTER'
                                          end),
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg,
                                          null,
                                          null) and
       not Security_pkg.Check_Rights_str((case
                                            when is_org_usi(pi_org_id) = 0 then
                                             'EISSD.WORKER.REGISTER'
                                            else
                                             'EISSD.WORKER_USI.REGISTER'
                                          end),
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg,
                                          true,
                                          false)) then
      return l_user_id;
    end if;
    -- Проверка для организаций телемаркетинга
    -- Имеет ли пользователь права в организации без связи 1009
    select count(*)
      into l_count
      from mv_org_tree t
     where t.org_reltype <> 1009
       and t.org_id = pi_org_id
    connect by prior t.org_id = t.org_pid
     start with t.org_id in (select distinct uo.org_id
                               from t_user_org uo
                              where uo.usr_id = pi_worker_id);
    if l_count = 0 then
      po_err_num := 2100;
      po_err_msg := 'У вас отсутствуют права на выполнение данной операции';
      return l_user_id;
    end if;
  
    if check_user_role(pi_worker_id, null) <> 1 then
      -- проверим право создавать пользователя с данными ролями
      if (not Security.Check_Rights_For_Roles(pi_roles,
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
        return l_user_id;
      end if;
    end if;
  
    select count(*)
      into l_login_count
      from T_USERS U
     where lower(U.USR_LOGIN) = lower(pi_login);
  
    if (pi_org_id = -1) then
      raise ex_invalid_org_id;
    end if;
    if (l_login_count > 0) then
      raise ex_invalid_login;
    end if;
  
    -- Проверка заблокированности организации
    select count(*)
      into l_block_org_count
      from t_organizations org
     where org.org_id = pi_org_id
       and org.is_enabled = 0;
  
    if l_block_org_count > 0 then
      raise ex_block_org;
    end if;
  
    l_person_id := Ins_Person(pi_first_name            => SUBSTR(pi_firstname,
                                                                 0,
                                                                 30),
                              pi_middle_name           => SUBSTR(pi_middlename,
                                                                 0,
                                                                 30),
                              pi_last_name             => SUBSTR(pi_lastname,
                                                                 0,
                                                                 30),
                              pi_sex                   => null,
                              pi_birth_day             => null,
                              pi_inn                   => null,
                              pi_email                 => SUBSTR(pi_email,
                                                                 0,
                                                                 255),
                              pi_phone                 => SUBSTR(pi_person_phone,
                                                                 0,
                                                                 16),
                              pi_reg_country           => null,
                              pi_reg_city              => null,
                              pi_reg_post_index        => null,
                              pi_reg_street            => null,
                              pi_reg_house             => null,
                              pi_reg_corp              => null,
                              pi_reg_flat              => null,
                              pi_reg_code_city         => null,
                              pi_reg_code_street       => null,
                              pi_fact_country          => null,
                              pi_fact_city             => null,
                              pi_fact_post_index       => null,
                              pi_fact_street           => null,
                              pi_fact_house            => null,
                              pi_fact_corp             => null,
                              pi_fact_flat             => null,
                              pi_fact_code_city        => null,
                              pi_fact_code_street      => null,
                              pi_passport_ser          => null,
                              pi_passport_num          => null,
                              pi_passport_receive_date => null,
                              pi_passport_receive_who  => null,
                              pi_sgs_id                => null,
                              pi_birthplace            => null,
                              pi_reg_region            => null,
                              pi_fact_region           => null,
                              pi_dlv_acc_type          => null,
                              pi_home_phone            => null,
                              pi_reg_city_lvl          => null,
                              pi_reg_street_lvl        => null,
                              pi_reg_house_code        => null,
                              pi_fact_city_lvl         => null,
                              pi_fact_street_lvl       => null,
                              pi_fact_house_code       => null,
                              pi_passport_type         => null,
                              pi_reg_addr_obj_id       => null,
                              pi_reg_house_obj_id      => null,
                              pi_fact_addr_obj_id      => null,
                              pi_fact_house_obj_id     => null);
  
    insert into T_USERS
      (USR_PERSON_ID,
       USR_LOGIN,
       USR_PWD_MD5,
       Org_Id,
       worker_id,
       system_id,
       DATE_LOGIN_TO,
       DATE_PSWD_TO,
       IS_TEMP_PASSWD,
       -- 58633
       FIO_DOVER,
       POSITION_DOVER,
       NA_OSNOVANII_DOVER,
       ADDRESS_DOVER,
       NA_OSNOVANII_DOC_TYPE,
       fio_dover_nominative,
       boss_email,
       -- 86340
       salt,
       hash_alg_id,
       employee_number)
    values
      (l_person_id,
       pi_login,
       pi_password_md5,
       pi_org_id,
       pi_worker_id,
       pi_system,
       pi_date_login_to,
       add_months(trunc(sysdate), 2),
       1,
       -- 58633
       PI_FIO_DOVER,
       PI_POSITION_DOVER,
       PI_NA_OSNOVANII_DOVER,
       PI_ADDRESS_DOVER,
       PI_NA_OSNOVANII_DOC_TYPE,
       pi_fio_dover_nominative,
       pi_boss_email,
       -- 86340
       pi_salt,
       NVL(pi_hash_alg_id, 1),
       pi_employee_number)
    returning USR_ID into l_user_id;
  
    update t_users_hist t
       set t.ip_address = pi_ip_address
     where t.user_id = l_user_id;
  
    --48924 - проверка на отсутствие взаимоисключающих ролей
    for item in (Select column_value as role_id from TABLE(pi_roles)) loop
      Select count(*)
        into cnt
        from TABLE(pi_roles) roles
       where column_value in
             (Select re.exc_role_id
                from t_roles_exc re
               where re.role_id = item.role_id);
      if (cnt <> 0) then
        raise ex_role_exc;
      end if;
    end loop;
  
    insert into T_USER_ORG
      (USR_ID, ORG_ID, ROLE_ID)
      select l_user_id, pi_org_id, role.*
        from (TABLE(cast(pi_roles as num_tab))) role;
    update t_users_hist t
       set t.order_num = pi_order_num
     where t.user_id = l_user_id;
  
    add_user_qualifications(l_user_id, pi_qualifications);
    add_user_scheds(l_user_id, pi_scheds);
    notifications.set_notifications_to_user(l_user_id,
                                            po_err_num,
                                            po_err_msg);
    return l_user_id;
  exception
    when no_data_found then
      return l_user_id;
    when ex_invalid_org_id then
      po_err_num := 1001;
      po_err_msg := 'Неверное значение идеинтификатора организации (org_id=' ||
                    pi_org_id || ').';
      rollback to sp_Add_User;
      return null;
    when ex_invalid_login then
      po_err_num := 1002;
      po_err_msg := 'Такой логин уже существует в системе (login=' ||
                    pi_login || ').';
      rollback to sp_Add_User;
      return null;
    when ex_role_exc then
      po_err_num := 1003;
      po_err_msg := 'Найдены взаимоисключающие роли.';
      rollback to sp_Add_User;
      return null;
    when ex_block_org then
      po_err_num := 1004;
      po_err_msg := 'Организация заблокирована. Создание пользователя невозможно.';
      rollback to sp_Add_User;
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      rollback to sp_Add_User;
      return null;
  end;
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
                       pi_ip_address in t_users_hist.ip_address%type,
                       -- 86340
                       pi_salt        in t_users.salt%type,
                       pi_hash_alg_id in t_users.HASH_ALG_ID%type,
                       pi_employee_number t_users.employee_number%TYPE,
                       po_err_num     out pls_integer,
                       po_err_msg     out t_Err_Msg)
    return T_USERS.USR_ID%type is
    right_to_edit     BOOLEAN := false;
    is_change_himself BOOLEAN;
    l_other_org_cou   pls_integer := 0;
    ex_role_exc exception;
    cnt           number;
    l_del_role    number := 0;
    l_max_hist_id number;
    l_person_id   T_PERSON.PERSON_ID%type;
    l_count       number;
    l_old_user    t_users%rowtype;
    --    l_is_only_pass_change   number(1) := 0;
    l_is_password_change number(1) := 0;
    l_block              number;
    --    l_is_data_change        number(1) := 0;
    l_cnt_role number;
  begin
    savepoint sp_change_user;
    logging_pkg.debug(pi_boss_email || ' ' || pi_email || ' ' || pi_employee_number, 'Change_User');
    is_change_himself := (pi_worker_id = pi_user_id);
    right_to_edit := Security_pkg.Check_Rights((case
                                                 when (is_org_usi(pi_org_id) > 0 or
                                                      is_user_usi(pi_user_id) > 0) then
                                                  5309
                                                 else
                                                  5304
                                               end),
                                               pi_org_id,
                                               pi_worker_id,
                                               po_err_num,
                                               po_err_msg,
                                               null,
                                               null);

    -- Проверка для организаций телемаркетинга
    -- Имеет ли пользователь права в организации без связи 1009
    select count(*)
      into l_count
      from mv_org_tree t
     where t.org_reltype <> 1009
       and t.org_id = pi_org_id
    connect by prior t.org_id = t.org_pid
     start with t.org_id in (select distinct uo.org_id
                               from t_user_org uo
                              where uo.usr_id = pi_worker_id);
    if l_count = 0 then
      po_err_num := 2100;
      po_err_msg := 'У вас отсутствуют права на выполнение данной операции';
      return null;
    end if;

    if (is_change_himself) then
      po_err_num := 0;
      po_err_msg := '';
    end if;
    if ((not right_to_edit) and (not is_change_himself)) then
      return null;
    end if;

    select nvl(max(t.hist_id), 0)
      into l_max_hist_id
      from t_users_hist t
     where t.user_id = pi_user_id;

    if (pi_password_md5 is not null) then
      select * into l_old_user from t_users t where t.usr_id = pi_user_id;

      if pi_password_md5 <> l_old_user.usr_pwd_md5 then
        l_is_password_change := 1;
      end if;

      update T_USERS U
         set U.USR_PWD_MD5    = pi_password_md5,
             u.org_id         = pi_org_id,
             u.worker_id      = pi_worker_id,
             u.tz_id          = NVL(pi_tz_id, 19),
             u.date_login_to  = nvl(pi_date_login_to, u.date_login_to),
             u.date_pswd_to = case
                                when pi_password_md5 <> u.usr_pwd_md5 then
                                 add_months(trunc(sysdate - 2 / 24), 2)
                                else
                                 u.date_pswd_to
                              end,
             u.is_temp_passwd = (case
                                  when pi_worker_id = pi_user_id and
                                       pi_password_md5 <> u.usr_pwd_md5 then
                                   0
                                  when pi_worker_id <> pi_user_id and
                                       pi_password_md5 <> u.usr_pwd_md5 then
                                   1
                                  else
                                   u.is_temp_passwd
                                end),
             u.count_attempts = 0,
             -- 58633
             U.FIO_DOVER             = PI_FIO_DOVER,
             U.POSITION_DOVER        = PI_POSITION_DOVER,
             U.NA_OSNOVANII_DOVER    = PI_NA_OSNOVANII_DOVER,
             U.ADDRESS_DOVER         = PI_ADDRESS_DOVER,
             U.NA_OSNOVANII_DOC_TYPE = PI_NA_OSNOVANII_DOC_TYPE,
             u.fio_dover_nominative  = pi_fio_dover_nominative,
             u.boss_email            = pi_boss_email,
             u.salt                  = pi_salt,
             u.HASH_ALG_ID           = NVL(pi_hash_alg_id, 1),
             u.employee_number       = pi_employee_number
       where U.USR_ID = pi_user_id
      returning u.is_enabled into l_block;
    Else
      update T_USERS U
         set u.Org_Id        = pi_org_id,
             u.tz_id         = NVL(pi_tz_id, 19),
             u.date_login_to = nvl(pi_date_login_to, u.date_login_to),
             u.worker_id     = pi_worker_id,
             -- 58633
             U.FIO_DOVER             = PI_FIO_DOVER,
             U.POSITION_DOVER        = PI_POSITION_DOVER,
             U.NA_OSNOVANII_DOVER    = PI_NA_OSNOVANII_DOVER,
             U.ADDRESS_DOVER         = PI_ADDRESS_DOVER,
             U.NA_OSNOVANII_DOC_TYPE = PI_NA_OSNOVANII_DOC_TYPE,
             u.fio_Dover_nominative  = pi_fio_dover_nominative,
             u.boss_email            = pi_boss_email,
             u.employee_number       = pi_employee_number
       where U.USR_ID = pi_user_id
      returning u.is_enabled into l_block;
    end if;

    if (not right_to_edit) and (is_change_himself) then
      l_person_id := Ins_Person(pi_first_name            => pi_firstname,
                                pi_middle_name           => pi_middlename,
                                pi_last_name             => pi_lastname,
                                pi_sex                   => null,
                                pi_birth_day             => null,
                                pi_inn                   => null,
                                pi_email                 => pi_email,
                                pi_phone                 => pi_person_phone,
                                pi_reg_country           => null,
                                pi_reg_city              => null,
                                pi_reg_post_index        => null,
                                pi_reg_street            => null,
                                pi_reg_house             => null,
                                pi_reg_corp              => null,
                                pi_reg_flat              => null,
                                pi_reg_code_city         => null,
                                pi_reg_code_street       => null,
                                pi_fact_country          => null,
                                pi_fact_city             => null,
                                pi_fact_post_index       => null,
                                pi_fact_street           => null,
                                pi_fact_house            => null,
                                pi_fact_corp             => null,
                                pi_fact_flat             => null,
                                pi_fact_code_city        => null,
                                pi_fact_code_street      => null,
                                pi_passport_ser          => null,
                                pi_passport_num          => null,
                                pi_passport_receive_date => null,
                                pi_passport_receive_who  => null,
                                pi_sgs_id                => null,
                                pi_birthplace            => null,
                                pi_reg_region            => null,
                                pi_fact_region           => null,
                                pi_dlv_acc_type          => null,
                                pi_HOME_PHONE            => null,
                                pi_reg_city_lvl          => null,
                                pi_reg_street_lvl        => null,
                                pi_reg_house_code        => null,
                                pi_fact_city_lvl         => null,
                                pi_fact_street_lvl       => null,
                                pi_fact_house_code       => null,
                                pi_passport_type         => null,
                                pi_reg_addr_obj_id       => null,
                                pi_reg_house_obj_id      => null,
                                pi_fact_addr_obj_id      => null,
                                pi_fact_house_obj_id     => null);

      update t_users u
         set u.usr_person_id = l_person_id, u.worker_id = pi_worker_id
       where u.usr_id = pi_user_id;
    end if;

    if (right_to_edit) then
      l_person_id := Ins_Person(pi_first_name            => pi_firstname,
                                pi_middle_name           => pi_middlename,
                                pi_last_name             => pi_lastname,
                                pi_sex                   => null,
                                pi_birth_day             => null,
                                pi_inn                   => null,
                                pi_email                 => pi_email,
                                pi_phone                 => pi_person_phone,
                                pi_reg_country           => null,
                                pi_reg_city              => null,
                                pi_reg_post_index        => null,
                                pi_reg_street            => null,
                                pi_reg_house             => null,
                                pi_reg_corp              => null,
                                pi_reg_flat              => null,
                                pi_reg_code_city         => null,
                                pi_reg_code_street       => null,
                                pi_fact_country          => null,
                                pi_fact_city             => null,
                                pi_fact_post_index       => null,
                                pi_fact_street           => null,
                                pi_fact_house            => null,
                                pi_fact_corp             => null,
                                pi_fact_flat             => null,
                                pi_fact_code_city        => null,
                                pi_fact_code_street      => null,
                                pi_passport_ser          => null,
                                pi_passport_num          => null,
                                pi_passport_receive_date => null,
                                pi_passport_receive_who  => null,
                                pi_sgs_id                => null,
                                pi_birthplace            => null,
                                pi_reg_region            => null,
                                pi_fact_region           => null,
                                pi_dlv_acc_type          => null,
                                pi_HOME_PHONE            => null,
                                pi_reg_city_lvl          => null,
                                pi_reg_street_lvl        => null,
                                pi_reg_house_code        => null,
                                pi_fact_city_lvl         => null,
                                pi_fact_street_lvl       => null,
                                pi_fact_house_code       => null,
                                pi_passport_type         => null,
                                pi_reg_addr_obj_id       => null,
                                pi_reg_house_obj_id      => null,
                                pi_fact_addr_obj_id      => null,
                                pi_fact_house_obj_id     => null);

      update t_users u
         set u.usr_person_id = l_person_id, u.worker_id = pi_worker_id
       where u.usr_id = pi_user_id;

      select count(*)
        into l_other_org_cou
        from T_USER_ORG UO
       where UO.USR_ID = pi_user_id
         and not UO.ORG_ID = pi_org_id;

      if (l_other_org_cou = 0 and (pi_roles is null or pi_roles.count = 0)) and
         is_worker_developer(pi_user_id) = 0 and
         check_user_role(pi_user_id, null) = 0 then
        -- Значит он работал в единственной организации
        -- и хотят удалить все роли в этой организации
        -- тогда мы лучше заблокируем пользователя
        -- не будем его оставлять без привязки к организации
        Block_User(pi_user_id,
                   1,
                   null,
                   pi_ip_address,
                   pi_worker_id,
                   po_err_num,
                   po_err_msg);
      elsif check_user_role(pi_user_id, null) = 1 and
            (pi_roles is null or pi_roles.count = 0) then
        delete from T_USER_ORG UO
         where UO.USR_ID = pi_user_id
           and UO.ORG_ID = pi_org_id
           and uo.role_id not in (101, 102, 103, 13454); --13454 дается и отбирается только через базу
        l_del_role := sql%rowcount;

        if l_del_role > 0 then
          insert into t_users_hist
            (hist_id, user_id, act_id, order_num, worker_id, ip_address)
          values
            (seq_t_users_hist.nextval,
             pi_user_id,
             5,
             pi_order_num,
             pi_worker_id,
             pi_ip_address);
        end if;
      else
        delete from T_USER_ORG UO
         where UO.USR_ID = pi_user_id
           and UO.ORG_ID = pi_org_id
           and uo.role_id not in (101, 102, 103, 13454) --13454 дается и отбирается только через базу
           and uo.role_id not in
               (Select column_value as role_id from TABLE(pi_roles));
        l_del_role := sql%rowcount;
        --48924 - проверка на отсутствие взаимоисключающих ролей
        for item in (Select column_value as role_id from TABLE(pi_roles)) loop
          Select count(*)
            into cnt
            from TABLE(pi_roles) roles
           where column_value in
                 (Select re.exc_role_id
                    from t_roles_exc re
                   where re.role_id = item.role_id);
          if (cnt <> 0) then
            raise ex_role_exc;
          end if;
        end loop;

        insert into T_USER_ORG
          (USR_ID, ORG_ID, ROLE_ID)
          select pi_user_id, pi_org_id, role.column_value
            from (TABLE(cast(pi_roles as num_tab))) role
           where role.column_value not in
                 (select uo.role_id
                    from T_USER_ORG UO
                   where UO.USR_ID = pi_user_id
                     and UO.ORG_ID = pi_org_id);
        l_cnt_role := sql%rowcount;

        if l_block = 1 then
          notifications.set_notifications_to_user(pi_user_id,
                                                  po_err_num,
                                                  po_err_msg);
        end if;

        if l_cnt_role > 0 or l_del_role > 0 then
          insert into t_users_hist
            (hist_id, user_id, act_id, order_num, worker_id, ip_address)
          values
            (seq_t_users_hist.nextval,
             pi_user_id,
             5,
             pi_order_num,
             pi_worker_id,
             pi_ip_address);
        end if;

      end if;
      if (pi_roles is null or pi_roles.count = 0) then
        update t_users u
           set u.org_id =
               (case
                 when u.org_id = pi_org_id then
                  (select uo.org_id
                     from t_user_org uo
                    where uo.usr_id = pi_user_id
                      and rownum = 1)
                 else
                  u.org_id
               end)
         where u.usr_id = pi_user_id;
      end if;
      --если не менялось ничего-значит меняется квалификация
      add_user_qualifications(pi_user_id, pi_qualifications);
      add_user_scheds(pi_user_id, pi_scheds);
    end if;

    if l_is_password_change = 1 then
      insert into t_users_hist
        (hist_id,
         user_id,
         act_id,
         order_num,
         worker_id,
         ip_address,
         salt,
         hash_alg_id)
      values
        (seq_t_users_hist.nextval,
         pi_user_id,
         4, -- Смена пароля
         pi_order_num,
         pi_worker_id,
         pi_ip_address,
         pi_salt,
         pi_hash_alg_id);
    end if;

    update t_users_hist t
       set t.order_num = pi_order_num, t.ip_address = pi_ip_address, t.employee_number = pi_employee_number
     where t.user_id = pi_user_id
       and t.hist_id > l_max_hist_id;

    update t_sessions s
       set s.cache_date = sysdate
     where s.id_worker = pi_user_id;
    return pi_user_id;
  exception
    when no_data_found then
      return pi_user_id;
    when ex_role_exc then
      po_err_num := 1003;
      po_err_msg := 'Найдены взаимоисключающие роли.';
      rollback to sp_change_user;
      return pi_user_id;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      rollback to sp_change_user;
      return pi_user_id;
  end change_user;
  ----------------------------------------------------------------------------
  function Get_Timezones(po_err_num out pls_integer,
                         po_err_msg out t_Err_Msg) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select t.tz_id, t.tz_name, t.tz_offset
        from t_timezone t
       order by t.tz_offset;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Timezones;
  ----------------------------------------------------------------------------
  function get_user_qualifications(pi_user_id in number) return sys_refcursor as
    res sys_refcursor;
  begin
    open res for
      select q.qualification_id as qualification,
             q.name,
             decode(t.id, null, 0, 1) as is_set
        from qualification q
        left join t_user_qualifications t
          on t.qualification = q.qualification_id
         and t.id_user = pi_user_id
         and t.is_actual = 1
       order by q.name;
    return res;
  end;
  ----------------------------------------------------------------------------
  function check_login_and_psswd(pi_login        in T_USERS.USR_LOGIN%type,
                                 pi_password_md5 in T_USERS.USR_PWD_MD5%type,
                                 pi_worker_id    in number,
                                 po_err_num      out pls_integer,
                                 po_err_msg      out varchar2) return number is
    res number;
  begin
    select count(*)
      into res
      from t_users t
     where lower(t.usr_login) = lower(pi_login)
       and lower(t.usr_pwd_md5) = lower(pi_password_md5);
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  ----------------------------------------------------------------------------
  function check_login(pi_login     in varchar2,
                       pi_org_id    in number,
                       pi_worker_id in number,
                       po_msg       out varchar2) return number as

    l_login_count number;
    l_err_num     number;
  begin
    if (not Security_pkg.Check_Rights(5304,
                                      pi_org_id,
                                      pi_worker_id,
                                      l_err_num,
                                      po_msg))

     then
      return l_err_num;
    end if;
    select count(*)
      into l_login_count
      from T_USERS U
     where lower(U.USR_LOGIN) = lower(pi_login);

    if (l_login_count > 0) then
      po_msg    := 'Такой логин уже существует в системе (login=' ||
                   pi_login || ').';
      l_err_num := 1;
    else
      po_msg    := 'Логин свободен (login=' || pi_login || ').';
      l_err_num := 0;
    end if;
    return l_err_num;
  end check_login;
  ----------------------------------------------------------------------------
  function get_org_by_user_id(pi_user_id   in number,
                              pi_worker_id in number,
                              po_err_num   out pls_integer,
                              po_err_msg   out varchar2) return number as
    res number;
  begin
    select distinct (t.org_id)
      into res
      from t_user_org t
     where t.usr_id = pi_user_id
       and rownum = 1; --СПЕЦИФИКА, НУЖНА ЛЮБАЯ ОРГАНИЗАЦИЯ
    return res;
  exception
    when no_data_found then
      po_err_num := -1;
      po_err_msg := 'Данные не найдены';
      return null;
  end get_org_by_user_id;
  ----------------------------------------------------------------------------------
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
                                po_err_msg   out t_Err_Msg) return cur_user is
    res         cur_user;
    l_documents num_tab := num_tab();
  begin
    if (not Security_pkg.Check_Rights_Orgs_str('EISSD.ABONENT.SEARCH',
                                               Get_User_Orgs_Tab(pi_worker_id),
                                               pi_worker_id,
                                               po_err_num,
                                               po_err_msg)) then
      return null;
    end if;
    if not (pi_par1 is null and pi_par2 is null) then
      select d.doc_id bulk collect
        into l_documents
        from t_documents d
       where (pi_par1 is null or d.doc_series = pi_par1)
         and (pi_par2 is null or d.doc_number = pi_par2);
    end if;

    if (pi_cl_type = c_client_person) then
      open res for
        select C.CLIENT_ID,
               P.PERSON_ID,
               P.PERSON_LASTNAME,
               P.PERSON_FIRSTNAME,
               P.PERSON_MIDDLENAME,
               P.PERSON_EMAIL,
               P.PERSON_PHONE,
               P.PERSON_INN,
               P.PERSON_BIRTHDAY,
               P.PERSON_SEX,
               D.DOC_ID            doc_id,
               D.DOC_SERIES        doc_series,
               D.DOC_NUMBER        doc_number,
               D.DOC_REGDATE       doc_date,
               D.DOC_EXTRAINFO     doc_info,
               D.DOC_TYPE          doc_type,
               A1.ADDR_ID          adr1_id,
               A1.ADDR_COUNTRY     adr1_country,
               A1.ADDR_INDEX       adr1_index,
               A1.ADDR_CITY        adr1_city,
               A1.ADDR_STREET      adr1_street,
               A1.ADDR_BUILDING    adr1_building,
               A1.ADDR_OFFICE      adr1_office,
               A1.ADDR_CORP        adr1_corp,
               A1.ADDR_CODE_CITY   adr1_code_city,
               A1.ADDR_CODE_STREET adr1_code_street,
               A2.ADDR_ID          adr2_id,
               A2.ADDR_COUNTRY     adr2_country,
               A2.ADDR_INDEX       adr2_index,
               A2.ADDR_CITY        adr2_city,
               A2.ADDR_STREET      adr2_street,
               A2.ADDR_BUILDING    adr2_building,
               A2.ADDR_OFFICE      adr2_office,
               A2.ADDR_CORP        adr2_corp,
               A2.ADDR_CODE_CITY   adr2_code_city,
               A2.ADDR_CODE_STREET adr2_code_street,
               C.CLIENT_TYPE,
               P.SGS_ID,
               0 /*PKG_SGS_LINK.GetBallancePClientFromSGS(l_ser,l_num)*/                   BALANCE, -- alla
               null                STATUS_PSTN,
               null                STATUS_GSM,
               null                KOL_GSM,
               null                ED_TARIFF_ID,
               a1.addr_block       adr1_addr_block,
               a1.addr_structure   adr1_addr_structure,
               a1.addr_fraction    adr1_addr_fraction,
               a2.addr_block       adr2_addr_block,
               a2.addr_structure   adr2_addr_structure,
               a2.addr_fraction    adr2_addr_fraction
          from T_CLIENTS   C,
               T_PERSON    P,
               T_ADDRESS   A1,
               T_ADDRESS   A2,
               T_DOCUMENTS D
         where C.FULLINFO_ID = P.PERSON_ID(+)
           and C.CLIENT_TYPE = c_client_person
           and D.DOC_TYPE = c_doc_type_passport
           and P.DOC_ID = D.DOC_ID(+)
           and P.ADDR_ID_REG = A1.ADDR_ID(+)
           and P.ADDR_ID_FACT = A2.ADDR_ID(+)
           and ( --(pi_par1 is null and pi_par2 is null) or
                d.doc_id in (select column_value from table(l_documents)))
           and ((not (pi_par1 is null)) or (not (pi_par2 is null)) or
               (not (pi_par3 is null)) or (not (pi_par4 is null)) or
               (not (pi_par5 is null)) or (not (pi_par6 is null)) or
               (not (pi_par7 is null)))
           and ( -- ((pi_all = 1) and
                ((pi_par1 is null) or (pi_par1 = D.DOC_SERIES)) and
                ((pi_par2 is null) or (pi_par2 = D.DOC_NUMBER)) and
                ((pi_par3 is null) or
                (lower(pi_par3) = lower(P.PERSON_FIRSTNAME))) and
                ((pi_par4 is null) or
                (lower(pi_par4) = lower(P.PERSON_LASTNAME))) and
                ((pi_par5 is null) or
                (lower(pi_par5) = lower(P.PERSON_MIDDLENAME))) and
                ((pi_par6 is null) or (lower(pi_par6) = lower(P.PERSON_INN))) and
                ((pi_par7 is null) or
                (lower(pi_par7) = lower(P.PERSON_PHONE))))
         order by CLIENT_ID asc;
    elsif (pi_cl_type = c_client_juristic) then
      open res for
        select C.CLIENT_ID,
               J.JURISTIC_ID,
               J.JUR_NAME,
               J.JUR_OGRN,
               J.JUR_EMAIL,
               J.JUR_SETTLEMENT_ACCOUNT,
               J.JUR_BANK_BIK,
               J.JUR_INN,
               J.JUR_KPP,
               J.JUR_OKPO,
               J.JUR_OKVED,
               J.JUR_CORR_AKK,
               J.JUR_THREASURY_NAME,
               J.JUR_THREASURY_ACC,
               A.ADDR_ID                adr_id,
               A.ADDR_COUNTRY           adr_country,
               A.ADDR_INDEX             adr_index,
               A.ADDR_CITY              adr_city,
               A.ADDR_STREET            adr_street,
               A.ADDR_BUILDING          adr_building,
               A.ADDR_OFFICE            adr_office,
               A.ADDR_CORP              adr_corp,
               A.ADDR_CODE_CITY         adr_code_city,
               A.ADDR_CODE_STREET       adr_code_street,
               A2.ADDR_ID               adr2_id,
               A2.ADDR_COUNTRY          adr2_country,
               A2.ADDR_INDEX            adr2_index,
               A2.ADDR_CITY             adr2_city,
               A2.ADDR_STREET           adr2_street,
               A2.ADDR_BUILDING         adr2_building,
               A2.ADDR_OFFICE           adr2_office,
               A2.ADDR_CORP             adr2_corp,
               A2.ADDR_CODE_CITY        adr2_code_city,
               A2.ADDR_CODE_STREET      adr2_code_street,
               RESP.PERSON_ID           resp_id,
               RESP.PERSON_PHONE        resp_phone,
               RESP.PERSON_EMAIL        resp_email,
               RESP.PERSON_LASTNAME     resp_lastname,
               RESP.PERSON_FIRSTNAME    resp_firstname,
               RESP.PERSON_MIDDLENAME   resp_middlename,
               RESP.PERSON_INN          resp_inn,
               RESP.PERSON_BIRTHDAY     resp_birthday,
               RESP.PERSON_SEX          resp_sex,
               D1.DOC_ID                resp_doc_id,
               D1.DOC_SERIES            resp_doc_series,
               D1.DOC_NUMBER            resp_doc_number,
               D1.DOC_REGDATE           resp_doc_date,
               D1.DOC_EXTRAINFO         resp_doc_info,
               D1.DOC_TYPE              resp_doc_type,
               TOUCH.PERSON_ID          touch_id,
               TOUCH.PERSON_PHONE       touch_phone,
               TOUCH.PERSON_EMAIL       touch_email,
               TOUCH.PERSON_LASTNAME    touch_lastname,
               TOUCH.PERSON_FIRSTNAME   touch_firstname,
               TOUCH.PERSON_MIDDLENAME  touch_middlename,
               TOUCH.PERSON_INN         touch_inn,
               TOUCH.PERSON_BIRTHDAY    touch_birthday,
               TOUCH.PERSON_SEX         touch_sex,
               D2.DOC_ID                touch_doc_id,
               D2.DOC_SERIES            touch_doc_series,
               D2.DOC_NUMBER            touch_doc_number,
               D2.DOC_REGDATE           touch_doc_date,
               D2.DOC_EXTRAINFO         touch_doc_info,
               D2.DOC_TYPE              touch_doc_type,
               BOSS.PERSON_ID           boss_id,
               BOSS.PERSON_PHONE        boss_phone,
               BOSS.PERSON_EMAIL        boss_email,
               BOSS.PERSON_LASTNAME     boss_lastname,
               BOSS.PERSON_FIRSTNAME    boss_firstname,
               BOSS.PERSON_MIDDLENAME   boss_middlename,
               BOSS.PERSON_INN          boss_inn,
               BOSS.PERSON_BIRTHDAY     boss_birthday,
               BOSS.PERSON_SEX          boss_sex,
               D3.DOC_ID                boss_doc_id,
               D3.DOC_SERIES            boss_doc_series,
               D3.DOC_NUMBER            boss_doc_number,
               D3.DOC_REGDATE           boss_doc_date,
               D3.DOC_EXTRAINFO         boss_doc_info,
               D3.DOC_TYPE              boss_doc_type,
               C.CLIENT_TYPE,
               J.SGS_ID,
               a.addr_block             adr_addr_block,
               a.addr_structure         adr_addr_structure,
               a.addr_fraction          adr_addr_fraction,
               a2.addr_block            adr2_addr_block,
               a2.addr_structure        adr2_addr_structure,
               a2.addr_fraction         adr2_addr_fraction
          from T_CLIENTS   C,
               T_JURISTIC  J,
               T_ADDRESS   A,
               T_ADDRESS   A2,
               T_PERSON    RESP,
               T_PERSON    TOUCH,
               T_PERSON    BOSS,
               T_DOCUMENTS D1,
               T_DOCUMENTS D2,
               T_DOCUMENTS D3
         where C.FULLINFO_ID = J.JURISTIC_ID
           and C.CLIENT_TYPE = c_client_juristic
           and A.ADDR_ID(+) = J.JUR_ADDRESS_ID
           and A2.ADDR_ID(+) = J.JUR_FACT_ADDRESS_ID
           and RESP.PERSON_ID(+) = J.JUR_RESP_ID
           and TOUCH.PERSON_ID(+) = J.JUR_TOUCH_ID
           and BOSS.PERSON_ID(+) = J.JUR_BOSS_ID
           and D1.DOC_ID(+) = RESP.DOC_ID
           and D2.DOC_ID(+) = TOUCH.DOC_ID
           and D3.DOC_ID(+) = BOSS.DOC_ID
           and ((not (pi_par1 is null)) or (not (pi_par2 is null)) or
               (not (pi_par3 is null)) or (not (pi_par4 is null)) or
               (not (pi_par5 is null)) or (not (pi_par6 is null)) or
               (not (pi_par7 is null)) or (not (pi_par8 is null)) or
               (not (pi_par9 is null)) or (not (pi_par10 is null)))
           and (((pi_all = 1) and
               ((pi_par1 is null) or (pi_par1 = J.JUR_OGRN)) and
               ((pi_par2 is null) or (lower(pi_par2) = lower(J.JUR_NAME))) and
               ((pi_par3 is null) or (pi_par3 = J.JUR_INN)) and
               ((pi_par4 is null) or
               (lower(pi_par4) = lower(RESP.PERSON_LASTNAME)) or
               (lower(pi_par4) = lower(TOUCH.PERSON_LASTNAME)) or
               (lower(pi_par4) = lower(BOSS.PERSON_LASTNAME))) and
               ((pi_par5 is null) or
               (lower(pi_par5) = lower(RESP.PERSON_FIRSTNAME)) or
               (lower(pi_par5) = lower(TOUCH.PERSON_FIRSTNAME)) or
               (lower(pi_par5) = lower(BOSS.PERSON_FIRSTNAME))) and
               ((pi_par6 is null) or
               (lower(pi_par6) = lower(RESP.PERSON_MIDDLENAME)) or
               (lower(pi_par6) = lower(TOUCH.PERSON_MIDDLENAME)) or
               (lower(pi_par6) = lower(BOSS.PERSON_MIDDLENAME))) and
               ((pi_par7 is null) or
               (lower(pi_par7) = lower(RESP.PERSON_PHONE)) or
               (lower(pi_par7) = lower(TOUCH.PERSON_PHONE)) or
               (lower(pi_par7) = lower(BOSS.PERSON_PHONE))) and
               ((pi_par8 is null) or
               (lower(pi_par8) = lower(RESP.PERSON_INN)) or
               (lower(pi_par8) = lower(TOUCH.PERSON_INN)) or
               (lower(pi_par8) = lower(BOSS.PERSON_INN))) and
               ((pi_par9 is null) or (pi_par9 = D1.DOC_SERIES) or
               (pi_par9 = D2.DOC_SERIES) or (pi_par9 = D3.DOC_SERIES)) and
               ((pi_par10 is null) or (pi_par10 = D1.DOC_NUMBER) or
               (pi_par10 = D2.DOC_NUMBER) or (pi_par10 = D3.DOC_NUMBER))) or
               ((pi_all = 0) and
               ((pi_par1 = J.JUR_OGRN) or
               (lower(pi_par2) = lower(J.JUR_NAME)) or
               (pi_par3 = J.JUR_INN) or
               (lower(pi_par4) = lower(RESP.PERSON_LASTNAME)) or
               (lower(pi_par4) = lower(TOUCH.PERSON_LASTNAME)) or
               (lower(pi_par4) = lower(BOSS.PERSON_LASTNAME)) or
               (lower(pi_par5) = lower(RESP.PERSON_FIRSTNAME)) or
               (lower(pi_par5) = lower(TOUCH.PERSON_FIRSTNAME)) or
               (lower(pi_par5) = lower(BOSS.PERSON_FIRSTNAME)) or
               (lower(pi_par6) = lower(RESP.PERSON_MIDDLENAME)) or
               (lower(pi_par6) = lower(TOUCH.PERSON_MIDDLENAME)) or
               (lower(pi_par6) = lower(BOSS.PERSON_MIDDLENAME)) or
               (lower(pi_par7) = lower(RESP.PERSON_PHONE)) or
               (lower(pi_par7) = lower(TOUCH.PERSON_PHONE)) or
               (lower(pi_par7) = lower(BOSS.PERSON_PHONE)) or
               (lower(pi_par8) = lower(RESP.PERSON_INN)) or
               (lower(pi_par8) = lower(TOUCH.PERSON_INN)) or
               (lower(pi_par8) = lower(BOSS.PERSON_INN)) or
               (pi_par9 = D1.DOC_SERIES) or (pi_par9 = D2.DOC_SERIES) or
               (pi_par9 = D3.DOC_SERIES) or (pi_par10 = D1.DOC_NUMBER) or
               (pi_par10 = D2.DOC_NUMBER) or (pi_par10 = D3.DOC_NUMBER))))
         order by CLIENT_ID asc;
    end if;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Client_List_test;
  -----------------------------------------------------------------------
  -- поиск пользователя по публичному ключу клиентского сертификата.
  -- Малышевский Задача № 32800
  -----------------------------------------------------------------------
  function get_user_by_cert(pi_cert      in varchar2,
                            pi_system_id in number,
                            po_system_id out number,
                            --                          po_cert         out sys_refcursor,
                            po_err_num out pls_integer,
                            po_err_msg out varchar2) return sys_refcursor is
    res        number;
    l_cert_str varchar2(4000) := pi_cert;
    l_tmp_str  t_user_details.pbk_sertificat%type;
    l_cert_tab string_tab := string_tab();
    l_sys_val  number := 0;
    l_role     sys_refcursor;
  begin
    if pi_cert is null then
      po_err_num := 1;
      po_err_msg := 'Не переданы ключи';
      return null;
    end if;
    -- Парсим строку
    loop
      l_tmp_str := substr(l_cert_str,
                          1,
                          (case
                            when instr(l_cert_str, ',') = 0 then
                             length(l_cert_str)
                            else
                             instr(l_cert_str, ',') - 1
                          end));
      l_cert_str := substr(l_cert_str,
                           (case
                             when instr(l_cert_str, ',') = 0 then
                              length(l_cert_str) + 1
                             else
                              instr(l_cert_str, ',') + 1
                           end));
      l_cert_tab.extend;
      l_cert_tab(l_cert_tab.count) := upper(trim(l_tmp_str));
      exit when l_cert_str is null;
    end loop;
    -- Ищем пользователя и систему
    select distinct ud.user_id, ud.system_id
      into res, po_system_id
      from t_user_details ud
     where upper(ud.pbk_sertificat) in
           (select column_value from table(l_cert_tab));

    select max(VALUE)
      into l_sys_val
      from T_SYSTEM_NEEDS
     where TYPE = decode(pi_system_id, 13, 13, 1);
    if (l_sys_val = 1) and res <> 958 and res <> 1008 and res <> 999 then
      po_err_num := 2;
      po_err_msg := 'Система работает в режиме ограничения доступа (обновление)';
      -- возвращаем пустой курсор
      return null;
    end if;
    return Get_User_By_Id2(res, 777, l_role, po_err_num, po_err_msg);
  exception
    when too_many_rows then
      po_err_num := 3;
      po_err_msg := 'Переданы ключи разных пользователей';
      return null;
    when no_data_found then
      po_err_num := 4;
      po_err_msg := 'Пользователь не найден';
      return null;
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  ---------------------------------------------------------------------------------------------
  -- 38664 Поиск прямых продавцов по организации
  ---------------------------------------------------------------------------------------------
  function Get_Direct_User_List_By_Org_Id(pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                                          pi_worker_id in T_USERS.USR_ID%type,
                                          po_err_num   out pls_integer,
                                          po_err_msg   out t_Err_Msg)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select distinct U.USR_ID,
                      PERSON_LASTNAME,
                      PERSON_FIRSTNAME,
                      PERSON_MIDDLENAME
        from T_USERS U, T_USER_ORG UO, T_PERSON P, t_roles t
       where UO.ORG_ID = pi_org_id
         and U.USR_ID = UO.USR_ID
         and USR_PERSON_ID = PERSON_ID
         and uo.role_id = t.role_id
         and t.role_name like '%Прямой продавец%'
       order by P.PERSON_LASTNAME asc, P.PERSON_FIRSTNAME asc;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Direct_User_List_By_Org_Id;
  ---------------------------------------------------------------------------------------------
  --проверка принадлежности воркера организации
  ---------------------------------------------------------------------------------------------
  function Check_Worker_In_Org(pi_org_id            in t_organizations.org_id%type,
                               pi_childrens_include in pls_integer := 1, -- включать детей
                               pi_user_id           in t_users.usr_id%type,
                               pi_worker_id         in T_USERS.USR_ID%type,
                               po_err_num           out pls_integer,
                               po_err_msg           out varchar2)
    return number is
    res       number;
    l_org_tab num_tab := num_tab();
    l_org_id  number;
  begin
    --кастыль из за прописанной константы на вэбе
    if pi_org_id = 2007250 then
      l_org_id := 2007269;
    else
      l_org_id := pi_org_id;
    end if;
    if pi_childrens_include = 1 then
      select tor.org_id bulk collect
        into l_org_tab
        from t_org_relations tor
      connect by prior tor.org_id = tor.org_pid
       start with tor.org_id = l_org_id;
    else
      l_org_tab := num_tab(l_org_id);
    end if;
    select count(distinct t.org_id)
      into res
      from t_user_org t
     where t.org_id in (select * from table(l_org_tab))
       and t.usr_id = pi_user_id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  ---------------------------------------------------------------------------------------------
  --получение данных справочника jur_billing_group
  ---------------------------------------------------------------------------------------------
  function get_jur_billing_group(po_err_num out pls_integer,
                                 po_err_msg out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select t.jbg_id, t.jbg_name from jur_billing_group t;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  
  ---------------------------------------------------------------------------------------------
  -- Получение данных справочника типов подтверждающих документов V_DOC_TYPES
  ---------------------------------------------------------------------------------------------
  function get_v_doc_types(po_err_num out pls_integer,
                           po_err_msg out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select t.V_DOC_TYPE_ID, t.V_DOC_TYPE_NAME from V_DOC_TYPES t;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end get_v_doc_types;
  
  ---------------------------------------------------------------------------------------------
  -- 51553 Надо функцию, которая бы по воркеру возвращала, все ли выводить или только GSM
  --- 0- без ограничения
  --- 1- ограниченный функционал
  ---------------------------------------------------------------------------------------------
  function Check_Limit_for_User(pi_user_id in number,
                                po_err_num out number,
                                po_err_msg out varchar2) return number is
    res       number;
    l_is_rtm  number;
    l_is_ecov number;
    l_is_tm   number;
  begin
    select min(s.ch), min(is_rtm)
      into res, l_is_rtm
      from (select 0 ch, rt.is_org_rtm is_rtm
              from t_users u
              join t_user_org uo
                on uo.usr_id = u.usr_id
              join t_organizations org
                on org.org_id = uo.org_id
              left join t_dic_mrf m
                on m.org_id = uo.org_id
              left join t_org_is_rtmob rt
                on rt.org_id = org.org_id
             where u.usr_id = pi_user_id) s;
    -- 82398
    select count(*)
      into l_is_ecov
      from t_org_ignore_right i, t_user_org uo
     where i.org_id = uo.org_id
       and i.type_org = 1
       and uo.usr_id = pi_user_id;
    --для доставки
    select count(*)
      into l_is_tm
      from t_user_org uo
      join t_org_relations r
        on r.org_id = uo.org_id
       and r.org_reltype = '1008'
     where uo.usr_id = pi_user_id;

    if (l_is_rtm > 0) then
      return 2;
    elsif l_is_ecov > 0 then
      return 3; -- без ограничений
    elsif l_is_tm > 0 then
      return 4; --телемаркетинг
    else
      return res;
    end if;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  ---------------------------------------------------------------------------------------------
  -- 52203 по пользователю определить, является ли он оператором или агентом
  ---------------------------------------------------------------------------------------------
  function Check_Is_Operator_Worker(pi_worker_id in number,
                                    po_is_head   out number,
                                    po_mrf       OUT SYS_REFCURSOR,
                                    po_err_num   out number,
                                    po_err_msg   out varchar2) return number is
    l_check     number;
    l_is_up_mrf number; -- Признак, что организация выше МРФ
  begin
    select count(*)
      into l_is_up_mrf
      from t_user_org t
     where t.usr_id = pi_worker_id
       and t.org_id in (0, 1, 2);

    select count(*)
      into po_is_head
      from t_user_org t
     where t.usr_id = pi_worker_id
       and t.org_id in (select org_id from T_ORG_ABONENT_IGNOR)/*(0, 2, 2008917, 2009091)*/;

    open po_mrf for
      select distinct m.id, m.name_mrf
        from t_dic_mrf m, t_dic_region dr, t_dic_region_info dri
       where (l_is_up_mrf > 0 or
             (m.org_id in
             (select t.org_id
                  from t_org_relations t
                Connect by prior t.org_pid = t.org_id
                 Start with t.org_id in
                            (select distinct uo.org_id
                               from t_user_org uo
                              where uo.usr_id = pi_worker_id)) or
             dri.rtmob_org_id in
             (select t.org_id
                  from t_org_relations t
                Connect by prior t.org_pid = t.org_id
                 Start with t.org_id in
                            (select distinct uo.org_id
                               from t_user_org uo
                              where uo.usr_id = pi_worker_id))))
         and m.id = dr.mrf_id
         and dri.reg_id = dr.reg_id;

    for ora_1 in (select distinct uo.org_id
                    from t_user_org uo
                   where uo.usr_id = pi_worker_id) loop
      l_check := is_org_usi(ora_1.org_id);
      if l_check = 1 then
        return l_check;
      end if;
    end loop;
    return 0;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Check_Is_Operator_Worker;

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
                                po_err_msg    out varchar2) is
    l_last_name   t_person.person_lastname%type;
    l_first_name  t_person.person_firstname%type;
    l_middle_name t_person.person_middlename%type;
    l_person_id   t_person.person_id%type;
    l_max_hist_id number;
  begin
    logging_pkg.debug(pi_email, 'Change_User');
    savepoint sp_Change_User_himself;
    po_err_num := 0;
    po_err_msg := '';

    select p.person_lastname, p.person_firstname, p.person_middlename
      into l_last_name, l_first_name, l_middle_name
      from t_person p
      join t_users u
        on u.usr_person_id = p.person_id
     where u.usr_id = pi_user_id;

    l_person_id := Ins_Person(pi_first_name            => l_first_name,
                              pi_middle_name           => l_middle_name,
                              pi_last_name             => l_last_name,
                              pi_sex                   => null,
                              pi_birth_day             => null,
                              pi_inn                   => null,
                              pi_email                 => pi_email,
                              pi_phone                 => pi_person_phone,
                              pi_reg_country           => null,
                              pi_reg_city              => null,
                              pi_reg_post_index        => null,
                              pi_reg_street            => null,
                              pi_reg_house             => null,
                              pi_reg_corp              => null,
                              pi_reg_flat              => null,
                              pi_reg_code_city         => null,
                              pi_reg_code_street       => null,
                              pi_fact_country          => null,
                              pi_fact_city             => null,
                              pi_fact_post_index       => null,
                              pi_fact_street           => null,
                              pi_fact_house            => null,
                              pi_fact_corp             => null,
                              pi_fact_flat             => null,
                              pi_fact_code_city        => null,
                              pi_fact_code_street      => null,
                              pi_passport_ser          => null,
                              pi_passport_num          => null,
                              pi_passport_receive_date => null,
                              pi_passport_receive_who  => null,
                              pi_sgs_id                => null,
                              pi_birthplace            => null,
                              pi_reg_region            => null,
                              pi_fact_region           => null,
                              pi_dlv_acc_type          => null,
                              pi_HOME_PHONE            => null,
                              pi_reg_city_lvl          => null,
                              pi_reg_street_lvl        => null,
                              pi_reg_house_code        => null,
                              pi_fact_city_lvl         => null,
                              pi_fact_street_lvl       => null,
                              pi_fact_house_code       => null,
                              pi_passport_type         => null,
                              pi_reg_addr_obj_id       => null,
                              pi_reg_house_obj_id      => null,
                              pi_fact_addr_obj_id      => null,
                              pi_fact_house_obj_id     => null);
    select nvl(max(t.hist_id), 0)
      into l_max_hist_id
      from t_users_hist t
     where t.user_id = pi_user_id;
    if (pi_password_md5 is not null) then
      update T_USERS U
         set U.USR_PWD_MD5    = pi_password_md5,
             u.worker_id      = pi_user_id,
             u.tz_id          = NVL(pi_tz_id, 19),
             u.date_pswd_to   = case
                                  when pi_password_md5 <> u.usr_pwd_md5 then
                                   add_months(trunc(sysdate - 2 / 24), 2)
                                  else
                                   u.date_pswd_to
                                end,
             u.is_temp_passwd =
             (case
               when pi_password_md5 <> u.usr_pwd_md5 then
                0
               else
                u.is_temp_passwd
             end),
             u.count_attempts = 0,
             u.usr_person_id  = l_person_id
       where U.USR_ID = pi_user_id;

    Else
      update T_USERS U
         set u.tz_id         = NVL(pi_tz_id, 19),
             u.worker_id     = pi_user_id,
             u.usr_person_id = l_person_id
       where U.USR_ID = pi_user_id;
    end if;

    update t_users_hist t
       set t.ip_address = pi_ip_address
     where t.user_id = pi_user_id
       and t.hist_id > l_max_hist_id;

    update t_sessions s
       set s.cache_date = sysdate
     where s.id_worker = pi_user_id;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'USERS.Change_User_himself');
      rollback to sp_Change_User_himself;
  end;
  --------------------------------------------------
  -- 57518 (57179) Функция, которая по воркеру возвращает список регионов, в которых он зарегистрирован
  --------------------------------------------------
  function Get_Region_by_Worker(pi_worker_id in number,
                                po_err_num   out number,
                                po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    if pi_worker_id is null then
      -- Достаем все регионы, в которых есть абонка
      open res for
        with tab as
         (select tree.org_id
            from mv_org_tree tree, t_dogovor dog, t_dogovor_prm dp
           where tree.root_rel_id = dog.org_rel_id
             and dog.dog_id = dp.dp_dog_id
             and dp.dp_prm_id = 4000)
        select distinct r.reg_id,
                        r.kl_name || ' ' || r.kl_socr reg_name,
                        r.mrf_id
          from tab
          join t_organizations org
            on org.org_id = tab.org_id
          join t_dic_region r
            on org.region_id = r.reg_id
         where r.is_mrf_region = 1
         order by r.reg_id;
    else
      open res for
        with tab as
         (select tor.org_id
            from t_org_relations tor
          connect by prior tor.org_id = tor.org_pid
           start with tor.org_id in
                      (select t.org_id
                         from t_user_org t
                        where t.usr_id = pi_worker_id))
        select distinct r.reg_id,
                        r.kl_name || ' ' || r.kl_socr reg_name,
                        r.mrf_id
          from tab
          join t_organizations org
            on org.org_id = tab.org_id
          join t_dic_region r
            on org.region_id = r.reg_id
         where r.is_mrf_region = 1
         order by r.reg_id;
    end if;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'USERS.Get_Region_by_Worker');
      return null;
  end Get_Region_by_Worker;
  --------------------------------------------------
  --57416 получение истории изменений пользователя
  --------------------------------------------------
  function Get_User_hist(pi_user_id   in number,
                         pi_worker_id in number,
                         po_err_num   out number,
                         po_err_msg   out varchar2) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select t.hist_id,
             t.user_id,
             t.act_id,
             dic.act_name,
             t.order_num,
             case
               when t.os_user is null or
                    t.os_user in ('EISSD', 'MPZ', 'IKESHPD') then
                t.worker_id
               else
                null
             end worker_id,
             case
               when t.os_user is null or
                    t.os_user in ('EISSD', 'MPZ', 'IKESHPD') then
                p.person_lastname || ' ' || p.person_firstname || ' ' ||
                p.person_middlename
               else
                t.os_user
             end worker_name,
             t.os_user,
             t.mod_date - 2 / 24 mod_date,
             u.usr_login,
             t.ip_address
        from t_users_hist t
        join t_dic_users_act dic
          on dic.act_id = t.act_id
        left join t_users u
          on u.usr_id = t.worker_id
        left join t_person p
          on p.person_id = u.usr_person_id
       where t.user_id = pi_user_id
       order by t.mod_date, t.hist_id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'USERS.Get_User_hist');
      return null;
  end;
  ----------------------------------------------------------------------------------------
  -- Принадлежность воркера организации
  ----------------------------------------------------------------------------------------
  function is_worker_in_org(pi_org_id    in t_organizations.org_id%type,
                            pi_worker_id in T_USERS.USR_ID%type,
                            po_err_num   out pls_integer,
                            po_err_msg   out varchar2) return number is
    res       number;
    l_org_tab num_tab := num_tab();
  begin
    l_org_tab := ORGS.Get_User_Orgs_Tab_With_Param1(pi_worker_id,
                                                    null,
                                                    1,
                                                    1,
                                                    1,
                                                    1,
                                                    0);

    select count(*)
      into res
      from table(l_org_tab)
     where column_value = nvl(pi_org_id, 0);
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end is_worker_in_org;
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
                              po_err_msg    out varchar2) is
    ex_fk_constraint exception;
    pragma exception_init(ex_fk_constraint, -02291);
  begin
    savepoint sp_save_channel_user;
    logging_pkg.debug(pi_worker_id || ',' || pi_channel_id,
                      'users.save_channel_user');
    --83124
    delete from t_user_params p
     where p.user_id = pi_worker_id
       and p.key = 'CHANNEL_DEFAULT';

    insert into t_user_params p
      (p.user_id, p.KEY, p.VALUE)
    values
      (pi_worker_id, 'CHANNEL_DEFAULT', pi_channel_id);

    insert into t_user_params_hst
      (user_id, KEY, VALUE)
      select p.user_id, p.KEY, p.VALUE
        from t_user_params p
       where p.user_id = pi_worker_id;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error('po_err_num=' || po_err_num || ',po_err_msg=' ||
                        po_err_msg,
                        'users.save_channel_user');
      rollback to sp_save_channel_user;
      return;
  end;
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
                            po_err_msg   out varchar2) is
  begin
    delete from t_user_params p
     where p.user_id = pi_worker_id
       and p.key not in ('CHANNEL_DEFAULT',
                         'THEME_DEFAULT',
                         'AGENT_EQUIPMENT_DEFAULT',
                         'RETURN_WAREHOUSE');

    insert into t_user_params p
      (p.user_id, p.KEY, p.VALUE)
      select pi_worker_id, par.key, par.value from table(pi_params) par;

    insert into t_user_params_hst
      (user_id, KEY, VALUE)
      select p.user_id, p.KEY, p.VALUE
        from t_user_params p
       where p.user_id = pi_worker_id;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error('po_err_num=' || po_err_num || ',po_err_msg=' ||
                        po_err_msg,
                        'users.save_param_user');
      return;
  end;

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
    return T_USERS.USR_ID%type is
    right_to_edit     BOOLEAN := false;
    is_change_himself BOOLEAN;
  begin
    savepoint sp_change_user_passwd;
    is_change_himself := (pi_worker_id = pi_user_id);
    right_to_edit := Security_pkg.Check_Rights((case
                                                 when (is_org_usi(pi_org_id) > 0 or
                                                      is_user_usi(pi_user_id) > 0) then
                                                  5309
                                                 else
                                                  5304
                                               end),
                                               pi_org_id,
                                               pi_worker_id,
                                               po_err_num,
                                               po_err_msg,
                                               null,
                                               null);

    if (is_change_himself) then
      po_err_num := 0;
      po_err_msg := '';
    end if;

    -- checking access for operation for specified user
    if ((not right_to_edit) and (not is_change_himself)) then
      return null;
    end if;

    -- Запись в историю «Восстановление пароля через web-интерфейс»
    if pi_change_type = 2 then
      insert into t_users_hist
        (hist_id,
         user_id,
         MOD_DATE,
         act_id,
         worker_id,
         ip_address,
         salt,
         hash_alg_id)
      values
        (seq_t_users_hist.nextval,
         pi_user_id,
         sysdate,
         8,
         pi_user_id,
         pi_ip_address,
         pi_salt,
         NVL(pi_hash_alg_id, 1));
    end if;

    insert into t_users_hist
      (hist_id,
       user_id,
       MOD_DATE,
       act_id,
       worker_id,
       ip_address,
       salt,
       hash_alg_id)
    values
      (seq_t_users_hist.nextval,
       pi_user_id,
       sysdate,
       4,
       pi_user_id,
       pi_ip_address,
       pi_salt,
       pi_hash_alg_id);

    update T_USERS U
       set U.USR_PWD_MD5    = pi_password_md5,
           u.worker_id      = pi_worker_id,
           u.date_pswd_to = case
                              when pi_password_md5 <> u.usr_pwd_md5 then
                               add_months(trunc(sysdate - 2 / 24), 2)
                              else
                               u.date_pswd_to
                            end,
           u.is_temp_passwd = (case
                                when pi_worker_id = pi_user_id and
                                     pi_password_md5 <> u.usr_pwd_md5 then
                                 0
                                when pi_worker_id <> pi_user_id and
                                     pi_password_md5 <> u.usr_pwd_md5 then
                                 1
                                else
                                 u.is_temp_passwd
                              end),
           u.count_attempts = 0,
           u.SALT           = pi_salt,
           u.HASH_ALG_ID    = NVL(pi_hash_alg_id, 1)
     where U.USR_ID = pi_user_id;

    return pi_user_id;
  exception
    when no_data_found then
      return pi_user_id;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      rollback to sp_change_user_passwd;
      return pi_user_id;
  end change_user_passwd;
  --------------------------------------------------
  --Создание или редактирование пользователей списком
  --------------------------------------------------
  function change_user_tab(pi_worker_id in number,
                           pi_user_info in user_tab,
                           po_err_num   out number,
                           po_err_msg   out varchar2) return user_res_tab is
    res                     user_res_tab := user_res_tab();
  --  l_system                number;
    l_fio_dover             VARCHAR2(200);
    l_position_dover        VARCHAR2(200);
    l_na_osnovanii_dover    VARCHAR2(200);
    l_address_dover         VARCHAR2(200);
    l_na_osnovanii_doc_type VARCHAR2(200);
    L_fio_dover_nominative  VARCHAR2(200);
    l_count_su              number;
    l_count_sa              number;
    l_old_count_su          number;
    l_old_count_sa          number;
    l_su_id                 number;
    l_err_num               number;
    l_err_msg               varchar2(4000);
  begin
    for inf in (select * from table(pi_user_info)) loop
      res.extend;
      res(res.count) := user_res_type(null, null, null, null);

      if not is_user_editable(pi_worker_id, inf.user_id) then
        po_err_num := 1;
        po_err_msg := 'Невозможно отредактировать пользователя другой структуры';
        return null;
      end if;

      if inf.user_id is null then
        res(res.count).usr_login := inf.login;

       /* if inf.system is null then
          select sum(case
                       when rr.right_string_id like 'MPZ.%' then
                        1
                       else
                        -1
                     end)
            into l_system
            from t_roles r
            join t_roles_perm rp
              on r.role_id = rp.rp_role_id
            join t_perm_rights pr
              on pr.pr_prm_id = rp.rp_perm_id
            join t_rights rr
              on rr.right_id = pr.pr_right_id
           where r.role_id in (select column_value from table(inf.roles))
             and rr.right_string_id is not null;

          if nvl(l_system, 0) > 0 then
            l_system := 13;
          else
            l_system := 0;
          end if;
        end if;*/
        res(res.count).usr_id := Add_User(pi_org_id                => nvl(inf.org_id,
                                                                          0),
                                          pi_login                 => inf.login,
                                          pi_password_md5          => inf.password_md5,
                                          pi_firstname             => inf.firstname,
                                          pi_middlename            => inf.middlename,
                                          pi_lastname              => inf.lastname,
                                          pi_email                 => inf.email,
                                          pi_roles                 => inf.roles,
                                          pi_worker_id             => pi_worker_id,
                                          pi_qualifications        => inf.qualifications,
                                          pi_scheds                => inf.scheds,
                                          pi_system                => nvl(inf.system,
                                                                          13),
                                          pi_person_phone          => inf.person_phone,
                                          pi_date_login_to         => inf.date_login_to,
                                          pi_order_num             => inf.order_num,
                                          pi_fio_dover             => inf.fio_dover,
                                          pi_position_dover        => inf.position_dover,
                                          pi_na_osnovanii_dover    => inf.na_osnovanii_dover,
                                          pi_address_dover         => inf.address_dover,
                                          pi_na_osnovanii_doc_type => inf.na_osnovanii_doc_type,
                                          pi_fio_dover_nominative  => inf.fio_dover_nominative,
                                          pi_boss_email            => inf.boss_email,
                                          pi_ip_address            => inf.ip_address,
                                          pi_salt                  => inf.salt,
                                          pi_hash_alg_id           => inf.hash_alg_id,
                                          pi_employee_number       => inf.employee_number,
                                          po_err_num               => res(res.count)
                                                                      .err_num,
                                          po_err_msg               => res(res.count)
                                                                      .err_msg);
        if nvl(res(res.count).err_num, 0) = 0 then
          --проверим есть ли роль супервайзера
          select count(r.role_id)
            into l_count_su
            from t_roles r
            join t_user_org uo
              on r.role_id = uo.role_id
            join t_system_parameters p
              on p.value = r.role_parent_id
           where p.sys_key = 'SUPERVISOR'
             and uo.usr_id = res(res.count).usr_id;

          if l_count_su > 0 then
            --есть роль супервайзера - создаем супервайзера
            l_su_id := supervizor.regSupervisor(pi_su_id        => null,
                                                pi_last_name    => inf.lastname,
                                                pi_first_name   => inf.firstname,
                                                pi_middle_name  => inf.middlename,
                                                pi_person_phone => inf.person_phone,
                                                pi_org_id       => inf.org_id,
                                                pi_user_id      => res(res.count)
                                                                   .usr_id,
                                                pi_worker_id    => pi_worker_id,
                                                po_err_num      => l_err_num,
                                                po_err_msg      => l_err_msg);

            if nvl(l_err_num, 0) <> 0 then
              res(res.count).err_num := l_err_num;
              res(res.count).err_msg := l_err_msg;
            end if;
          end if;
        end if;
      else
        select u.fio_dover,
               u.position_dover,
               u.na_osnovanii_dover,
               u.address_dover,
               u.na_osnovanii_doc_type,
               u.fio_dover_nominative
          into l_fio_dover,
               l_position_dover,
               l_na_osnovanii_dover,
               l_address_dover,
               l_na_osnovanii_doc_type,
               L_fio_dover_nominative
          from t_users u
         where u.usr_id = inf.user_id;

        select nvl(sum(case
                         when p.sys_key = 'SUPERVISOR' then
                          1
                         else
                          0
                       end),
                   0),
               nvl(sum(case
                         when p.sys_key = 'SELLERACTIVE' then
                          1
                         else
                          0
                       end),
                   0)
          into l_old_count_su, l_old_count_sa
          from t_roles r
          join t_user_org uo
            on r.role_id = uo.role_id
          join t_system_parameters p
            on p.value = r.role_parent_id
         where (p.sys_key = 'SUPERVISOR' or p.sys_key = 'SELLERACTIVE')
           and uo.usr_id = inf.user_id;

        res(res.count).usr_login := inf.login;
        res(res.count).usr_id := Change_User(pi_user_id               => inf.user_id,
                                             pi_login                 => inf.login,
                                             pi_password_md5          => inf.password_md5,
                                             pi_firstname             => inf.firstname,
                                             pi_middlename            => inf.middlename,
                                             pi_lastname              => inf.lastname,
                                             pi_email                 => inf.email,
                                             pi_roles                 => inf.roles,
                                             pi_org_id                => nvl(inf.org_id,
                                                                             0),
                                             pi_worker_id             => pi_worker_id,
                                             pi_qualifications        => inf.qualifications,
                                             pi_scheds                => inf.scheds,
                                             pi_person_phone          => inf.person_phone,
                                             pi_tz_id                 => inf.tz_id,
                                             pi_date_login_to         => inf.date_login_to,
                                             pi_order_num             => inf.order_num,
                                             pi_fio_dover             => nvl(inf.fio_dover,
                                                                             l_fio_dover),
                                             pi_position_dover        => nvl(inf.position_dover,
                                                                             l_position_dover),
                                             pi_na_osnovanii_dover    => nvl(inf.na_osnovanii_dover,
                                                                             l_na_osnovanii_dover),
                                             pi_address_dover         => nvl(inf.address_dover,
                                                                             l_address_dover),
                                             pi_na_osnovanii_doc_type => nvl(inf.na_osnovanii_doc_type,
                                                                             l_na_osnovanii_doc_type),
                                             pi_fio_dover_nominative  => nvl(inf.fio_dover_nominative,
                                                                             L_fio_dover_nominative),
                                             pi_boss_email            => inf.boss_email,
                                             pi_ip_address            => inf.ip_address,
                                             pi_salt                  => inf.salt,
                                             pi_hash_alg_id           => NVL(inf.hash_alg_id,
                                                                             1),
                                             pi_employee_number       => inf.employee_number,
                                             po_err_num               => res(res.count)
                                                                         .err_num,
                                             po_err_msg               => res(res.count)
                                                                         .err_msg);
        if nvl(res(res.count).err_num, 0) = 0 then
          select nvl(sum(case
                           when p.sys_key = 'SUPERVISOR' then
                            1
                           else
                            0
                         end),
                     0),
                 nvl(sum(case
                           when p.sys_key = 'SELLERACTIVE' then
                            1
                           else
                            0
                         end),
                     0)
            into l_count_su, l_count_sa
            from t_roles r
            join t_user_org uo
              on r.role_id = uo.role_id
            join t_system_parameters p
              on p.value = r.role_parent_id
           where (p.sys_key = 'SUPERVISOR' or p.sys_key = 'SELLERACTIVE')
             and uo.usr_id = inf.user_id;
          if l_old_count_su = 0 and l_count_su > 0 then
            --проверим есть ли связанный супервайзер
            begin
              select su.su_id
                into l_su_id
                from t_supervisor su
               where su.su_user_id = inf.user_id;

              l_su_id := supervizor.blockSupervisor(pi_su_id     => l_su_id,
                                                    pi_op_id     => 1,
                                                    pi_worker_id => pi_worker_id,
                                                    po_err_num   => l_err_num,
                                                    po_err_msg   => l_err_msg);
            exception
              when no_data_found then
                l_su_id := supervizor.regSupervisor(pi_su_id        => null,
                                                    pi_last_name    => inf.lastname,
                                                    pi_first_name   => inf.firstname,
                                                    pi_middle_name  => inf.middlename,
                                                    pi_person_phone => inf.person_phone,
                                                    pi_org_id       => inf.org_id,
                                                    pi_user_id      => res(res.count)
                                                                       .usr_id,
                                                    pi_worker_id    => pi_worker_id,
                                                    po_err_num      => l_err_num,
                                                    po_err_msg      => l_err_msg);
            end;
            if nvl(l_err_num, 0) <> 0 then
              res(res.count).err_num := l_err_num;
              res(res.count).err_msg := l_err_msg;
            end if;
          elsif l_old_count_su > 0 and l_count_su = 0 then
            begin
              select su.su_id
                into l_su_id
                from t_supervisor su
               where su.su_user_id = inf.user_id;

              l_su_id := supervizor.blockSupervisor(pi_su_id     => l_su_id,
                                                    pi_op_id     => 0,
                                                    pi_worker_id => pi_worker_id,
                                                    po_err_num   => l_err_num,
                                                    po_err_msg   => l_err_msg);
            exception
              when no_data_found then
                null;
            end;
          end if;
          if l_old_count_sa > 0 and l_count_sa = 0 then
            update t_seller_active sa
               set sa.sa_user_id = null
             where sa.sa_user_id = inf.user_id;
          end if;
        end if;
      end if;
    end loop;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  -------------------------------------------------
  --Массовая блокировка/разблокировка пользователей
  -------------------------------------------------
  function block_user_tab(pi_worker_id       in number,
                          pi_user_block_info in rec_num_3_str_1_tab,
                          -- 70937
                          pi_ip_address in t_users_hist.ip_address%type,
                          po_err_num    out number,
                          po_err_msg    out varchar2) return user_res_tab is
    res user_res_tab := user_res_tab();
  begin

    for inf in (select number_1 usr_id, number_2 is_block, str_1 order_num
                  from table(pi_user_block_info)) loop

      if not is_user_editable(pi_worker_id, inf.usr_id) then
        po_err_num := 1;
        po_err_msg := 'Невозможно заблокировать пользователя другой структуры';
        return null;
      end if;

      res.extend;
      res(res.count) := user_res_type(null, null, null, null);
      res(res.count).usr_id := inf.usr_id;
      Block_User(pi_usr_id     => inf.usr_id,
                 pi_block      => inf.is_block,
                 pi_order_num  => inf.order_num,
                 pi_ip_address => pi_ip_address,
                 pi_worker_id  => pi_worker_id,
                 po_err_num    => res(res.count).err_num,
                 po_err_msg    => res(res.count).err_msg);
    end loop;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  -------------------------------------------------------
  --определение общей организации пользователей
  -------------------------------------------------------
  function get_general_org_by_users(pi_usrers  in num_tab,
                                    po_err_num out number,
                                    po_err_msg out varchar2)
    return sys_refcursor is
    res     sys_refcursor;
    l_count number;
  begin
    select count(*) into l_count from table(pi_usrers);

    open res for
      select distinct bb.org_id,
                      bb.org_name,
                      case
                        when r.org_reltype is not null then
                         1
                        else
                         0
                      end is_gph,
                      rt.is_org_rtm structure
        from (select aa.org_id, o.org_name
                from (select distinct uo.usr_id, uo.org_id
                        from t_user_org uo
                       where uo.usr_id in
                             (select column_value from table(pi_usrers))) aa
                join t_organizations o
                  on aa.org_id = o.org_id
               group by aa.org_id, o.org_name
              having count(aa.usr_id) = l_count) bb
        left join t_org_relations r
          on r.org_id = bb.org_id
         and r.org_reltype = 1007
        left join t_org_is_rtmob rt
          on rt.org_id = bb.org_id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
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
                                 pi_house_obj_id   in T_ADDRESS_HOUSE_OBJ.ID%type,
                                 po_err_num        out number,
                                 po_err_msg        out varchar2)
    return sys_refcursor is
    l_city      T_ADDRESS.ADDR_CITY%type;
    l_street    T_ADDRESS.ADDR_STREET%type;
    l_city_code T_ADDRESS.ADDR_CODE_CITY%type;
    l_city_lvl  t_address.addr_city_lvl%type;
    l_region    T_ADDRESS.REGION_ID%type;
    l_addr_id   number;
    res         sys_refcursor;
    l_err       number;
  begin

    if pi_kl_region is not null then
      SELECT dr.reg_id
        INTO l_region
        FROM T_DIC_REGION dr
       WHERE dr.KL_REGION = pi_kl_region;
    end if;

    if pi_street is null or pi_city is null and pi_addr_obj_id is not null then
      begin
        select ao.a_name, ao_p.a_name
          into l_street, l_city
          from T_ADDRESS_OBJECT ao
          join T_ADDRESS_OBJECT ao_p
            on ao.PARENT_ID = ao_p.id
           and nvl(ao_p.is_deleted, 0) = 0
         where ao.kl_region = pi_kl_region
           and ao.id = pi_addr_obj_id
           and nvl(ao.is_deleted, 0) = 0
           and ao.IS_STREET = 1;
      exception
        when no_data_found then
          BEGIN
            select ao.a_name
              into l_city
              from T_ADDRESS_OBJECT ao
             where ao.kl_region = pi_kl_region
               and ao.id = pi_addr_obj_id
               and nvl(ao.is_deleted, 0) = 0;
          exception
            when no_data_found then
              l_err := 1;
          END;
      end;
    end if;

    l_addr_id := addresse_pkg.Ins_Address(pi_country        => pi_country,
                                         pi_city           => nvl(pi_city,
                                                                  l_city),
                                         pi_index          => pi_index,
                                         pi_street         => nvl(pi_street,
                                                                  l_street),
                                         pi_building       => pi_building,
                                         pi_office         => pi_office,
                                         pi_corp           => pi_corp,
                                         pi_city_code      => nvl(pi_city_code,
                                                                  l_city_code),
                                         pi_city_lvl       => nvl(pi_city_lvl,
                                                                  l_city_lvl),
                                         pi_street_code    => pi_street_code,
                                         pi_street_lvl     => pi_street_lvl,
                                         pi_house_code     => pi_house_code,
                                         pi_addr_Oth       => null,
                                         pi_region         => l_region,
                                         pi_addr_block     => pi_addr_block,
                                         pi_addr_structure => pi_addr_structure,
                                         pi_addr_fraction  => pi_addr_fraction,
                                         pi_addr_obj_id    => pi_addr_obj_id,
                                         pi_house_obj_id   => pi_house_obj_id);

    open res for
      select a.* from t_address a where a.addr_id = l_addr_id;
    if l_err = 1 then
      po_err_num := 1;
      po_err_msg := 'Адрес выбран не из справочника';
    end if;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  -------------------------------------------------------------
  --Определение регионов пользователя
  -------------------------------------------------------------
  function get_region_by_worker_right2(pi_worker_id in number,
                                       pi_right_str in string_tab,
                                       pi_org_id    in num_tab)
    return ARRAY_NUM_2 is
  begin
    return security_pkg.get_region_by_worker_right2(pi_worker_id,
                                                    pi_right_str,
                                                    pi_org_id);
  end;
  -------------------------------------------------------------
  --по воркеру определяет принадлежность его (организаций в каких у него есть роли) к Ростелекому или РТ-Мобайлу
  -------------------------------------------------------------
  function Get_User_belonging(pi_worker_id in number,
                              po_err_num   out number,
                              po_err_msg   out varchar2) return sys_refcursor is
  begin
    return security_pkg.Get_User_belonging(pi_worker_id,
                                           po_err_num,
                                           po_err_msg);
  end;
  ----------------------------------------------------------------
  -- получение структур по пользователю
  ----------------------------------------------------------------
  procedure get_user_structures(pi_worker_id in number,
                                po_is_rtk    out boolean,
                                po_is_rtm    out boolean,
                                po_err_num   out number,
                                po_err_msg   out varchar2) is
  begin
    security_pkg.get_user_structures(pi_worker_id,
                                     po_is_rtk,
                                     po_is_rtm,
                                     po_err_num,
                                     po_err_msg);
  
  end;
  -------------------------------------------------------------

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
    return sys_refcursor as
    res sys_refcursor;
  begin
    open res for
      select distinct tree.org_id, level
        from mv_org_tree tree
      connect by prior tree.org_id = tree.org_pid
       start with tree.org_id in
                  (select distinct tr.org_id
                     from t_user_org      t,
                          t_organizations o,
                          mv_org_tree     tr,
                          t_dogovor       td,
                          t_dogovor_prm   dp
                    where t.org_id = o.org_id
                      and o.org_id = tr.org_id
                      and tr.id = td.org_rel_id
                      and td.dog_id = dp.dp_dog_id
                      and td.is_enabled = 1
                      and td.dog_id = dp.dp_dog_id
                      and dp.dp_prm_id in (select * from table(pi_dog_perm))
                      and t.usr_id = pi_user_id)
       order by level;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end getOrgByUserAndPerm;

  -----------------------------------------------------------------------------
  -- 70937 Функция сохранения ссылки для смены пароля
  -----------------------------------------------------------------------------
  procedure saveLinkChangePasswd(pi_usr_login          in t_users.usr_login%type,
                                 pi_link_change_passwd in t_users.link_change_passwd%type,
                                 pi_worker_id          in t_users.usr_id%type,
                                 po_err_num            out pls_integer,
                                 po_err_msg            out t_Err_Msg) is
  begin
    savepoint sp_saveLinkChangePasswd;
    update t_users t
       set t.link_change_passwd      = pi_link_change_passwd,
           t.date_link_change_passwd = sysdate
     where t.usr_login = pi_usr_login;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      rollback to sp_saveLinkChangePasswd;
  end saveLinkChangePasswd;

  -----------------------------------------------------------------------------
  -- 70937 Функция получения юзера по ссылке для смены пароля
  -----------------------------------------------------------------------------
  function getUserByLinkChangePasswd(pi_link_change_passwd in t_users.link_change_passwd%type,
                                     pi_worker_id          in t_users.usr_id%type,
                                     po_err_num            out pls_integer,
                                     po_err_msg            out t_Err_Msg)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select t.usr_id,
             t.usr_login,
             (case
               when sysdate - t.date_link_change_passwd > 1 then
                0
               else
                1
             end) is_actual
        from t_users t
       where t.link_change_passwd = pi_link_change_passwd;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end getUserByLinkChangePasswd;

  ----------------------------------------------------------------------------------------
  -- Принадлежность воркера организации (для мультивыбора)
  ----------------------------------------------------------------------------------------
  function is_worker_in_org1(pi_org_id    in num_tab,
                             pi_worker_id in T_USERS.USR_ID%type,
                             po_err_num   out pls_integer,
                             po_err_msg   out varchar2) return number is
    res        number;
    l_org_tab  num_tab := num_tab();
    l_user_tab num_tab := num_tab();
  begin
    l_user_tab := ORGS.Get_User_Orgs_Tab_With_Param1(pi_worker_id,
                                                     null,
                                                     1,
                                                     1,
                                                     1,
                                                     1,
                                                     0);
    select * bulk collect
      into l_org_tab
      from (select column_value
              from table(l_user_tab)
            intersect
            select column_value from table(pi_org_id));

    if l_org_tab.count < pi_org_id.count then
      res := 0;
    else
      res := 1;
    end if;
    return res;

  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end is_worker_in_org1;
  ----------------------------------------------------------------------------------------
  ----------------------------------------------------------------------------------------
  function Get_Regions_User(pi_worker_id in number) return num_tab is
    res num_tab;
  begin
    return orgs.Get_Regions_User(pi_worker_id);
  end;
  ----------------------------------------------------------------------------------------
  function Get_Dog_Perm_by_Worker_Id(pi_worker_id in number)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select dp.dp_prm_id
        from t_user_org uo
        join t_org_relations r
          on r.org_id = uo.org_id
        join t_dogovor d
          on d.org_rel_id = r.id
         and d.is_enabled = 1
        join t_dogovor_prm dp
          on dp.dp_dog_id = d.dog_id
         and dp.dp_is_enabled = 1
       where uo.usr_id = pi_worker_id;
    return res;
  end; 
  --------------------------------------------------------------------------------------
  -- Получение регионов пользователя по праву (со списком прав)
  --------------------------------------------------------------------------------------
 function get_region_by_worker_right(pi_worker_id in number,
                                     pi_right     in string_tab,
                                     po_err_num   out number,
                                     po_err_msg   out varchar2)
   return string_tab is
   l_func_name varchar2(100) := 'users.get_region_by_worker_right';
 begin
   return get_region_by_worker_RightOrg(pi_worker_id => pi_worker_id,
                                        pi_right     => pi_right,
                                        pi_orgs      => null,
                                        po_err_num   => po_err_num,
                                        po_err_msg   => po_err_msg);
 exception
   when others then
     po_err_num := sqlcode;
     po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
     logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
     return null;
 end;

  ---------------------------------------------------------------------------------------
  -- Список регионов пользователя для MVNO
  ---------------------------------------------------------------------------------------
  function getMVNORegionByWorker(pi_worker_id in number,
                                 po_err_num   out number,
                                 po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      with tab as
       (select tor.org_id
          from t_org_relations tor
        connect by prior tor.org_id = tor.org_pid
               AND tor.org_reltype <> 1009
         start with tor.org_id in
                    (select t.org_id
                       from t_user_org t
                      where t.usr_id = pi_worker_id))
      select distinct r.id,
                      r.name reg_name,
                      r.reg_id,
                      dr.kl_region,
                      dr.mrf_id
        from tab
        join t_organizations org
          on org.org_id = tab.org_id
        join t_dic_region dr
          on dr.reg_id = org.region_id
        join t_dic_mvno_region r
          on dr.reg_id = r.reg_id
       order by r.name;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'USERS.getMVNORegionByWorker');
      return null;
  end getMVNORegionByWorker;
  --------------------------------------------------------------------------------------
  -- Получение регионов пользователя по списку прав с учетом организаций
  --------------------------------------------------------------------------------------
  function get_region_by_worker_RightOrg(pi_worker_id in number,
                                         pi_right     in string_tab,
                                         pi_orgs      in num_tab,
                                         po_err_num   out number,
                                         po_err_msg   out varchar2)
    return string_tab is
    l_func_name varchar2(100) := 'users.get_region_by_worker_RightOrg';
    res         string_tab;
    l_user_orgs Num_Tab;
    l_cnt       number;
    l_user_reg string_tab;
    l_org_reg  string_tab;

  begin
    --получим регионы пользователя

    -- проверим ЕЦОВ
    select count(*)
      into l_cnt
      from t_user_org t
      join t_roles_perm rp
        on rp.rp_role_id = t.role_id
      join t_perm_rights pr
        on pr.pr_prm_id = rp.rp_perm_id
      join t_rights rr
        on rr.right_id = pr.pr_right_id
     where t.usr_id = pi_worker_id
       and rr.right_string_id in (select * from table(pi_right))
       and t.org_id in
           (select org_id from t_org_ignore_right where TYPE_ORG = 1);
    if l_cnt > 0 then
      --если ецов
      select distinct dr.kl_region bulk collect
        into l_user_reg
        from t_dic_region dr
        join t_dic_region_data d
          on d.reg_id = dr.reg_id
         and d.visible = 1
       where dr.kl_region not in ('94', '95', '91', '92', '99')
         and dr.mrf_id = 7
       order by dr.kl_region;
    else
      -- Получаем все организации пользователя, в которых есть эти права
      select uo.org_id bulk collect
        into l_user_orgs
        from t_user_org uo
        join t_roles_perm rp
          on rp.rp_role_id = uo.role_id
        join t_perm_rights pr
          on pr.pr_prm_id = rp.rp_perm_id
        join t_rights rr
          on rr.right_id = pr.pr_right_id
       where uo.usr_id = pi_worker_id
         and rr.right_string_id in (select * from table(pi_right));

      with tabs as
       (select *
          from (select tor.org_id,
                       min(level) OVER(partition by connect_by_root tor.org_id) lvlo,
                       level lvl,
                       r.org_id reg_org_id
                  from t_org_relations tor
                  join t_organizations o
                    on o.org_id = tor.org_id
                  left join t_dic_mrf m
                    on m.org_id = tor.org_id
                  left join t_dic_region r
                    on r.org_id = tor.org_id
                  left join dual d
                    on tor.org_id in (0, 1, 2)
                 where nvl(r.reg_id,
                           nvl(m.id, decode(d.dummy, null, null, 1))) is not null
                connect by prior tor.org_pid = tor.org_id
                 start with tor.org_id in (select * from table(l_user_orgs))) tt
         where (lvlo = lvl or reg_org_id =org_id))
      select distinct t.kl_region bulk collect
        into l_user_reg
        from t_dic_region t
        join (select tor.org_id
                from t_org_relations tor
              connect by prior tor.org_id = tor.org_pid
               start with tor.org_id in (select org_id from tabs)) tab
          on tab.org_id = t.org_id
       where t.kl_region not in ('94', '95', '91', '92', '99')
       order by t.kl_region;
    end if;
    ------------------------

    if pi_orgs is null or pi_orgs is empty then
      return l_user_reg;
    else
      --получим регионы от переданных организаций
      with tabs as
       (select *
          from (select tor.org_id,
                       min(level) OVER(partition by connect_by_root tor.org_id) lvlo,
                       level lvl
                  from t_org_relations tor
                  join t_organizations o
                    on o.org_id = tor.org_id
                  left join t_dic_mrf m
                    on m.org_id = tor.org_id
                  left join t_dic_region r
                    on r.org_id = tor.org_id
                  left join dual d
                    on tor.org_id in (0, 1, 2)
                 where nvl(r.reg_id,
                           nvl(m.id, decode(d.dummy, null, null, 1))) is not null
                connect by prior tor.org_pid = tor.org_id
                 start with tor.org_id in (select * from table(pi_orgs))) tt
         where lvlo = lvl)
      select distinct t.kl_region bulk collect
        into l_org_reg
        from t_dic_region t
        join (select tor.org_id
                from t_org_relations tor
              connect by prior tor.org_id = tor.org_pid
               start with tor.org_id in (select org_id from tabs)) tab
          on tab.org_id = t.org_id;

      select distinct r.kl_region bulk collect
        into res
        from t_dic_region r
        join t_dic_region_data d
          on d.reg_id = r.reg_id
         and d.visible = 1
        join table(l_user_reg) u
          on u.column_value = r.kl_region
        join table(l_org_reg) o
          on o.column_value = r.kl_region
       where r.kl_region not in ('94', '95', '91', '92', '99')
       order by r.kl_region;

      return res;
    end if;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;

  --Получение списка пользователей, которых пользователь может выбрать при создании заявки
  function Get_List_User_by_users(pi_org_id    t_organizations.org_id%type,
                                  pi_rights    in string_tab,
                                  pi_worker_id in T_USERS.USR_ID%type,
                                  po_err_num   out number,
                                  po_err_msg   out varchar2)
    return sys_refcursor is
    res         sys_refcursor;
    l_func_name varchar2(100) := 'USERS.Get_List_User_by_users';
  begin

    open res for
      select distinct tuo.usr_id,
                      person_lastname,
                      person_firstname,
                      person_middlename,
                      tu.usr_login
        from t_user_org TUO
        join t_users TU
          on tuo.usr_id = tu.usr_id
        join t_person TP
          on tu.usr_person_id = tp.person_id
        join table(pi_rights) t
          on 1 = 1
        join t_rights r
          on r.right_string_id = t.column_value
       where tuo.org_id = pi_org_id
         --and tu.is_enabled = 0 --117188 убрала ограничение на заблокированные 
         and security_pkg.Check_Rights_Number(r.right_id,
                                          tuo.org_id,
                                          tuo.usr_id) = 1
       order by lower(person_lastname),
                lower(person_firstname),
                lower(person_middlename);
    return res;
  exception
    when others then
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      po_err_num := sqlcode;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;

  --------------------------------------------------------
  --Определение организации пользователя по умолчанию в регионе
  ---------------------------------------------------------
   function get_default_org_by_region(pi_worker_id in number,
                                     pi_kl_region in varchar2,
                                     po_err_num   out number,
                                     po_err_msg   out varchar2)
    return sys_refcursor is
    res          sys_refcursor;
    l_res_org_id number;
    l_func_name  varchar2(100) := 'USERS.get_default_org_by_region';
    l_org_user   number;
    l_org_reg    number;
  begin
    begin
      select nvl(org_id, 0)
        into l_org_reg
        from t_dic_region r
       where r.kl_region = pi_kl_region;
    exception
      when others then
        po_err_num:=1;
        po_err_msg:='Некорректный код региона';
        return null;
    end;

    begin
      select p.org_id
        into l_org_user
        from t_users p
       where p.usr_id = pi_worker_id;
    exception
      when others then
        po_err_num:=1;
        po_err_msg:='Пользователь не найден';
        return null;
    end;
    begin
      select org_id
        into l_res_org_id
        from (select org_right.org_id,
                     lvl,
                     row_number() over(order by lvl,usr_lvl) rn
                from (select distinct o.org_id, level usr_lvl
                        from MV_ORG_TREE T
                        join T_ORGANIZATIONS O
                          on T.ORG_ID = O.ORG_ID
                         and o.is_enabled <> 0
                       START WITH T.ORG_ID = l_org_user
                      cONNECT BY PRIOR t.ORG_ID = t.ORG_PID
                             AND t.ORG_RELTYPE IN (1001)) org_right
                join (select distinct o.org_id, level lvl
                       from MV_ORG_TREE T
                       join T_ORGANIZATIONS O
                         on T.ORG_ID = O.ORG_ID
                        and T.ORG_RELTYPE IN
                            (1001, 1004, 1006, 1007, 1005, 1008, 1009, 999)
                        and o.is_enabled <> 0
                      START WITH T.ORG_ID = l_org_reg
                     cONNECT BY PRIOR t.ORG_ID = t.ORG_PID
                            AND t.ORG_RELTYPE IN (1001,
                                                  1004,
                                                  1006,
                                                  1007,
                                                  1005,
                                                  1008,
                                                  1009,
                                                  999)) org_reg
                  on org_right.org_id = org_reg.org_id)
       where rn = 1;
    exception
      when others then
        l_res_org_id:=null;
    end;

    open res for
      select o.org_id, o.org_name
        from t_organizations o
       where o.org_id = l_res_org_id;

    return res;
  exception
    when others then
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      po_err_num := sqlcode;
      logging_pkg.error(pi_worker_id||';'||pi_kl_region||';'|| po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;

  ----------------------------------------------------------------------------------------
  -- Сохранение склада по умолчанию для пользователя
  -- %param pi_worker_id Иднетификатор пользователя
  -- %param pi_agent_id Идентификатор агента выдачи (t_lira_agent_equipment)
  -- %param pi_return_warehouse склад возврата по умолчанию
  -- %param po_err_num Код ошибки
  -- %param po_err_msg Сообщение об ошибке
  ----------------------------------------------------------------------------------------
  procedure save_agent_equipment_user(pi_worker_id        in number,
                                      pi_agent_id         in number,
                                      pi_return_warehouse in number,
                                      po_err_num          out number,
                                      po_err_msg          out varchar2) is
    ex_fk_constraint exception;
    pragma exception_init(ex_fk_constraint, -02291);
  begin
    savepoint sp_save_agent_equipment_user;
    logging_pkg.info(pi_worker_id || ',' || pi_agent_id,
                     'users.save_agent_equipment_user');
  
    delete from t_user_params p
     where p.user_id = pi_worker_id
       and p.key in ('AGENT_EQUIPMENT_DEFAULT', 'RETURN_WAREHOUSE');
  
    if pi_agent_id is not null then
      insert into t_user_params p
        (p.user_id, p.KEY, p.VALUE)
      values
        (pi_worker_id, 'AGENT_EQUIPMENT_DEFAULT', pi_agent_id);
    end if;
  
    if pi_return_warehouse is not null then
      insert into t_user_params p
        (p.user_id, p.KEY, p.VALUE)
      values
        (pi_worker_id, 'RETURN_WAREHOUSE', pi_return_warehouse);
    end if;
  
    insert into t_user_params_hst
      (user_id, KEY, VALUE)
      select p.user_id, p.KEY, p.VALUE
        from t_user_params p
       where p.user_id = pi_worker_id
         and p.key in ('AGENT_EQUIPMENT_DEFAULT', 'RETURN_WAREHOUSE');
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error('po_err_num=' || po_err_num || ',po_err_msg=' ||
                        po_err_msg,
                        'users.save_agent_equipment_user');
      rollback to sp_save_agent_equipment_user;
      return;
  end;

end USERS;
/
