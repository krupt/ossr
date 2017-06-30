CREATE OR REPLACE package body user_pkg is

  ----------------------------------------------------
  --Информация о пользователе для профиля
  --%param pi_user_id идентификатор пользователя 
  --%param pi_worker_id идентификатор пользователя, запрашивающего информацию
  --%param po_roles список доступных ролей пользователя
  --%param po_param доп. параметры пользователя
  --%param po_err_num код ошибки
  --%param po_err_msg сообщение об ошибке
  ----------------------------------------------------
  function get_user_profile(pi_user_id   in number,
                            pi_worker_id in number,
                            po_roles     out sys_refcursor,
                            po_param     out sys_refcursor,
                            po_err_num   out number,
                            po_err_msg   out varchar2) return sys_refcursor is
    res sys_refcursor;
    PU_NAME constant varchar2(100) := '.get_user_profile';
    is_change_himself BOOLEAN;
    countLdapUsers    number;
    --l_param           REQUEST_PARAM_TAB;
    right_to_edit BOOLEAN := false;
  begin
    po_err_num := 0;
    po_err_msg := '';
  
    is_change_himself := (pi_worker_id = pi_user_id);
    select count(*)
      into countLdapUsers
      from T_LDAPUSERS
     where usr_id = pi_user_id;
    if (not is_change_himself) then
      right_to_edit := Security_pkg.Check_Rights_Orgs_str('EISSD.WORKER.VIEW',
                                                          Get_User_Orgs_Tab(pi_user_id),
                                                          pi_worker_id,
                                                          po_err_num,
                                                          po_err_msg,
                                                          true,
                                                          false);
    
      if (not right_to_edit) then
        po_err_num := 1;
        po_err_msg := 'У Вас отсутствуют права на просмотр данного пользователя.';
        return null;
      end if;
    end if;
  
    open po_roles for
      select distinct O.ORG_ID,
                      O.ORG_NAME,
                      ro.org_name filial_name,
                      R.ROLE_ID,
                      R.ROLE_NAME,
                      r.is_privilege
        from t_user_org uo
        join t_roles r
          on r.role_id = uo.role_id
        join T_ORGANIZATIONS O
          on UO.ORG_ID = O.ORG_ID
        left join t_dic_region dr
          on dr.reg_id = o.region_id
        left join t_organizations ro
          on ro.org_id = dr.org_id
       where uo.usr_id = pi_user_id
       order by O.ORG_NAME, R.ROLE_NAME;
  
    open po_param for
      select par.key, par.value
        from t_user_params par
       where par.user_id = pi_user_id
         and par.key != 'CHANNEL_DEFAULT'
      union
      select 'CHANNEL_DEFAULT',
             to_char(nvl(d_ch.channel_id, d_org_ch.channel_id))
        from t_users u
        left join t_user_params ch
          on u.usr_id = ch.user_id
         and ch.key = 'CHANNEL_DEFAULT'
        left join t_dic_channels d_ch
          on d_ch.channel_id = ch.value
         and sysdate between nvl(d_ch.date_start, sysdate) and
             nvl(d_ch.date_end, sysdate)
        left join t_org_channels org_ch
          on org_ch.org_id = u.org_id
         and org_ch.default_type = 1
        left join t_dic_channels d_org_ch
          on d_org_ch.channel_id = org_ch.channel_id
         and sysdate between nvl(d_org_ch.date_start, sysdate) and
             nvl(d_org_ch.date_end, sysdate)
       where u.usr_id = pi_user_id
         and nvl(d_ch.channel_id, d_org_ch.channel_id) is not null;
  
    open res for
      select U.USR_ID,
             USR_LOGIN,
             p.PERSON_LASTNAME,
             p.PERSON_FIRSTNAME,
             p.PERSON_MIDDLENAME,
             p.PERSON_EMAIL,
             USR_STATUS,
             U.IS_ENABLED,
             u.org_id,
             p.PERSON_PHONE,
             u.date_login_to,
             u.date_pswd_to,
             u.is_temp_passwd,
             u.system_id,
             u.tz_id,
             sa.sa_id,
             su.su_id,
             U.FIO_DOVER,
             U.POSITION_DOVER,
             U.NA_OSNOVANII_DOVER,
             U.ADDRESS_DOVER,
             U.NA_OSNOVANII_DOC_TYPE,
             u.fio_dover_nominative,
             u.boss_email,
             psa.person_lastname || ' ' || psa.person_firstname || ' ' ||
             psa.person_middlename sa_fio,
             psu.person_lastname || ' ' || psu.person_firstname || ' ' ||
             psu.person_middlename su_fio,
             l.user_principal_name,
             case
               WHEN countLdapUsers > 0 THEN
                0
               WHEN u.date_pswd_to + 2 / 24 < sysdate OR
                    u.is_temp_passwd = 1 THEN
                1
               ELSE
                0
             end need_pwd_change,
             u.employee_number
        from T_USERS U
        left join T_PERSON p
          on u.USR_PERSON_ID = p.PERSON_ID
        left join t_seller_active sa
          on sa.sa_user_id = u.usr_id
         and sa.sa_is_block = 0
        left join t_supervisor su
          on su.su_user_id = u.usr_id
         and su.su_is_block = 0
        left join t_person psa
          on psa.person_id = sa.sa_person_id
        left join t_person psu
          on psu.person_id = su.su_person_id
        left join t_ldapusers l
          on l.usr_id = u.usr_id
       where U.USR_ID = pi_user_id;
  
    return res;
  
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        PKG_NAME || PU_NAME);
      return null;
  end;

  ---------------------------------------------------------------------------------------------
  -- Функция для изменения пользователем своего профиля
  --%param pi_user_id  Идентификатор пользователя
  --%param pi_old_password хэш старого пароля
  --%param pi_new_password кэш нового пароля
  --%param pi_SALT  salt
  --%param pi_HASH_ALG_ID алгоритм расчета хэша
  --%param pi_email емайл
  --%param pi_person_phone контактный телефон
  --%param pi_tz_id таймзона
  --%param pi_params параметры
  --%param pi_org_id организация по умолчанию
  --%param pi_ip_address ip адрес с которого производятся изменения
  --%param po_err_num код ошибки
  --%param po_err_msg сообщение об ошибке
  ---------------------------------------------------------------------------------------------
  procedure Change_User_himself(pi_user_id      in T_USER_ORG.USR_ID%type,
                                pi_old_password in T_USERS.USR_PWD_MD5%type,
                                pi_new_password in T_USERS.USR_PWD_MD5%type,
                                pi_SALT         in t_users.salt%type,
                                pi_HASH_ALG_ID  in t_users.hash_alg_id%type,
                                pi_email        in T_PERSON.PERSON_EMAIL%type,
                                pi_person_phone in t_person.person_phone%type, -- телефон
                                pi_tz_id        in t_users.tz_id%type,
                                pi_params       in REQ_PARAM_TAB,
                                pi_org_id       in number,
                                pi_ip_address   in t_users_hist.ip_address%type,
                                po_err_num      out number,
                                po_err_msg      out varchar2) is
    l_last_name   t_person.person_lastname%type;
    l_first_name  t_person.person_firstname%type;
    l_middle_name t_person.person_middlename%type;
    l_person_id   t_person.person_id%type;
    l_max_hist_id number;
    ex_fk_constraint exception;
    pragma exception_init(ex_fk_constraint, -02291);
    PU_NAME constant varchar2(100) := '.Change_User_himself';
    l_old_pass t_users.usr_pwd_md5%type;
    ex_err_org exception;
    l_check number;
  begin
    savepoint sp_Change_User_himself;
  
    po_err_num := 0;
    po_err_msg := '';
  
    --проверка на орг-цию
    if pi_org_id is not null then
      select count(*)
        into l_check
        from t_org_relations tor
       where tor.org_id = pi_org_id
      connect by prior tor.org_id = tor.org_pid
       start with tor.org_id in
                  (select uo.org_id
                     from t_user_org uo
                    where uo.usr_id = pi_user_id);
      if l_check = 0 then
        raise ex_err_org;
      end if;
    end if;
  
    --редактирование параметров пользователя
    if pi_params is not null then
      delete from t_user_params p where p.user_id = pi_user_id;
    
      insert into t_user_params p
        (p.user_id, p.KEY, p.VALUE)
        select pi_user_id, par.key, par.value
          from table(pi_params) par
         where par.VALUE is not null;
    
      insert into t_user_params_hst
        (user_id, KEY, VALUE)
        select p.user_id, p.KEY, p.VALUE
          from t_user_params p
         where p.user_id = pi_user_id;
    
      insert into t_users_hist
        (hist_id, user_id, act_id, order_num, worker_id, os_user, mod_date)
      values
        (Seq_t_Users_Hist.nextval,
         pi_user_id,
         9, --изменение парамс
         null,
         pi_user_id,
         upper(sys_context('USERENV', 'OS_USER')),
         sysdate);
    end if;
  
    --редактирование общих данных
    select p.person_lastname,
           p.person_firstname,
           p.person_middlename,
           u.usr_pwd_md5
      into l_last_name, l_first_name, l_middle_name, l_old_pass
      from t_person p
      join t_users u
        on u.usr_person_id = p.person_id
     where u.usr_id = pi_user_id;
  
    l_person_id := users.Ins_Person(pi_first_name            => l_first_name,
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
  
    if (pi_new_password is not null) then
      --проверим старый пароль
      if upper(l_old_pass) <> upper(nvl(pi_old_password, '-1')) then
        po_err_num := 2;
        po_err_msg := 'Неверный текущий пароль';
        return;
      end if;
    
      if pi_new_password != pi_old_password then
        --20150616 89447
        insert into t_users_hist h
          (hist_id,
           user_id,
           MOD_DATE,
           act_id,
           worker_id,
           ip_address,
           salt,
           hash_alg_id,
           USR_PWD_MD5,
           OLD_PWD_MD5)
        values
          (seq_t_users_hist.nextval,
           pi_user_id,
           sysdate,
           4,
           pi_user_id,
           pi_ip_address,
           pi_salt,
           pi_hash_alg_id,
           pi_new_password,
           pi_old_password);
      end if;
    
      update T_USERS U
         set U.USR_PWD_MD5    = pi_new_password,
             u.worker_id      = pi_user_id,
             u.tz_id          = NVL(nvl(pi_tz_id, u.tz_id), 19),
             u.date_pswd_to = case
                                when pi_new_password <> u.usr_pwd_md5 then
                                 add_months(trunc(sysdate - 2 / 24), 2)
                                else
                                 u.date_pswd_to
                              end,
             u.is_temp_passwd = (case
                                  when pi_new_password <> u.usr_pwd_md5 then
                                   0
                                  else
                                   u.is_temp_passwd
                                end),
             u.count_attempts = 0,
             u.usr_person_id  = l_person_id,
             u.salt           = pi_SALT,
             u.hash_alg_id    = nvl(pi_HASH_ALG_ID, 1),
             u.org_id         = nvl(pi_org_id, u.org_id)
       where U.USR_ID = pi_user_id;
    
    Else
      update T_USERS U
         set u.tz_id         = NVL(pi_tz_id, 19),
             u.worker_id     = pi_user_id,
             u.usr_person_id = l_person_id,
             u.org_id        = nvl(pi_org_id, u.org_id)
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
    when no_data_found then
      po_err_num := 1;
      po_err_msg := 'Некорректный идентификатор пользователя';
      rollback to sp_Change_User_himself;
      return;
    when ex_err_org then
      po_err_num := 2;
      po_err_msg := 'Некорректно выбрана организация по умолчанию';
      rollback to sp_Change_User_himself;
      return;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        PKG_NAME || PU_NAME);
      rollback to sp_Change_User_himself;
      return;
  end;

  -------------------------------------------------
  --Редактирование данных доверенности пользователя
  --%param pi_user_id идентификатор пользователя
  --%param pi_FIO_DOVER ФИО в родительном падеже
  --%param pi_POSITION_DOVER Должность в родительном падеже
  --%param pi_NA_OSNOVANII_DOVER № документа
  --%param pi_ADDRESS_DOVER  Адрес в доверенности
  --%param pi_NA_OSNOVANII_DOC_TYPE На основании (документ) в родительном падеже
  --%param pi_fio_dover_nominative ФИО в именительном падеже
  --%param po_err_num код ошибки
  --%param po_err_msg сообщение об ошибке
  ---------------------------------------------------
  procedure Change_User_dover(pi_user_id               in T_USER_ORG.USR_ID%type,
                              pi_FIO_DOVER             in varchar2,
                              pi_POSITION_DOVER        in varchar2,
                              pi_NA_OSNOVANII_DOVER    in varchar2,
                              pi_ADDRESS_DOVER         in varchar2,
                              pi_NA_OSNOVANII_DOC_TYPE in varchar2,
                              pi_fio_dover_nominative  in varchar2,
                              pi_ip_address            in t_users_hist.ip_address%type,
                              po_err_num               out number,
                              po_err_msg               out varchar2) is
    PU_NAME constant varchar2(100) := '.Change_User_dover';
    l_max_hist_id number;
  begin
    select nvl(max(t.hist_id), 0)
      into l_max_hist_id
      from t_users_hist t
     where t.user_id = pi_user_id;
  
    update T_USERS U
       set u.worker_id             = pi_user_id,
           u.fio_dover             = pi_FIO_DOVER,
           u.position_dover        = pi_POSITION_DOVER,
           u.na_osnovanii_dover    = pi_NA_OSNOVANII_DOVER,
           u.address_dover         = pi_ADDRESS_DOVER,
           u.na_osnovanii_doc_type = pi_NA_OSNOVANII_DOC_TYPE,
           u.fio_dover_nominative  = pi_fio_dover_nominative
     where U.USR_ID = pi_user_id;
  
    update t_users_hist t
       set t.ip_address = pi_ip_address
     where t.user_id = pi_user_id
       and t.hist_id > l_max_hist_id;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        PKG_NAME || PU_NAME);
      return;
  end;
  --------------------------------------------------
  -- получение истории изменений пользователя
  --------------------------------------------------
  function Get_User_hist(pi_user_id in number,
                         po_err_num out number,
                         po_err_msg out varchar2) return sys_refcursor is
    res sys_refcursor;
    PU_NAME constant varchar2(100) := '.Get_User_hist';
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
                    t.os_user in ('EISSD', 'MPZ', 'IKESHPD', 'HAMITOV') then
                p.person_lastname || ' ' || p.person_firstname || ' ' ||
                p.person_middlename
               else
                t.os_user
             end worker_name,
             t.os_user,
             t.mod_date,
             u.usr_login,
             t.ip_address
        from t_users_hist t
        join t_dic_users_act dic
          on dic.act_id = t.act_id
        join t_users u
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
                        PKG_NAME || PU_NAME);
      return null;
  end;

  --------------------------
  --Построние дерева организаций
  ----------------------------
  function get_tree_level(pi_worker_id    in number,
                          pi_start_org_id in number,
                          po_err_num      out number,
                          po_err_msg      out varchar2,
                          pi_show_blocked NUMBER := 0) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      SELECT T.ORG_ID,
             T.ORG_PID,
             T.ORG_NAME,
             T.HAS_CHILD,
             T.KL_REGION,
             T.ORG_RELTYPE
        FROM (select O.ORG_ID,
                     T.ORG_PID,
                     O.ORG_NAME,
                     R.KL_REGION,
                     T.ORG_RELTYPE,
                     SIGN(COUNT(TC.ORG_ID)) HAS_CHILD,
                     (select COUNT(*)
                        from MV_ORG_TREE M1
                       WHERE M1.ORG_ID = O.ORG_ID
                       START WITH M1.ORG_ID IN
                                  (SELECT ORG_ID
                                     from t_user_org
                                    WHERE usr_id = pi_worker_id)
                              and m1.org_reltype != 1009
                      CONNECT BY PRIOR M1.ORG_PID = M1.ORG_ID
                             AND M1.ORG_RELTYPE IN
                                 (1001, 1002, 1004, 1006, 1007, 1008, 999)) C_PARENT,
                     (select COUNT(*)
                        from MV_ORG_TREE M1
                       WHERE M1.ORG_ID in
                             (SELECT ORG_ID
                                from t_user_org
                               WHERE usr_id = pi_worker_id)
                       START WITH M1.ORG_ID = O.ORG_ID
                              and m1.org_reltype != 1009
                      CONNECT BY PRIOR M1.ORG_PID = M1.ORG_ID
                             AND M1.ORG_RELTYPE IN
                                 (1001, 1002, 1004, 1006, 1007, 1008, 999)) C_CHILD
                from MV_ORG_TREE T
                join T_ORGANIZATIONS O
                  on T.ORG_ID = O.ORG_ID
                 and T.ORG_RELTYPE IN (1001, 1002, 1004, 1006, 1007, 1008, 999)
                 and o.is_enabled <> 0
                left join t_dogovor dog
                  on dog.org_rel_id = t.id
                left join MV_ORG_TREE TC
                  on T.ORG_ID = TC.ORG_PID
                 and TC.ORG_RELTYPE IN (1001, 1002, 1004, 1006, 1007, 1008, 999)
                left join T_ORGANIZATIONS OC
                  on TC.ORG_ID = OC.ORG_ID
                 and oc.is_enabled <> 0
                left join t_dic_region r
                  on r.reg_id = o.region_id
                left join t_dic_mrf m
                  on m.org_id = o.org_id
               where T.ORG_PID = pi_start_org_id
                 and (NVL(pi_show_blocked, 0) = 1 OR NVL(dog.is_enabled, 1) <> 0)
               GROUP BY O.ORG_ID,
                        T.ORG_PID,
                        O.ORG_NAME,
                        T.ORG_RELTYPE,
                        R.KL_REGION,
                        m.id,
                        T.ORG_RELTYPE
               order by decode(m.id, null, 1, 0), T.ORG_RELTYPE, O.ORG_NAME) T
       WHERE C_PARENT > 0
          or C_CHILD > 0;
  
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'user_pkg.get_tree_level');
      return null;
  end;

  --------------------------------------------------------
  --Список email для которых разрешена отправка писем с востановлением пароля
  --------------------------------------------------------
  function get_email_for_restoge_passw(po_err_num out number,
                                       po_err_msg out varchar2)
    return sys_refcursor is
    PU_NAME constant varchar2(100) := '.get_email_for_restoge_passw';
    res sys_refcursor;
  begin
    open res for
      select a.email_address from T_EMAIL_ADDRESSES a;
  
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        PKG_NAME || PU_NAME);
      return null;
  end;

  ---------------------------------------------------------
  --Установка ссылки на востановление пароля
  --%param pi_login Логин пользователя
  --%param pi_remote_ip ip адрес с которого производится запрос
  --%param pi_code Ссылка для востановления пароля
  --%param po_err_num код ошибки
  --%param po_err_msg сообщение об ошибке
  ---------------------------------------------------------
  function set_restore_passw_code(pi_login     in varchar2,
                                  pi_remote_ip in varchar2,
                                  pi_code      in varchar2,
                                  po_err_num   out number,
                                  po_err_msg   out varchar2) return number is
    PU_NAME constant varchar2(100) := '.set_restore_passw_code';
    l_usr_id              number;
    l_count_change_passwd number;
    l_is_enabled          number;
    l_boss_email          t_users.boss_email%type;
    l_person_email        t_person.person_email%type;
  begin
    savepoint sp_set_code;
    po_err_num := 0;
  
    -- Поиск пользователя
    begin
      select t.usr_id,
             t.count_change_passwd,
             t.is_enabled,
             t.boss_email,
             p.person_email
        into l_usr_id,
             l_count_change_passwd,
             l_is_enabled,
             l_boss_email,
             l_person_email
        from t_users t, t_person p
       where t.usr_login = lower(pi_login)
         and t.usr_person_id = p.person_id;
    exception
      when no_data_found then
        po_err_num := 1;
        po_err_msg := 'Неверно указано имя пользователя';
        return null;
    end;
    -- Запись в историю «Запрос на восстановление пароля через web-интерфейс»
    insert into t_users_hist
      (hist_id,
       user_id,
       USR_LOGIN,
       MOD_DATE,
       act_id,
       order_num,
       worker_id,
       ip_address)
    values
      (seq_t_users_hist.nextval,
       l_usr_id,
       lower(pi_login),
       sysdate,
       7,
       null,
       l_usr_id,
       pi_remote_ip);
  
    -- Проверка количества попыток смены пароля
    if l_count_change_passwd >= 3 then
      po_err_num := 2;
      po_err_msg := 'Превышен лимит запросов на восстановление пароля. Повторите попытку завтра или обратитесь в службу технической поддержки.';
      return null;
    end if;
    -- Проверка на блокировку пользователя
    if l_is_enabled = 0 then
      po_err_num := 3;
      po_err_msg := 'Ваша учетная запись заблокирована. Для разблокировки необходимо обратиться в службу технической поддержки.';
      return null;
    end if;
    -- Проверка на заполненность email
    if l_boss_email is null and l_person_email is null then
      po_err_num := 4;
      po_err_msg := 'Для Вашей учетной записи не указаны адреса электронной почты. Для восстановления пароля необходимо обратиться в службу технической поддержки.';
      return null;
    end if;
  
    if pi_code is null then
      po_err_num := 6;
      po_err_msg := 'Некорректная ссылка для восстановления пароля';
      return null;
    end if;
  
    update t_users u
       set u.count_change_passwd     = nvl(u.count_change_passwd, 0) + 1,
           u.link_change_passwd      = pi_code,
           u.date_link_change_passwd = sysdate
     where u.usr_id = l_usr_id;
  
    return l_usr_id;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        PKG_NAME || PU_NAME);
      rollback to sp_set_code;
      return null;
  end;

  ---------------------------------------------------------
  --Установка временого хэша пароля (параметры workerId,hash,salt,algo)
  --%param pi_worker_id пользователь
  --%param pi_hash кэш нового пароля
  --%param pi_SALT salt
  --%param pi_HASH_ALG_ID алгоритм расчета хэша
  --%param pi_ip_address ip адрес с которого производятся изменения
  --%param po_err_num код ошибки
  --%param po_err_msg сообщение об ошибке
  ---------------------------------------------------------
  procedure save_temp_password(pi_worker_id   in number,
                               pi_hash        in varchar2,
                               pi_SALT        in t_users.salt%type,
                               pi_HASH_ALG_ID in t_users.hash_alg_id%type,
                               pi_ip_address  in varchar2,
                               po_err_num     out number,
                               po_err_msg     out varchar2) is
    PU_NAME constant varchar2(100) := '.save_temp_password';
  begin
    savepoint sp_save_psw;
    -- Запись в историю «Восстановление пароля через web-интерфейс»    
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
       pi_worker_id,
       sysdate,
       8,
       pi_worker_id,
       pi_ip_address,
       pi_salt,
       pi_hash_alg_id);
  
    insert into t_users_hist
      (hist_id,
       user_id,
       MOD_DATE,
       act_id,
       worker_id,
       ip_address,
       salt,
       hash_alg_id,
       USR_PWD_MD5)
    values
      (seq_t_users_hist.nextval,
       pi_worker_id,
       sysdate,
       4,
       pi_worker_id,
       pi_ip_address,
       pi_salt,
       pi_hash_alg_id,
       pi_hash);
  
    update T_USERS U
       set U.USR_PWD_MD5        = pi_hash,
           u.worker_id          = pi_worker_id,
           u.date_pswd_to = case
                              when pi_hash <> u.usr_pwd_md5 then
                               add_months(trunc(sysdate - 2 / 24), 2)
                              else
                               u.date_pswd_to
                            end,
           u.is_temp_passwd     = 1,
           u.count_attempts     = 0,
           u.SALT               = pi_salt,
           u.HASH_ALG_ID        = NVL(pi_hash_alg_id, 1),
           u.link_change_passwd = null
     where U.USR_ID = pi_worker_id;
  
    if sql%rowcount = 0 then
      po_err_num := 1;
      po_err_msg := 'Некорректный идентификатор пользователя';
      return;
    end if;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        PKG_NAME || PU_NAME);
      rollback to sp_save_psw;
      return;
  end;

  --------------------------------------------
  --Получения Идентификатора пользователя по ссылке на востановление пароля
  --------------------------------------------
  function get_worker_by_restore_code(pi_code    in varchar2,
                                      po_err_num out number,
                                      po_err_msg out varchar2) return number is
    l_usr_id number;
    PU_NAME constant varchar2(100) := '.save_temp_password';
  begin
    select u.usr_id
      into l_usr_id
      from t_users u
     where lower(u.link_change_passwd) = lower(pi_code)
       and sysdate - u.date_link_change_passwd < 1;
    return l_usr_id;
  exception
    when no_data_found then
      po_err_num := 0;
      --po_err_msg := 'Пользователь не найден';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        PKG_NAME || PU_NAME);
      return null;
  end;

  -----------------------------------------------
  --Получение каналов с организации пользователя
  -----------------------------------------------
  function get_user_channels(pi_worker_id in number,
                             po_err_num   out number,
                             po_err_msg   out varchar2) return sys_refcursor is
    l_func_name varchar2(100) := 'user_pkg.get_user_channels';
    res         sys_refcursor;
  begin
    open res for
      select distinct ch.channel_id
        from t_user_org uo
        join t_org_channels ch
          on ch.org_id = uo.org_id
       where uo.usr_id = pi_worker_id;
  
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;

  ------------------------------------------------
  --Определение является ли пользователь пользователем м2м
  ------------------------------------------------
  function is_user_m2m(pi_worker_id in number) return number is
    l_is_m2m_worker number;
    l_func_name     varchar2(100) := 'user_pkg.is_user_m2m';
  begin
    select count(*)
      into l_is_m2m_worker
      from t_user_details d
     where d.user_id = pi_worker_id;
    if l_is_m2m_worker > 0 then
      return 1;
    else
      return 0;
    end if;
  exception
    when others then
      logging_pkg.error(pi_worker_id || ';' || SQLCODE || '.' || sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        l_func_name);
      return null;
  end;

  --построение дерева организаций с учетом прав пользователя в регионе
  function get_tree_level_by_right(pi_worker_id    in number,
                                   pi_kl_region    in varchar2,
                                   pi_start_org_id in number,
                                   pi_right_str    in varchar2,
                                   po_err_num      out number,
                                   po_err_msg      out varchar2)
    return sys_refcursor is
  begin
    return get_tree_level_by_right(pi_worker_id    => pi_worker_id,
                                   pi_kl_region    => pi_kl_region,
                                   pi_start_org_id => pi_start_org_id,
                                   pi_right_str    => string_tab(pi_right_str),
                                   po_err_num      => po_err_num,
                                   po_err_msg      => po_err_msg);
  end;
      
  --построение дерева организаций с учетом прав пользователя в регионе
  function get_tree_level_by_right(pi_worker_id    in number,
                                   pi_kl_region    in varchar2,
                                   pi_start_org_id in number,
                                   pi_right_str    in string_tab,
                                   po_err_num      out number,
                                   po_err_msg      out varchar2)
    return sys_refcursor is
    res         sys_refcursor;
    l_func_name varchar2(120) := 'user_pkg.get_tree_level_by_right';
    l_org_reg   number;
    --    l_not_org   number;
    l_org_mrf number;
    User_Orgs num_tab;
  begin
  
    begin
      select r.org_id, m.org_id
        into l_org_reg, l_org_mrf
        from t_dic_region r
        join t_dic_mrf m
          on m.id = r.mrf_id
       where r.kl_region = pi_kl_region
         and r.org_id is not null;
    exception
      when no_data_found then
        po_err_num := 1;
        po_err_msg := 'Некорректный код региона';
        return null;
    end;     
       
    open res for
    with user_orgs as 
    (select DISTINCT TUO.ORG_ID
                      FROM T_USER_ORG TUO
                      JOIN T_ROLES_PERM RP
                        ON RP.RP_ROLE_ID = TUO.ROLE_ID
                      JOIN T_PERM_RIGHTS PR
                        ON PR.PR_PRM_ID = RP.RP_PERM_ID
                      join t_rights r
                        on r.right_id = PR.PR_RIGHT_ID                       
                     WHERE PR.PR_RIGHT_ID IS NOT NULL
                       AND r.RIGHT_STRING_ID in  (select * from table(pi_right_str))    
                       AND TUO.USR_ID = pi_worker_id),
     Right_orgs as
      (select distinct m1.org_id
         from /*MV_ORG_TREE*/ t_org_relations M1
        START WITH M1.ORG_ID in
                   (select * from user_orgs)
       CONNECT BY M1.ORG_PID = PRIOR M1.ORG_ID
              AND M1.ORG_RELTYPE IN
                  (1001, 1002, 1004, 1006, 1007, 1008, 1009))
     SELECT T.ORG_ID,
            T.ORG_PID,
            T.ORG_NAME,
            T.HAS_CHILD,
            T.KL_REGION,
            decode(c_rigth, 0, 0, 1) c_rigth
       FROM (select O.ORG_ID,
                    T.ORG_PID,
                    O.ORG_NAME,
                    R.KL_REGION,
                    SIGN(COUNT(TC.ORG_ID)) HAS_CHILD,
                    (select count(*)
                       from Right_orgs u
                      where u.org_id = O.ORG_ID) c_rigth,
                    (select COUNT(*)
                       from /*MV_ORG_TREE*/ t_org_relations M1
                      WHERE M1.ORG_ID IN (SELECT * from Right_orgs)
                     START WITH M1.ORG_ID = O.ORG_ID
                     CONNECT BY PRIOR M1.ORG_PID = M1.ORG_ID
                            AND M1.ORG_RELTYPE IN
                                (1001, 1002, 1004, 1006, 1007, 1008, 1009)) C_PARENT_rigth,
                    (select COUNT(*)
                       from /*MV_ORG_TREE*/ t_org_relations M1
                      WHERE M1.ORG_ID in (SELECT * from Right_orgs)
                      START WITH M1.ORG_ID = O.ORG_ID
                     CONNECT BY  M1.ORG_PID = PRIOR M1.ORG_ID
                            AND M1.ORG_RELTYPE IN
                                (1001, 1002, 1004, 1006, 1007, 1008, 1009)) C_CHILD_rigth
               from /*MV_ORG_TREE*/ t_org_relations T
               join T_ORGANIZATIONS O
                 on T.ORG_ID = O.ORG_ID
                and T.ORG_RELTYPE IN
                    (1001, 1002, 1004, 1006, 1007, 1008, 1009)
                and o.is_enabled <> 0
               left join /*MV_ORG_TREE*/
             t_org_relations TC
                 on T.ORG_ID = TC.ORG_PID
                and TC.ORG_RELTYPE IN
                    (1001, 1002, 1004, 1006, 1007, 1008, 1009)
               left join T_ORGANIZATIONS OC
                 on TC.ORG_ID = OC.ORG_ID
                and oc.is_enabled <> 0
               left join t_dic_region r
                 on r.reg_id = o.region_id
               left join t_dic_mrf m
                 on m.org_id = o.org_id
               left join t_dogovor dog
                  on dog.org_rel_id = t.id  
              where (T.ORG_PID = pi_start_org_id or
                    t.org_id = l_org_reg and
                    t.org_reltype = 1001 and  pi_start_org_id is null)
                and NVL(dog.is_enabled, 1) <> 0 -- ограничение на не заблокированные договора                        
              GROUP BY O.ORG_ID,
                       T.ORG_PID,
                       O.ORG_NAME,
                       T.ORG_RELTYPE,
                       R.KL_REGION,
                       m.id
              order by decode(m.id, null, 1, 0), T.ORG_RELTYPE, O.ORG_NAME) T
      WHERE (C_PARENT_rigth > 0 or C_CHILD_rigth > 0)
      order by ORG_NAME;
  
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;

  ------------------------------------------------------------
  --Получение информации о пользователе(light)
  ------------------------------------------------------------
  function get_user_light(pi_user_id in number,
                          po_err_num out number,
                          po_err_msg out varchar2) return sys_refcursor is
    res         sys_refcursor;
    l_func_name varchar2(100) := 'user_pkg.get_user_light';
  begin
    open res for
      select u.usr_id,
             u.usr_login,
             p.person_lastname,
             p.person_firstname,
             p.person_middlename,
             o.org_name,
             o.org_id,
             p.person_phone,
             p.person_email
        from t_users u
        left join t_person p
          on u.usr_person_id = p.person_id
        join t_organizations o
          on o.org_id = u.org_id
       where u.usr_id = pi_user_id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;

  FUNCTION get_user_default_region(pi_worker_id NUMBER) RETURN VARCHAR2 AS
    v_default_region VARCHAR2(2);
  BEGIN
    BEGIN
      SELECT VALUE
      INTO v_default_region
      FROM t_user_params
      WHERE user_id = pi_worker_id
            AND key = 'REGION_DEFAULT'
            AND ROWNUM <= 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        BEGIN
          SELECT r.kl_region
          INTO v_default_region
          FROM t_users u
          JOIN t_organizations o
          USING (org_id)
          JOIN t_dic_region r ON r.reg_id = o.region_id
          WHERE usr_id = pi_worker_id;
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
    END;
    RETURN v_default_region;
  END;

end user_pkg;
/
