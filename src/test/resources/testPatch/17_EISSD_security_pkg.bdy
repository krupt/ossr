CREATE OR REPLACE package body security_pkg is

       c_package constant varchar2(30) := $$PLSQL_UNIT;
  
function get_user_orgs_right(pi_worker_id in number, pi_org_id in number) 
   return num_tab RESULT_CACHE RELIES_ON(t_user_org, t_roles_perm, t_perm_rights) is
   l_res num_tab;
 begin
   
   select distinct pr.pr_right_id
     bulk collect into l_res
     from t_user_org uo
     join t_roles_perm rp
       on rp.rp_role_id = uo.role_id
     join t_perm_rights pr
       on pr.pr_prm_id = rp.rp_perm_id
     left join t_org_ignore_right_change ch  
       on ch.org_id=uo.org_id
    where uo.usr_id = pi_worker_id     
      and nvl(ch.org_change_id,uo.ORG_ID) = pi_org_id;  
   
   return l_res;
 end;
 
/*
 * @param pi_dog_id ИД договора    
 * @param pi_dog_is_enabled содержит статус договора. 
 *         1 - Договор с организацией открыт
 *         0 - Договор с организацией заблокирован 
 */
 function get_dog_rights(pi_dog_id in number, pi_dog_is_enabled NUMBER := 1) 
   return num_tab RESULT_CACHE RELIES_ON(t_dogovor, t_dogovor_prm, t_perm_rights, t_dogovor_rights) is
   l_res num_tab;
begin
  select right_id
        bulk collect into l_res
        from (
        select pr.pr_right_id as right_id 
          from t_dogovor d    
          join t_dogovor_prm dp            
            on dp.dp_dog_id = d.dog_id
           and (dp.dp_is_enabled = 1 OR dp.dp_is_enabled = pi_dog_is_enabled)
          join t_perm_rights pr
            on pr.pr_prm_id = dp.dp_prm_id            
         where d.dog_id = pi_dog_id
           and (d.is_enabled = 1 OR d.is_enabled = pi_dog_is_enabled)
         union
        select dr.right_id as right_id 
          from t_dogovor d                              
          join t_dogovor_rights dr 
            on dr.dog_id = d.dog_id                    
         where d.dog_id = pi_dog_id   
           and (d.is_enabled = 1 OR d.is_enabled = pi_dog_is_enabled)
        ); 
        
   return l_res;              
end;   
 
 function get_all_right 
   return num_tab RESULT_CACHE RELIES_ON(t_rights) is
   l_res num_tab;
   
 begin
   
   select r.right_id
     bulk collect into l_res
     from t_rights r;

    return l_res;
 end; 

/*
 * @param pi_org_id ИД Организации
 * @param pi_dog_is_enabled содержит статус договора. 
 *         1 - Договор с организацией открыт
 *         0 - Договор с организацией заблокирован
 */
 function get_orgs_right(pi_org_id in number, pi_dog_is_enabled NUMBER := 1) 
   return num_tab RESULT_CACHE RELIES_ON(t_organizations, t_rights, t_org_relations, t_dogovor, mv_org_tree, t_dogovor_prm, t_perm_rights, t_dogovor_rights) is
   l_res num_tab := num_tab();
   l_tmp num_tab;
 begin

   if (IS_ORG_USI( pi_org_id) = 0) then    
      
      for rec in (
        select distinct d.dog_id 
          from t_organizations o 
          join t_org_relations r
            on o.root_org_id = r.org_id
          join t_dogovor d on r.id = d.org_rel_id and (d.is_enabled = 1 OR d.is_enabled = pi_dog_is_enabled)          
         where o.org_id = pi_org_id               
           and o.org_id <> 3
        ) loop
        
        l_tmp := l_res;
        --l_res := l_tmp multiset union get_dog_rights(rec.dog_id);
        l_res := l_tmp multiset union get_dog_rights(rec.dog_id, pi_dog_is_enabled);
      end loop;  
      
    else
       
       l_res := get_all_right();
       
    end if;
    return l_res;
 end;   
 
  
  -------------------------------------------------------------------------
  -- Получение маски прав пользователя на основе его ролей во всех организациях
  -- c ограничением прав ролей правами организаций пользователя
  function Get_User_Mask_By_Orgs(pi_worker_id in T_USERS.USR_ID%type)
    return num_tab is
    l_res num_tab := num_tab();
    l_tmp num_tab;
  begin  
    
    if (pi_worker_id = 777) then
      return get_all_right();  
    end if;  
    
    for item in (select distinct uo.org_id
                   from T_USER_ORG uo
                  where uo.USR_ID = pi_worker_id) loop      
      l_tmp := l_res;
      l_res := l_tmp multiset union (get_user_orgs_right(pi_worker_id, item.org_id) multiset intersect get_orgs_right(item.org_id) );                                                   
    end loop;
    
    return l_res;
  end Get_User_Mask_By_Orgs;
  -------------------------------------------------------------------------
  function Check_User_Right2_Int(pi_right_id  in T_RIGHTS.RIGHT_ID%type,
                                 pi_worker_id in T_USERS.USR_ID%type)
    return BOOLEAN is    
  begin
    RETURN pi_right_id MEMBER OF Get_User_Mask_By_Orgs(pi_worker_id);   
  end Check_User_Right2_Int;
  ------------------------------------------------------------------------------------
  -- 50802 Получение ИД права по его строковому ИД
  ------------------------------------------------------------------------------------
  function get_right_id_by_str_id(pi_str_right_id in T_RIGHTS.RIGHT_STRING_ID%type) 
    return number RESULT_CACHE RELIES_ON(t_rights) is
    res number;
  begin
    select t.right_id
      into res
      from t_rights t
     where t.right_string_id = pi_str_right_id;
    return res;
  end get_right_id_by_str_id;
  -------------------------------------------------------------------------
  -- Внутреняя функция проверки прав
  -- состоит из:
  -- 1. Маски ролей, которыми обладает пользователь в заданной организации
  -- 2. Маски ролей, которыми обладает пользователь в родительских и курирующих организациях
  -- 3. Маски организации pi_org_id (состоит из масок договоров между ней и курирующими организациями)
  -- Примечания:
  -- 1. При обходе родительских и курирующих организаций права пользователя ограничиваются правами организаций
  -- 2. Если пользователь является сотрудником курирующей организации 2-го уровня и выше, то по умолчанию
  -- маска прав пользователя в такой организаци ограничивается правами чтения, если pi_recursive=false
  -- 3. По умолчанию считается, что права, вычесленные при обходе родительских и курирующих организаций, в которых
  -- работает пользователь должны ограничиваться маской организации pi_org_id. Чтобы убрать ограничение, необходимо передать
  -- pi_is_up = true
  -------------------------------------------------------------------------
     
 function check_right_inner(
                             pi_org_id       in number,
                             pi_worker_id    in number,
                             pi_right_id     in number,
                             pi_is_up        in number,
                             pi_is_recursive in number,
                             pi_dog_is_enabled NUMBER := 1
                            )
    return number is
    l_par_org_count number := 0; -- количество курирующих или родительских организаций для pi_org_id, в которых работает пользователь
    l_cnt_right boolean;
    l_cnt_right_role boolean;
    l_cnt_right_org boolean;    
    l_res boolean := false;
    l_read_only number;    
  begin    
    --для системного пользователя права все есть всегда
    if pi_worker_id = 777 then
      return 1;
    end if;  
    -- суммируем права пользователя в курирующих и родительских организациях
    for rec1 in (select distinct t.org_pid, t.org_reltype, t.rel_path
                   from t_user_org uo,
                        t_org_ignore_right_change ch,
                        (select org_pid,
                                r.org_reltype,
                                Sys_Connect_By_Path(r.ORG_RELTYPE, '/') rel_path
                           from t_org_relations r, t_organizations o
                          where r.org_pid <> -1
                            and o.org_id = r.org_pid
                         connect by prior r.org_pid = r.org_id
                          start with r.org_id = pi_org_id) t
                  where ch.org_id(+)=uo.org_id
                    and t.org_pid = nvl(ch.org_change_id,uo.org_id)
                    and uo.usr_id = pi_worker_id) loop

      l_par_org_count := l_par_org_count + 1;
      -- права пользователя на основе ролей в организации
      l_cnt_right_role := pi_right_id member of get_user_orgs_right(pi_worker_id, rec1.org_pid);

      -- если организация является родителем или прямым куратором pi_org_id,
      -- а также если указан признак рекурсии, то учитываем права организации
      -- в результирующей маске
      if (rec1.org_reltype = 1001 or pi_is_recursive > 0 or
         instr(rec1.rel_path, '1004', 1, 2) = 0 or
         instr(rec1.rel_path, '999', 1, 2) = 0) or
         (nvl(is_org_rtmob(rec1.org_pid), 1) = 1 and
         (instr(rec1.rel_path, '1004', 1, 3) = 0 and
         instr(rec1.rel_path, '999', 1, 3) = 0))
      then
        
        l_cnt_right_org := pi_right_id member of get_orgs_right(rec1.org_pid, pi_dog_is_enabled);
        
      else
         -- если организация является куратором больше первого уровня, то ограничиваем права данной
         -- организации операциями чтения
         select count(*)
            into l_read_only
            from t_rights r
           where r.right_id = pi_right_id
             and r.right_readonly = 1;             
              
         l_cnt_right_org := pi_right_id member of get_orgs_right(rec1.org_pid, pi_dog_is_enabled) and (l_read_only = 1);
         
      end if;
      -- ограничиваем права пользователя правами организации и суммируем с правами, вычесленными на пред. шаге
      l_res := l_res or (l_cnt_right_role and l_cnt_right_org);

    end loop;
    if (pi_is_up = 0 or l_par_org_count = 0) then
      -- ограничиваем вычесленные права правами организации pi_org_id
      -- суммируем роли пользователя в организации, в которой
      -- выполняется операция
      l_cnt_right := pi_right_id member of get_user_orgs_right(pi_worker_id, pi_org_id);

      -- добавляем права пользователя в организации pi_org_id к
      -- его эффективным правам в родительских и курирующих организациях
      l_res := l_res or l_cnt_right;
      
      -- получаем маску организации, в которой выполняется операция
      l_cnt_right_org := pi_right_id member of get_orgs_right(pi_org_id, pi_dog_is_enabled);
       
      -- ограничиваем правами организации, в которой выполняется операция
      l_res := l_res AND l_cnt_right_org;
    end if;
    if l_res then
       return 1;
    else
       return 0;    
    end if;   
end;

  ------------------------------------------------------------------------------------
  -- Проверка прав (pi_right) пользователя pi_worker_id в организации
  ------------------------------------------------------------------------------------
  function Check_Rights( pi_right        in T_RIGHTS.RIGHT_ID%type, -- идентификатор права
                         pi_org_id       in T_ORGANIZATIONS.ORG_ID%type, -- идентификатор организации
                         pi_worker_id    in pls_integer, -- идентификатор пользователя
                         po_err_num      out pls_integer,
                         po_err_msg      out varchar2,
                         pi_is_up        in boolean := false, -- ограничивать права пользователя правами родительских или курирующих организаций, если пользователь в них работает
                         pi_is_recursive in boolean := false, -- учитывать все права пользователя в курирующих организациях 2-го и более уровней
                         pi_dog_is_enabled NUMBER := 1
                        ) 
   return BOOLEAN is
  begin
    if (check_right_inner(pi_org_id,
                          pi_worker_id,
                          pi_right,
                          sys.diutil.bool_to_int(pi_is_up),
                          sys.diutil.bool_to_int(pi_is_recursive),
                          pi_dog_is_enabled) = 0) then
      po_err_num := c_err_access_denied_code;
      po_err_msg := c_err_access_denied_mess || ' (' || pi_right || ')';
      return false;
    end if;
    return true;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, c_package || '.' || 'Check_Rights'); 
      return false; 
  end;
  
  function Check_Rights_Number(pi_right        in T_RIGHTS.RIGHT_ID%type, -- идентификатор права
                        pi_org_id       in T_ORGANIZATIONS.ORG_ID%type, -- идентификатор организации
                        pi_worker_id    in number) -- идентификатор пользователя
   return NUMBER is
   l_err_code number;
   l_err_msg varchar2(32000);
   l_res boolean;
   begin
     l_res := Check_Rights(pi_right, pi_org_id, pi_worker_id, l_err_code, l_err_msg,false,false);
     
     if nvl(l_err_code, 0) not in (0, c_err_access_denied_code) then
        raise_application_error (-20000, l_err_code|| ' ' || l_err_msg);
     end if;
     
     return sys.diutil.bool_to_int(l_res);
   end;
  ------------------------------------------------------------------------------------
  -- 50802
  -- Проверка прав (pi_right) пользователя pi_worker_id в организации
  -- pi_org_id
  ------------------------------------------------------------------------------------
  function Check_Rights_str( pi_str_right_id in T_RIGHTS.RIGHT_STRING_ID%type, -- идентификатор права
                             pi_org_id       in T_ORGANIZATIONS.ORG_ID%type, -- идентификатор организации
                             pi_worker_id    in pls_integer, -- идентификатор пользователя
                             po_err_num      out pls_integer,
                             po_err_msg      out varchar2,
                             pi_is_up        in boolean := false, -- ограничивать права пользователя правами родительских или курирующих организаций, если пользователь в них работает
                             pi_is_recursive in boolean := false, -- учитывать все права пользователя в курирующих организациях 2-го и более уровней
                             pi_dog_is_enabled NUMBER := 1
                            ) 
   return BOOLEAN is
    l_right_id T_RIGHTS.RIGHT_ID%type;
  begin
    l_right_id := get_right_id_by_str_id(pi_str_right_id);
    
    return Check_Rights(l_right_id,
                        pi_org_id,
                        pi_worker_id,
                        po_err_num,
                        po_err_msg,
                        nvl(pi_is_up, false),
                        nvl(pi_is_recursive, false),
                        pi_dog_is_enabled);
  end Check_Rights_str;
  ---------------------------------------------------------------------------
  function Check_User_Right2(pi_right_id  in T_RIGHTS.RIGHT_ID%type,
                             pi_worker_id in T_USERS.USR_ID%type,
                             po_err_num   out pls_integer,
                             po_err_msg   out varchar2) return BOOLEAN is
    res boolean := false;
  begin
    res := Check_User_Right2_Int(pi_right_id, pi_worker_id);
    if (not res) then
      po_err_num := c_err_access_denied_code;
      po_err_msg := c_err_access_denied_mess || ' (' || pi_right_id || ')';
    end if;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return false;
  end Check_User_Right2;
  ------------------------------------------------------------------------------------
  -- 50802 Функция проверки прав по строковому ИД
  ------------------------------------------------------------------------------------
  function Check_User_Right_str(pi_str_right_id in T_RIGHTS.RIGHT_STRING_ID%type,
                                pi_worker_id    in T_USERS.USR_ID%type,
                                po_err_num      out pls_integer,
                                po_err_msg      out varchar2) return BOOLEAN is
    l_right_id T_RIGHTS.RIGHT_ID%type;
  begin
    l_right_id := get_right_id_by_str_id(pi_str_right_id);
    
    return Check_User_Right2(l_right_id,
                             pi_worker_id,
                             po_err_num,
                             po_err_msg);
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return false;
  end Check_User_Right_str;
  ---------------------------------------------------------------------------
  -- Проверка прав (pi_right) пользователя pi_worker_id в массиве организаций
  -- Возвращает TRUE, если пользователь обладает заданными правами в любой
  -- из организаций, переданных в массиве pi_orgs
  function Check_Rights_Orgs(pi_right        in T_RIGHTS.RIGHT_ID%type,
                             pi_orgs         in num_tab,
                             pi_worker_id    in pls_integer,
                             po_err_num      out pls_integer,
                             po_err_msg      out varchar2,
                             pi_is_up        in boolean := false, -- ограничивать права пользователя правами родительских или курирующих организаций, если пользователь в них работает
                             pi_is_recursive in boolean := false)
    return BOOLEAN is
  begin
    if (pi_orgs.count > 0) then 
      for i in pi_orgs.first .. pi_orgs.last loop
        if (check_right_inner(pi_orgs(i),
                          pi_worker_id,
                          pi_right,
                          sys.diutil.bool_to_int(pi_is_up),
                          sys.diutil.bool_to_int(pi_is_recursive)) <> 0) then
          return true;
        end if;
      end loop;
    end if;
    po_err_num := c_err_access_denied_code;
    po_err_msg := c_err_access_denied_mess || ' (' || pi_right || ')';
    return false;
  end Check_Rights_Orgs;
  ------------------------------------------------------------------------------------
  -- 50802
  -- Проверка прав (pi_right) пользователя pi_worker_id в массиве организаций
  -- Возвращает TRUE, если пользователь обладает заданными правами в любой
  -- из организаций, переданных в массиве pi_orgs
  ------------------------------------------------------------------------------------
  function Check_Rights_Orgs_str(pi_str_right_id in T_RIGHTS.RIGHT_STRING_ID%type,
                                 pi_orgs         in num_tab,
                                 pi_worker_id    in pls_integer,
                                 po_err_num      out pls_integer,
                                 po_err_msg      out varchar2,
                                 pi_is_up        in boolean := false, -- ограничивать права пользователя правами родительских или курирующих организаций, если пользователь в них работает
                                 pi_is_recursive in boolean := false)
    return BOOLEAN is
    l_right_id T_RIGHTS.RIGHT_ID%type;
  begin
    l_right_id := get_right_id_by_str_id(pi_str_right_id);
    
    return Check_Rights_Orgs(l_right_id,
                             pi_orgs,
                             pi_worker_id,
                             po_err_num,
                             po_err_msg,
                             nvl(pi_is_up, false),
                             nvl(pi_is_recursive, false));
  
  end Check_Rights_Orgs_str;
  -------------------------------------------------------------
  --по воркеру определяет принадлежность его (организаций в каких у него есть роли) к Ростелекому или РТ-Мобайлу
  -------------------------------------------------------------
  function Get_User_belonging(pi_worker_id in number,
                              po_err_num   out number,
                              po_err_msg   out varchar2) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select max(case
                   when is_org_rtmob = 1 or is_org_rtmob is null then
                    1
                   else
                    0
                 end) is_org_rtmob,
             max(case
                   when is_org_rtmob = 0 or is_org_rtmob is null then
                    1
                   else
                    0
                 end) is_org_rt
        from (select rt.is_org_rtm is_org_rtmob
                from t_user_org t
                join t_org_is_rtmob rt
                  on rt.org_id = t.org_id
               where t.usr_id = pi_worker_id) tab;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  
  ----------------------------------------------------------------
  -- получение структур по пользователю
  ----------------------------------------------------------------
  procedure get_user_structures(pi_worker_id in number,
                                po_is_rtk    out boolean,
                                po_is_rtm    out boolean,
                                po_err_num   out number,
                                po_err_msg   out varchar2) is
    l_user_belonging sys_refcursor;
    l_is_worker_rtk  number;
    l_is_worker_rtm  number;
    l_err_num        number;
    l_err_msg        varchar2(4000);
  begin
    l_user_belonging := Get_User_belonging(pi_worker_id,
                                           l_err_num,
                                           l_err_msg);
    fetch l_user_belonging
      into l_is_worker_rtm, l_is_worker_rtk;

    po_is_rtk := nvl(l_is_worker_rtk, 0) = 1;
    po_is_rtm := nvl(l_is_worker_rtm, 0) = 1;
  end;
  ----------------------------------------------------------------------------
  function Get_User_By_Id(pi_user_id    in T_USERS.USR_ID%type,
                          po_favor_menu out sys_refcursor)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select distinct U.USR_ID,
                      USR_LOGIN,
                      PERSON_LASTNAME,
                      PERSON_FIRSTNAME,
                      PERSON_MIDDLENAME,
                      PERSON_EMAIL,
                      USR_STATUS,
                      group_concat_user_roles(pi_user_id, UO.ORG_ID) roles,
                      U.IS_ENABLED,
                      u.org_id,
                      u.date_login_to,
                      u.date_pswd_to,
                      u.is_temp_passwd,
                      u.link_change_passwd,
                      u.date_link_change_passwd,
                      -- 86340
                      u.SALT,
                      u.HASH_ALG_ID
        from T_USERS U, T_USER_ORG UO, T_PERSON
       where U.USR_ID = UO.USR_ID
         and USR_PERSON_ID = PERSON_ID
         and U.USR_ID = pi_user_id;
  
    open po_favor_menu for
      select m.menu_id
        from t_user_favorit_menu m
       where m.user_id = pi_user_id
       order by 1;
  
    return res;
  end Get_User_By_Id;
  ------------------------------------------------------------------------------------
  function Get_User_By_Id2(pi_user_id   in T_USERS.USR_ID%type,
                           pi_worker_id in T_USERS.USR_ID%type,
                           po_roles     out sys_refcursor,
                           po_err_num   out pls_integer,
                           po_err_msg   out varchar2) return sys_refcursor is
    res               sys_refcursor;
    is_change_himself BOOLEAN;
    right_to_edit     BOOLEAN := false;
    l_cnt_priv        pls_integer; --для определения,привилегированная ли уч. запись
    l_roles_exc       num_tab;
    l_str1            varchar2(1000);
    l_str2            varchar2(1000);
    tp_roles_exc      array_num_str2 := array_num_str2(null, null, null);
    tab_roles_exc     num_str2_tab := num_str2_tab();
    l_count           number;
    l_beg             timestamp;
    l_end             timestamp;
    l_param REQUEST_PARAM_TAB;
    l_roles_str varchar2(4000);
  begin
    l_beg := systimestamp;
    logging_pkg.debug('pi_user_id=' || pi_user_id || ' pi_worker_id=' ||
                      pi_worker_id,
                      'Get_User_By_Id2');
    is_change_himself := (pi_worker_id = pi_user_id);
    -- checking access for operation for specified user
    right_to_edit := Security_pkg.Check_Rights_Orgs(5303,
                                                    Get_User_Orgs_Tab( /*pi_user_id*/pi_worker_id),
                                                    pi_worker_id,
                                                    po_err_num,
                                                    po_err_msg);
  
    if (is_change_himself) then
      po_err_num := 0;
      po_err_msg := '';
    end if;
    -- checking access for operation for specified user
    if ((not right_to_edit) and (not is_change_himself)) then
      return null;
    end if;
  
    -- Берем организации юзера
    /* select count(*)
     into l_count
     from t_user_org uo, t_user_org wo
    where uo.usr_id = pi_user_id
      and wo.usr_id = pi_worker_id
      and uo.org_id in (select t.org_id
                          from mv_org_tree t
                        connect by prior t.org_id = t.org_pid
                         start with t.org_id = wo.org_id);*/
    if pi_worker_id <> pi_user_id then
      with tab as
       (SELECT distinct T.ORG_ID
          FROM MV_ORG_TREE T
        CONNECT BY PRIOR T.ORG_ID = T.ORG_PID
         START WITH T.ORG_ID in
                    (select WO.ORG_ID
                       from T_USER_ORG WO
                      where WO.USR_ID = pi_worker_id))
      SELECT COUNT(*)
        into l_count
        FROM T_USER_ORG UO
        join tab
          on tab.ORG_ID = UO.ORG_ID
       WHERE UO.USR_ID = pi_user_id;
    
      if l_count = 0 then
        po_err_num := 1;
        po_err_msg := 'У Вас отсутствуют права на просмотр данного пользователя.';
      end if;
    end if;
  
    select count(*)
      into l_cnt_priv
      from t_user_org uo, t_organizations o, t_roles r
     where uo.usr_id = pi_user_id
       and uo.org_id = o.org_id
       and o.is_enabled = 1
       and uo.role_id = r.role_id
       and r.is_privilege = 1;
       
    select REQUEST_PARAM_TYPE(key => par.key, value => par.value) bulk collect
      into l_param
      from t_user_params par
     where par.user_id = pi_user_id;
     
    l_roles_str:=group_concat_user_roles(pi_user_id, null); 
  
    open res for
      select U.USR_ID, /*UO.ORG_ID usr_org_id,*/
             USR_LOGIN,
             p.PERSON_LASTNAME,
             p.PERSON_FIRSTNAME,
             p.PERSON_MIDDLENAME,
             p.PERSON_EMAIL,
             USR_STATUS,
             l_roles_str roles,
             U.IS_ENABLED,
             u.org_id,
             -- 38664 Для прямого продавца
             p.PERSON_PHONE,
             u.date_login_to,
             u.date_pswd_to,
             u.is_temp_passwd,
             u.system_id,
             s.name           system_name,
             u.tz_id,
             -- 58633
             U.FIO_DOVER,
             U.POSITION_DOVER,
             U.NA_OSNOVANII_DOVER,
             U.ADDRESS_DOVER,
             U.NA_OSNOVANII_DOC_TYPE,
             u.fio_dover_nominative,
             u.boss_email,
             l.user_principal_name,
             nvl(ch.value, org_ch.channel_id) channel_id, --83124 
             l_param params,
             sa.sa_id,
             l.user_principal_name,
             sa.sa_emp_num,
             psa.person_lastname || ' ' || psa.person_firstname || ' ' ||
             psa.person_middlename sa_fio,
             su.su_id,
             su.su_emp_num,
             psu.person_lastname || ' ' || psu.person_firstname || ' ' ||
             psu.person_middlename su_fio,
             -- 86340
             u.salt,
             u.HASH_ALG_ID,
             u.employee_number
        from T_USERS U      
        left join T_PERSON p
          on u.USR_PERSON_ID = p.PERSON_ID
        left join t_system s
          on s.id = nvl(u.system_id, 0)
        left join t_user_params ch
          on ch.user_id = u.usr_id
         and ch.key = 'CHANNEL_DEFAULT'
        left join t_org_channels org_ch
          on org_ch.org_id = u.org_id
         and org_ch.default_type = 1      
      --
        left join t_ldapusers l
          on u.usr_id = l.usr_id
        left join t_seller_active sa
          on sa.sa_user_id = u.usr_id
         and sa.sa_is_block = 0
        left join t_person psa
          on psa.person_id = sa.sa_person_id
        left join t_supervisor su
          on su.su_user_id = u.usr_id
         and su.su_is_block = 0
        left join t_person psu
          on psu.person_id = su.su_person_id
        left join t_ldapusers l
          on u.usr_id = l.usr_id
       where U.USR_ID = pi_user_id;
  
    for oraRoles in (select distinct t.role_id
                       from t_user_org t
                      where t.usr_id = pi_user_id) loop
      select re.exc_role_id bulk collect
        into l_roles_exc
        from t_roles_exc re
       where re.role_id = oraRoles.role_id;
    
      if (l_roles_exc.count <> 0) then
        for item in (Select column_value as role_exc, r.role_name
                       from TABLE(l_roles_exc) t
                       join t_roles r
                         on t.column_value = r.role_id) loop
          if (l_str1 is null) then
            l_str1 := item.role_exc;
            l_str2 := item.role_name;
          else
            l_str1 := l_str1 || ',' || item.role_exc;
            l_str2 := l_str2 || ',' || item.role_name;
          end if;
        end loop;
      else
        l_str1 := null;
        l_str2 := null;
      end if;
      tp_roles_exc.num1 := oraRoles.role_id;
      tp_roles_exc.str1 := l_str1;
      tp_roles_exc.str2 := l_str2;
      tab_roles_exc.extend;
      tab_roles_exc(tab_roles_exc.last) := tp_roles_exc;
    end loop;
  
    for oraRolesExc in (select distinct l.role_id
                          from t_user_org l
                         where l.usr_id = pi_user_id) loop
      for oraRec in (Select distinct re.role_id, r.role_name
                       from t_roles_exc re
                       join t_roles r
                         on re.role_id = r.role_id
                      where re.exc_role_id = oraRolesExc.role_id) loop
        for i in 1 .. tab_roles_exc.count loop
          if (tab_roles_exc(i).num1 = oraRolesExc.Role_Id) then
            if (tab_roles_exc(i).str1 is not null) then
              tab_roles_exc(i).str1 := tab_roles_exc(i)
                                       .str1 || ',' || oraRec.role_id;
              tab_roles_exc(i).str2 := tab_roles_exc(i)
                                       .str2 || ',' || oraRec.Role_Name;
            else
              tab_roles_exc(i).str1 := oraRec.role_id;
              tab_roles_exc(i).str2 := oraRec.Role_Name;
            end if;
          end if;
        end loop;
      end loop;
    end loop;
  
    open po_roles for
      Select u.org_id as usr_org_id,
             uo.org_id as role_org_id,
             uo.role_id,
             r.role_name,
             re.str1 as exc_roles,
             re.str2 as exc_roles_name,
             (case
               when l_cnt_priv > 0 then
                1
               else
                0
             end) is_privilege
        from t_user_org uo
        join t_users u
          on uo.usr_id = u.usr_id
        join (Select * from TABLE(tab_roles_exc)) re
          on uo.role_id = re.num1
        join t_roles r
          on r.role_id = uo.role_id
       where uo.usr_id = pi_user_id;
    l_end := systimestamp;
    add_logging(pi_action     => 'Get_User_By_Id2',
                pi_time_begin => l_beg,
                pi_time_end   => l_end,
                pi_worker_id  => pi_worker_id,
                pi_descript   => '',
                po_err_num    => po_err_num,
                po_err_msg    => po_err_msg);
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      l_end      := systimestamp;
      add_logging(pi_action     => 'Get_User_By_Id2',
                  pi_time_begin => l_beg,
                  pi_time_end   => l_end,
                  pi_worker_id  => pi_worker_id,
                  pi_descript   => po_err_msg,
                  po_err_num    => po_err_num,
                  po_err_msg    => po_err_msg);
      return null;
  end Get_User_By_Id2;
  ------------------------------------------------------------------------------------
  function Get_User_By_Id2_for_mpz(pi_user_id    in T_USERS.USR_ID%type,
                                   pi_worker_id  in T_USERS.USR_ID%type,
                                   po_favor_menu out sys_refcursor,
                                   po_err_num    out pls_integer,
                                   po_err_msg    out varchar2)
    return sys_refcursor is
    res               sys_refcursor;
    is_change_himself BOOLEAN;
    l_param           REQUEST_PARAM_TAB;
    right_to_edit     BOOLEAN := false;
  begin
    po_err_num := 0;
    po_err_msg := '';
  
    is_change_himself := (pi_worker_id = pi_user_id);
  
    if (not is_change_himself) then
    
      right_to_edit := Security_pkg.Check_Rights_Orgs_str('EISSD.WORKER.VIEW',
                                                          Get_User_Orgs_Tab(pi_user_id),
                                                          pi_worker_id,
                                                          po_err_num,
                                                          po_err_msg);
    
      if ( /*(l_count = 0)*/
          (not right_to_edit)) then
        po_err_num := 1;
        po_err_msg := 'У Вас отсутствуют права на просмотр данного пользователя.';
        return null;
      end if;
    end if;
  
    select REQUEST_PARAM_TYPE(key => par.key, value => par.value) bulk collect
      into l_param
      from t_user_params par
     where par.user_id = pi_user_id;
  
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
             nvl(d_ch.channel_id, d_org_ch.channel_id) channel_id,
             l_param params,
             sa.sa_id,
             su.su_id
        from T_USERS U
        left join T_PERSON p
          on u.USR_PERSON_ID = p.PERSON_ID
        left join t_user_params ch
          on ch.user_id = u.usr_id
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
        left join t_seller_active sa
          on sa.sa_user_id = u.usr_id
         and sa.sa_is_block = 0
        left join t_supervisor su
          on su.su_user_id = u.usr_id
         and su.su_is_block = 0
       where U.USR_ID = pi_user_id;
  
    open po_favor_menu for
      select m.menu_id
        from t_user_favorit_menu m
       where m.user_id = pi_user_id
       order by 1;
  
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  ------------------------------------------------------------------
  function group_concat_user_roles(pi_worker_id in T_USER_ORG.USR_ID%type,
                                   pi_org_id    in T_USER_ORG.ORG_ID%type)
    return varchar2 is
    res     varchar2(2000) := '';
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
    --            and ((pi_org_id is null) or (pi_org_id = -1) or (pi_org_id = UO.ORG_ID));
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
  ------------------------------------------------------------------------------------
  -- Проверка прав, указанных в массиве pi_rights для пользователя pi_worker_id в организации
  -- Функция возвращает истину, если пользователь имеет хотя бы одно право из списка
  function Check_Any_Rights(pi_rights       in num_tab, -- массив идентификаторов прав
                            pi_org_id       in T_ORGANIZATIONS.ORG_ID%type, -- идентификатор организации
                            pi_worker_id    in pls_integer, -- идентификатор пользователя
                            po_err_num      out pls_integer,
                            po_err_msg      out varchar2,
                            pi_is_up        in boolean := false, -- ограничивать права пользователя правами родительских или курирующих организаций, если пользователь в них работает
                            pi_is_recursive in boolean := false) -- учитывать все права пользователя в курирующих организациях 2-го и более уровней
   return BOOLEAN is    
  begin
    
    if (pi_rights.count > 0) then 
      for i in pi_rights.first .. pi_rights.last loop
        if (check_right_inner(pi_org_id,
                          pi_worker_id,
                          pi_rights(i),
                          sys.diutil.bool_to_int(pi_is_up),
                          sys.diutil.bool_to_int(pi_is_recursive)) <> 0) then
          return true;
        end if;
      end loop;
    end if;
    po_err_num := c_err_access_denied_code;
    po_err_msg := c_err_access_denied_mess || ' (' || get_str_by_num_tab(pi_rights) || ')';
    return false;
  
  end;
  ------------------------------------------------------------------------------------
  -- 50802
  -- Проверка прав, указанных в массиве pi_rights для пользователя pi_worker_id в организации
  -- Функция возвращает истину, если пользователь имеет хотя бы одно право из списка
  ------------------------------------------------------------------------------------
  function Check_Any_Rights_str(pi_str_rights   in string_tab, -- массив идентификаторов прав
                                pi_org_id       in T_ORGANIZATIONS.ORG_ID%type, -- идентификатор организации
                                pi_worker_id    in pls_integer, -- идентификатор пользователя
                                po_err_num      out pls_integer,
                                po_err_msg      out varchar2,
                                pi_is_up        in boolean := false, -- ограничивать права пользователя правами родительских или курирующих организаций, если пользователь в них работает
                                pi_is_recursive in boolean := false) -- учитывать все права пользователя в курирующих организациях 2-го и более уровней
   return BOOLEAN is
    l_right_id num_tab;
  begin
  
    select t.right_id bulk collect
      into l_right_id
      from t_rights t
     where t.right_string_id in
           (select column_value right_id from table(pi_str_rights));
  
    return Check_Any_Rights(l_right_id,
                            pi_org_id,
                            pi_worker_id,
                            po_err_num,
                            po_err_msg,
                            nvl(pi_is_up, false),
                            nvl(pi_is_recursive, false));
  
  end Check_Any_Rights_str;
  ------------------------------------------------------------------------------------
  -- Проверка наличия права (pi_right) пользователя pi_worker_id в любой из организаций,
  -- сотрудником которых он является
  function Check_Rights_Any_Org(pi_right     in T_RIGHTS.RIGHT_ID%type, -- идентификатор права
                                pi_worker_id in pls_integer, -- идентификатор пользователя
                                po_err_num   out pls_integer,
                                po_err_msg   out varchar2)
  
   return BOOLEAN is
  
  begin
  
    if (not pi_right MEMBER OF Get_User_Mask_By_Orgs(pi_worker_id)) then
      po_err_num := c_err_access_denied_code;
      po_err_msg := c_err_access_denied_mess || ' (' || pi_right || ')';
      return false;
    end if;
  
    return true;
  
  end;
  ------------------------------------------------------------------------------------
  -- 50802
  -- Проверка наличия права (pi_right) пользователя pi_worker_id в любой из организаций,
  -- сотрудником которых он является
  ------------------------------------------------------------------------------------
  function Check_Rights_Any_Org_str(pi_str_right_id in T_RIGHTS.RIGHT_STRING_ID%type, -- идентификатор права
                                    pi_worker_id    in pls_integer, -- идентификатор пользователя
                                    po_err_num      out pls_integer,
                                    po_err_msg      out varchar2)
  
   return BOOLEAN is
    l_right_id T_RIGHTS.RIGHT_ID%type;
  begin
  
    l_right_id := get_right_id_by_str_id(pi_str_right_id);
  
    return Check_Rights_Any_Org(l_right_id,
                                pi_worker_id,
                                po_err_num,
                                po_err_msg);
  
  end Check_Rights_Any_Org_str;
  -------------------------------------------------------------------------
  -------------------------------------------------------------------------
  function Check_Rights_Any_Org_str2(pi_str_right_id in T_RIGHTS.RIGHT_STRING_ID%type, -- идентификатор права
                                     pi_worker_id    in pls_integer, -- идентификатор пользователя
                                     po_err_num      out pls_integer,
                                     po_err_msg      out varchar2)
    return number is
  begin
    return sys.diutil.bool_to_int(Check_Rights_Any_Org_str(pi_str_right_id,
                                                           pi_worker_id,
                                                           po_err_num,
                                                           po_err_msg));
  end Check_Rights_Any_Org_str2;   
  ------------------------------------------------------------------------------------  
  procedure Check_Orgs_Dog_Enabled(pi_worker_id in T_SESSIONS.ID_WORKER%type) is
    l_usi_cur_count  pls_integer := 0;
    l_sp_cur_count   pls_integer := 0;
    l_dil_cur_count  pls_integer := 0;
    l_dil_cur_count2 pls_integer := 0;
  begin
  
    select count(ORH.ORG_PID)
      into l_usi_cur_count
      from T_USER_ORG UO, mv_org_tree ORH
     where UO.USR_ID = pi_worker_id
       and ORH.ORG_ID = UO.ORG_ID
       and ORH.Root_Reltype = c_rel_tp_crtr_main;
  
    select count(ORH2.ORG_PID)
      into l_sp_cur_count
      from T_USER_ORG UO, mv_org_tree ORH, mv_org_tree ORH2, T_DOGOVOR D
     where UO.USR_ID = pi_worker_id
       and ORH.ORG_ID = UO.ORG_ID
       and ORH.ROOT_RELTYPE = c_rel_tp_sp_sl
       and ORH.Root_Rel_Id = D.ORG_REL_ID
       and D.IS_ENABLED = 1
       and ORH.ROOT_ORG_PID = ORH2.ORG_ID
       and ORH2.ROOT_RELTYPE = c_rel_tp_crtr_main;
  
    select count(ORH2.ORG_PID)
      into l_dil_cur_count2
      from T_USER_ORG UO, mv_org_tree ORH, mv_org_tree ORH2, T_DOGOVOR D
     where UO.USR_ID = pi_worker_id
       and ORH.ORG_ID = UO.ORG_ID
       and ORH.Root_Reltype in (c_rel_tp_dlr_sl, 999)
       and ORH.Root_Rel_Id = D.ORG_REL_ID
       and D.IS_ENABLED = 1
       and ORH.ROOT_ORG_PID = ORH2.ORG_ID
       and ORH2.Root_Reltype = c_rel_tp_crtr_main;
  
    select count(ORH3.ORG_PID)
      into l_dil_cur_count
      from T_USER_ORG  UO,
           mv_org_tree ORH,
           mv_org_tree ORH2,
           mv_org_tree ORH3,
           T_DOGOVOR   D,
           T_DOGOVOR   D2
     where UO.USR_ID = pi_worker_id
       and ORH.ORG_ID = UO.ORG_ID
       and ORH.Root_Reltype in (c_rel_tp_dlr_sl, 999)
       and ORH.Root_Rel_Id = D.ORG_REL_ID
       and D.IS_ENABLED = 1
       and ORH.ROOT_ORG_PID = ORH2.ORG_ID
       and ORH2.Root_Reltype = c_rel_tp_sp_sl
       and ORH2.Root_Rel_Id = D2.ORG_REL_ID
       and D2.IS_ENABLED = 1
       and ORH2.ROOT_ORG_PID = ORH3.ORG_ID
       and ORH3.Root_Reltype = c_rel_tp_crtr_main;
  
    if (l_usi_cur_count + l_sp_cur_count + l_dil_cur_count +
       l_dil_cur_count2 = 0) then
      raise ex_org_havent_rights;
    end if;
  
  end Check_Orgs_Dog_Enabled;
  ------------------------------------------------------------------------
  -- Проверяет, работает ли пользователь в родительской организации или
  -- в организации, курирующей родительскую организацию
  function Check_User_Access_Parent_Org(pi_worker_id in T_USERS.USR_ID%type)
    return pls_integer is
    l_usr_orgs num_tab;
    --l_org_pid  T_ORGANIZATIONS.ORG_ID%type;
  begin
    l_usr_orgs := get_user_orgs_tab(pi_worker_id);
    for i in l_usr_orgs.first .. l_usr_orgs.last loop
      -- 52522 Т.к. была орг-ия с 2-мя родителями (с mv_org_tree не катит, менюшки не видно)
      for j in (select ORG_PID
                  from T_ORG_RELATIONS r
                 where ORG_ID = l_usr_orgs(i)
                   and r.org_reltype = c_rel_tp_parent) loop
        if (j.org_pid = -1) then
          return 1;
        end if;
      end loop;
    end loop;
    return 0;
  end Check_User_Access_Parent_Org;

  -------------------------------------------------------------------------
  -- Проверка прав (pi_right) по dog_id
  function Check_Rights_Dog(pi_right   in T_RIGHTS.RIGHT_ID%type, -- идентификатор права
                            pi_dog_id  in t_dogovor.dog_id%type,
                            po_err_num out pls_integer,
                            po_err_msg out varchar2) return BOOLEAN is
    --l_res raw(64);
  begin
    return pi_right MEMBER OF get_dog_rights(pi_dog_id);   
   exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ', ' || po_err_msg ,
                        c_package || '.' || 'Check_Rights_Dog');
      return false;   
  end;
  ------------------------------------------------------------------------------------
  -- 50802
  ------------------------------------------------------------------------------------
  function Check_Rights_Dog_str(pi_str_right_id in T_RIGHTS.RIGHT_STRING_ID%type, -- идентификатор права
                                pi_dog_id       in t_dogovor.dog_id%type,
                                po_err_num      out pls_integer,
                                po_err_msg      out varchar2) return BOOLEAN is
    l_right_id T_RIGHTS.RIGHT_ID%type;
  begin
    l_right_id := get_right_id_by_str_id(pi_str_right_id);
    return Check_Rights_Dog(l_right_id, pi_dog_id, po_err_num, po_err_msg);
  end Check_Rights_Dog_str;
  
  
  function Check_Any_Rights_Dog_str(pi_str_right_id in string_tab, -- идентификатор права
                                    pi_dog_id       in t_dogovor.dog_id%type,
                                    po_err_num      out pls_integer,
                                    po_err_msg      out varchar2) 
     return number is
     l_right_id num_tab;
     l_res num_tab;
  begin
    
     select r.right_id
       bulk collect into l_right_id
       from t_rights r 
       join table(pi_str_right_id) t
         on t.Column_value = r.right_string_id;
     
     l_res := l_right_id multiset intersect get_dog_rights(pi_dog_id);  
  
     return sign(l_res.Count);
     
     exception
      when others then
        po_err_num := sqlcode;
        po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
        logging_pkg.error(po_err_num || ', ' || po_err_msg ,
                          c_package || '.' || 'Check_Any_Rights_Dog_str');
     return 0;                     
  end;  
  
  function Check_Any_Rights_Perm_str(pi_str_right_id in string_tab, -- идентификатор права
                                     pi_perm_id       in t_dogovor.dog_id%type,
                                     po_err_num      out pls_integer,
                                     po_err_msg      out varchar2) 
     return number is
     l_cnt number := 0;
  begin
         
     select count(*)   
       into l_cnt     
       from t_perm_rights pr
       join t_rights r
         on r.right_id = pr.pr_right_id
       join table(pi_str_right_id) t
         on t.Column_value = r.right_string_id
      where pr.pr_prm_id = pi_perm_id;
      
     return sign(l_cnt);  
     
    exception
      when others then
        po_err_num := sqlcode;
        po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
        logging_pkg.error(po_err_num || ', ' || po_err_msg ,
                          c_package || '.' || 'Check_Any_Rights_Perm_str');
    return 0;                      
  end;      
  
  -------------------------------------------------------------------------------------------
  -- Функция возвращающая массив организаций видимых пользователю
  function Get_User_Orgs_Tab_With_Param1(pi_worker_id           in T_USERS.USR_ID%type, -- пользователь
                                         pi_org_id              in T_ORGANIZATIONS.ORG_ID%type := null, -- организация с которой начинаем
                                         pi_self_include        in pls_integer := 1, -- включать pi_org_id
                                         pi_childrens_include   in pls_integer := 1, -- включать детей
                                         pi_curated_include     in pls_integer := 1, -- включать курируемых
                                         pi_curated_sub_include in pls_integer := 1, -- включать курируемых субдиллерами
                                         pi_curators_include    in pls_integer := 0, -- включать кураторов
                                         pi_tm_1009_include     in pls_integer := 1) -- включать 1009 связь телемаркетинга
   return num_tab is
    res        num_tab := null;
    l_table_0  num_tab := num_tab();
    l_table_1  num_tab := num_tab();
    l_table_2  num_tab := num_tab();
    l_table_3  num_tab := num_tab();
    l_table_03 num_tab := num_tab();
    l_rel_tab  num_tab := num_tab();
    l_rel_tab2 num_tab := num_tab();
  begin
    -- Организации, в которых работает пользователь
    if (pi_org_id is null) then
      select distinct UO.ORG_ID bulk collect
        into l_table_0
        from T_USER_ORG UO
       where UO.USR_ID = pi_worker_id;
    elsif (pi_worker_id is null) then
      select distinct O.ORG_ID bulk collect
        into l_table_0
        from T_ORGANIZATIONS O
       where pi_org_id = O.ORG_ID;
    else
      Select Column_Value bulk collect
        into l_table_0
        from Table(Get_User_Orgs_Tab(pi_worker_id, 1))
       where Column_Value = pi_org_id;
      If l_table_0.Count = 0 then
        select distinct UO.ORG_ID bulk collect
          into l_table_0
          from T_USER_ORG UO
         where UO.USR_ID = pi_worker_id;
      end If;
    end if;
  
    if pi_tm_1009_include = 1 then
      l_rel_tab  := num_tab(1003, 1004, 1007, 1008, 1009, 999);
      l_rel_tab2 := num_tab(1002, 1004, 1007, 1008, 1009, 999);
    else
      l_rel_tab  := num_tab(1003, 1004, 1007, 1008, 999);
      l_rel_tab2 := num_tab(1002, 1004, 1007, 1008, 999);
    end if;
  
    -- Организации, которые связаны с l_table_0 отношениями типа подчиненности (дети для l_table_0)
    if (pi_childrens_include = 1) then
      select distinct ORH.ORG_ID bulk collect
        into l_table_1
        from mv_org_tree ORH
      Connect by prior orh.org_id = orh.org_pid
             and orh.org_reltype in (1001, 1006) -- костыль для связного и инфо-города
       Start with ORH.ORG_pID in (select * from TABLE(l_table_0))
              and (pi_tm_1009_include = 1 or orh.org_reltype != 1009);
    end if;
  
    -- Организации, которые связаны с l_table_0 и l_table_1 отношениями типа курирование (где l_table_0 и l_table_1 - кураторы)
    if (pi_curated_include = 1) then
      select distinct ORH.ORG_ID bulk collect
        into l_table_2
        from mv_org_tree ORH
       where ((ORH.Root_Org_Pid in (select * from TABLE(l_table_0))) or
             (ORH.Root_Org_Pid in (select * from TABLE(l_table_1))))
         and (ORH.ROOT_RELTYPE In
             (select column_value from table(l_rel_tab)));
    end if;
  
    if (pi_curated_sub_include = 1) then
      select distinct ORH.ORG_ID bulk collect
        into l_table_3
        from mv_org_tree ORH
       where ((ORH.Root_Org_Pid in (select * from TABLE(l_table_0))) or
             (ORH.Root_Org_Pid in (select * from TABLE(l_table_1))) or
             (ORH.Root_Org_Pid in (select * from TABLE(l_table_2))))
         and (ORH.ROOT_RELTYPE in
             (select column_value from table(l_rel_tab)));
    end if;
    -- Если не надо включать pi_org_id
    if ((pi_org_id is not null) and (pi_self_include <> 1)) then
      l_table_0 := new num_tab();
    end if;
    -- Организации, которые связаны с l_table_0, l_table_1 и l_table_2  отношениями типа
    -- подчиненности (кураторы для l_table_0, l_table_1 и l_table_2)
    if (pi_curators_include = 1) then
      select distinct ORH.Root_Org_Pid bulk collect
        into l_table_03
        from mv_org_tree ORH
       where (ORH.ORG_ID in (select *
                               from TABLE(l_table_0)
                             union all
                             select *
                               from TABLE(l_table_1)
                             union all
                             select *
                               from TABLE(l_table_2)
                             union all
                             select * from TABLE(l_table_3)))
         and (ORH.Root_Reltype in
             (select column_value from table(l_rel_tab2)));
    end if;
    -- Результат
    select distinct O.ORG_ID bulk collect
      into res
      from T_ORGANIZATIONS O
     where O.ORG_ID in (select *
                          from TABLE(l_table_0)
                        union all
                        select *
                          from TABLE(l_table_1)
                        union all
                        select *
                          from TABLE(l_table_2)
                        union all
                        select *
                          from TABLE(l_table_3)
                        union all
                        select * from TABLE(l_table_03));
    return res;
  end Get_User_Orgs_Tab_With_Param1;
  ----------------------------------------------------------------
  function Get_User_Orgs_Tab_By_Right(pi_worker_id           in T_USERS.USR_ID%type, -- пользователь
                                      pi_right_id            in T_RIGHTS.RIGHT_ID%type, -- право
                                      pi_org_id              in T_ORGANIZATIONS.ORG_ID%type := null, -- организация с которой начинаем
                                      pi_self_include        in pls_integer := 1, -- включать pi_org_id
                                      pi_childrens_include   in pls_integer := 1, -- включать детей
                                      pi_curated_include     in pls_integer := 1, -- включать курируемых
                                      pi_curated_sub_include in pls_integer := 1, -- включать курируемых субдиллерами
                                      pi_curators_include    in pls_integer := 0, -- включать кураторов
                                      pi_tm_1009_include     in pls_integer := 1) -- включать 1009 связь телемаркетинга
  
   return num_tab is
    res       num_tab;
    tmp       num_tab;
    tmp2      num_tab;
    l_cnt     number;
    l_rel_tab num_tab;
  begin
    -- Если не передано право или воркер вызываем старую функцию
    if pi_right_id is null or pi_worker_id is null or
      --nvl(pi_worker_id, 1032) = 1032 or
       is_worker_developer(pi_worker_id) = 1 then
      return security_pkg.Get_User_Orgs_Tab_With_Param1(pi_worker_id           => pi_worker_id,
                                                        pi_org_id              => pi_org_id,
                                                        pi_self_include        => pi_self_include,
                                                        pi_childrens_include   => pi_childrens_include,
                                                        pi_curated_include     => pi_curated_include,
                                                        pi_curated_sub_include => pi_curated_sub_include,
                                                        pi_curators_include    => pi_curators_include,
                                                        pi_tm_1009_include     => pi_tm_1009_include);
    else
      --Для ЕЦОВ
      Select Count(*)
        into l_cnt
        from t_user_org t
       where t.usr_id = Pi_worker_id
         and t.org_id in
             (select org_id from t_org_ignore_right where TYPE_ORG = 1);
    
      If l_cnt > 0 then
        Select tor.org_id bulk collect
          into res
          from t_org_relations tor
        Connect by prior tor.org_id = tor.org_pid
         Start with tor.org_id = 2001269;
      else
        if pi_tm_1009_include = 1 then
          l_rel_tab := num_tab(1001,
                               1002,
                               1004,
                               1006,
                               1007,
                               1008,
                               1009,
                               999);
        else
          l_rel_tab := num_tab(1001, 1002, 1004, 1006, 1007, 1008, 999);
        end if;
        -- Если передана организация
        -- Проверяем, имеет ли юзер в этой (или вышестоящей) организации нужное право
        select count(*)
          into l_cnt
          from t_user_org tuo
          join t_roles_perm rp
            on rp.rp_role_id = tuo.role_id
          join t_perm_rights pr
            on pr.pr_prm_id = rp.rp_perm_id
           and pr.pr_right_id = pi_right_id
         where tuo.usr_id = pi_worker_id
           and tuo.org_id in
               (select distinct tor.org_id
                  from t_org_relations tor
                connect by tor.org_id = prior tor.org_pid
                       and tor.org_reltype in
                           (select column_value from table(l_rel_tab))
                       and prior tor.org_reltype in
                            (select column_value from table(l_rel_tab))
                 start with tor.org_id = pi_org_id);
      
        if l_cnt > 0 then
          -- Если право есть, возвращаем дерево огранизаций, начиная с переданной
          res := Get_User_Orgs_Tab_With_Param1(pi_worker_id           => pi_worker_id,
                                               pi_org_id              => pi_org_id,
                                               pi_self_include        => pi_self_include,
                                               pi_childrens_include   => pi_childrens_include,
                                               pi_curated_include     => pi_curated_include,
                                               pi_curated_sub_include => pi_curated_sub_include,
                                               pi_curators_include    => pi_curators_include,
                                               pi_tm_1009_include     => pi_tm_1009_include);
        else
          for OrgRec in (SELECT DISTINCT TUO.ORG_ID
                           FROM T_USER_ORG TUO
                           JOIN T_ROLES_PERM RP
                             ON RP.RP_ROLE_ID = TUO.ROLE_ID
                           JOIN T_PERM_RIGHTS PR
                             ON PR.PR_PRM_ID = RP.RP_PERM_ID
                            AND PR.PR_RIGHT_ID = pi_right_id
                           join (SELECT TOR.ORG_ID
                                  FROM T_ORG_RELATIONS TOR
                                CONNECT BY PRIOR TOR.ORG_ID = TOR.ORG_PID
                                 START WITH TOR.ORG_ID = nvl(pi_org_id, 1)) T
                             ON T.ORG_ID = TUO.ORG_ID
                          WHERE PR.PR_RIGHT_ID IS NOT NULL
                            AND TUO.USR_ID = pi_worker_id) loop
            tmp := security_pkg.Get_User_Orgs_Tab_With_Param1(pi_worker_id           => pi_worker_id,
                                                              pi_org_id              => OrgRec.Org_Id,
                                                              pi_self_include        => pi_self_include,
                                                              pi_childrens_include   => pi_childrens_include,
                                                              pi_curated_include     => pi_curated_include,
                                                              pi_curated_sub_include => pi_curated_sub_include,
                                                              pi_curators_include    => pi_curators_include,
                                                              pi_tm_1009_include     => pi_tm_1009_include);
          
            select r.column_value bulk collect
              into tmp2
              from table(res) r
            union
            select t.column_value from table(tmp) t;
            res := tmp2;
          end loop;
        end if;
      end if;
    end if;
    return res;
  end Get_User_Orgs_Tab_By_Right;
  ------------------------------------------------------------------------------------
  -- 50802 Раньше была в ORGS
  ------------------------------------------------------------------------------------
  function Get_User_Orgs_Tab_By_Right_str(pi_worker_id           in T_USERS.USR_ID%type, -- пользователь
                                          pi_str_right_id        in T_RIGHTS.RIGHT_STRING_ID%type,
                                          pi_org_id              in T_ORGANIZATIONS.ORG_ID%type := null, -- организация с которой начинаем
                                          pi_self_include        in pls_integer := 1, -- включать pi_org_id
                                          pi_childrens_include   in pls_integer := 1, -- включать детей
                                          pi_curated_include     in pls_integer := 1, -- включать курируемых
                                          pi_curated_sub_include in pls_integer := 1, -- включать курируемых субдиллерами
                                          pi_curators_include    in pls_integer := 0, -- включать кураторов
                                          pi_tm_1009_include     in pls_integer := 1) -- включать 1009 связь телемаркетинга
  
   return num_tab is
    l_right_id T_RIGHTS.RIGHT_ID%type;
    po_err_num number;
    po_err_msg varchar2(2500);
  begin
    l_right_id := get_right_id_by_str_id(pi_str_right_id);
    return security_pkg.Get_User_Orgs_Tab_By_Right(pi_worker_id,
                                                   l_right_id,
                                                   pi_org_id,
                                                   pi_self_include,
                                                   pi_childrens_include,
                                                   pi_curated_include,
                                                   pi_curated_sub_include,
                                                   pi_curators_include,
                                                   pi_tm_1009_include);
   exception
      when others then
        po_err_num := sqlcode;
        po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
        logging_pkg.error(po_err_num || ', ' || po_err_msg ,
                          c_package || '.' || 'Get_User_Orgs_Tab_By_Right_str');
      return num_tab();                                                                     
  end Get_User_Orgs_Tab_By_Right_str;
  -------------------------------------------------------------
  --Определение регионов пользователя
  -------------------------------------------------------------
  function get_region_by_worker_right2(pi_worker_id in number,
                                       pi_right_str in string_tab,
                                       pi_org_id    in num_tab)
    return ARRAY_NUM_2 is
    org_1001    num_tab;
    org_1009    num_tab;
    --org_1008    num_tab;
    --reg_1008    num_tab;
    reg_1001    num_tab;
    res         ARRAY_NUM_2;
    l_org_right num_tab;
    org_root    num_tab;
    reg_root    num_tab;
    reg_all     num_tab;
    l_cnt       number;
    l_org_tab   num_tab;
    org_dog     num_tab;
  begin
  
    if pi_org_id is null or pi_org_id.count = 0 then
      l_org_tab := num_tab(0);
    else
      l_org_tab := pi_org_id;
    end if;
    --Заглушка для ЕЦОВ
    Select Count(*)
      into l_cnt
      from t_user_org t
      join t_roles_perm rp
        on rp.rp_role_id = t.role_id
      join t_perm_rights pr
        on pr.pr_prm_id = rp.rp_perm_id
      join t_rights rr
        on rr.right_id = pr.pr_right_id
     where t.usr_id = Pi_worker_id
       and rr.right_string_id in (select * from table(pi_right_str))
       and t.org_id in
           (select org_id from t_org_ignore_right where TYPE_ORG = 1);
  
    If l_cnt > 0 then
      Select rec_num_2(tt.org_id, r.reg_id) bulk collect
        into res
        from (select distinct tor.org_id
                from mv_org_tree tor
              Connect by prior tor.org_id = tor.org_pid
               Start with tor.org_id = 2001269) tt
        join t_dic_region r
          on 1 = 1
         and r.mrf_id = 7
         and r.org_id is not null;
    
      return res;
    end if;
    select uo.org_id bulk collect
      into l_org_right
      from t_user_org uo
      join t_roles_perm rp
        on rp.rp_role_id = uo.role_id
      join t_perm_rights pr
        on pr.pr_prm_id = rp.rp_perm_id
      join t_rights rr
        on rr.right_id = pr.pr_right_id
     where uo.usr_id = Pi_Worker_id
       and rr.right_string_id in (select * from table(pi_right_str));
  
    SELECT distinct o_r.org_id bulk collect
      into org_1001
      FROM T_ORG_RELATIONS O_R
     where O_R.ORG_RELTYPE in (1001, 1002, 1003, 1004, 1006, 1007, 999)
       and o_r.dog_id is null
     START WITH O_R.ORG_ID IN (select * from table(l_org_right))
    CONNECT BY PRIOR O_R.ORG_ID = O_R.ORG_PID
           AND O_R.ORG_RELTYPE in (1001, 1002, 1003, 1004, 1006, 1007, 999)
           and (prior nvl(O_R.dog_id, -1) = nvl(O_R.dog_id, -1) or
               prior O_R.dog_id is null or O_R.dog_id is null);
  
    SELECT distinct o_r.org_id bulk collect
      into org_1009
      FROM T_ORG_RELATIONS O_R
     START WITH O_R.ORG_ID IN
                (SELECT distinct o_r.org_id
                   FROM T_ORG_RELATIONS O_R
                  where O_R.ORG_RELTYPE in (1009)
                  START WITH O_R.ORG_pID IN (select * from table(org_1001))
                 CONNECT BY PRIOR O_R.ORG_ID = O_R.ORG_PID
                        AND O_R.ORG_RELTYPE in (1009))
    CONNECT BY PRIOR O_R.ORG_ID = O_R.ORG_PID; 
    
   /*SELECT distinct o_r.org_id bulk collect
      into org_1009
      FROM T_ORG_RELATIONS O_R
     where O_R.ORG_RELTYPE in (1009)
     START WITH O_R.ORG_PID IN (select * from table(org_1001))
    CONNECT BY PRIOR O_R.ORG_ID = O_R.ORG_PID
           AND O_R.ORG_RELTYPE in (1009);*/
    -------------------------------------------
/*    SELECT distinct o_r.org_id bulk collect
      into org_1008
      FROM T_ORG_RELATIONS O_R
     where O_R.ORG_RELTYPE not in (1009)
       and not exists
     (SELECT tor.org_id
              FROM T_ORG_RELATIONS tor
             where tor.ORG_RELTYPE in (1009)
            --and o_r.dog_id is null
             START WITH tor.ORG_ID IN (o_r.org_id)
            CONNECT BY PRIOR tor.ORG_ID = tor.ORG_PID)
     START WITH O_R.ORG_ID IN
                (SELECT distinct o_r.org_id
                   FROM T_ORG_RELATIONS O_R
                  where O_R.ORG_RELTYPE in (1008)
                 --and o_r.dog_id is null
                  START WITH O_R.ORG_ID IN
                             (select * from table(l_org_right))
                 CONNECT BY PRIOR O_R.ORG_ID = O_R.ORG_PID)
    CONNECT BY PRIOR O_R.ORG_ID = O_R.ORG_PID;
  
    select reg.reg_id bulk collect
      into reg_1008
      from table(org_1008) t
      join table(orgs.Get_Reg_by_Org(t.column_value, pi_worker_id)) tab
        on 1 = 1
      join t_dic_region reg
        on reg.reg_id = tab.column_value;*/
    -------------------------------------------
    SELECT distinct o_r.org_id bulk collect
      into org_dog
      FROM T_ORG_RELATIONS O_R
     where O_R.ORG_RELTYPE in (1001, 1002, 1003, 1004, 1006, 1007, 999)
       and o_r.dog_id is not null
     START WITH O_R.ORG_ID IN (select * from table(l_org_right))
    CONNECT BY PRIOR O_R.ORG_ID = O_R.ORG_PID
           AND O_R.ORG_RELTYPE in (1001, 1002, 1003, 1004, 1006, 1007, 999)
           and (prior nvl(O_R.dog_id, -1) = nvl(O_R.dog_id, -1) or
               prior O_R.dog_id is null or O_R.dog_id is null);
  
    select reg.reg_id bulk collect
      into reg_1001
      from table(org_1001) t
      join t_dic_region reg
        on reg.org_id = t.column_value;
  
    if reg_1001 is null or reg_1001.count = 0 then
      select o.region_id bulk collect
        into reg_1001
        from table(org_dog) d
        join t_organizations o
          on o.org_id = d.column_value
       where o.region_id is not null;
    end if;
  
    SELECT distinct o_r.org_id bulk collect
      into org_root
      FROM mv_org_tree O_R
     where O_R.ORG_RELTYPE in (1001, 1002, 1003, 1004, 1006, 1007, 999)
     START WITH O_R.ORG_ID IN (select column_value from table(l_org_tab))
    CONNECT BY PRIOR O_R.ORG_ID = O_R.ORG_PID
           AND O_R.ORG_RELTYPE in (1001, 1002, 1003, 1004, 1006, 1007, 999);
  
    select reg.reg_id bulk collect
      into reg_root
      from table(org_root) t
      join t_dic_region reg
        on reg.org_id = t.column_value;
  
    select r1.column_value bulk collect
      into reg_all
      from table(reg_1001) r1
      join table(reg_root) r2
        on r1.column_value = r2.column_value;
  
    select rec_num_2(org_id, reg_id) bulk collect
      into res
      from (select t1.column_value org_id, null reg_id
              from table(org_1001) t1
            union
            select t2.column_value, reg.column_value
              from table(org_1009) t2
              left join table(reg_all) reg
                on 1 = 1
           /* union
            select t2.column_value, reg.column_value
              from table(org_1008) t2
              join table(reg_1008) reg
                on 1 = 1*/
            union
            select t3.column_value, reg.column_value
              from table(org_dog) t3
              left join table(reg_all) reg
                on 1 = 1);
    return res;
  exception
    when others then
      logging_pkg.error(pi_worker_id || ',' || sqlcode || '.' || sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        c_package || '.' || 'get_region_by_worker_right');
      return null;
  end;
  ---------------------
  procedure Check_Orgs_Enabled(pi_worker_id in T_SESSIONS.ID_WORKER%type) is
    l_count pls_integer := 0;
  begin
  
    select count(*)
      into l_count
      from t_user_org uo, t_organizations o
     where o.is_enabled = 1
       and o.org_id = uo.org_id
       and uo.usr_id = pi_worker_id;
  
    if (l_count = 0) then
      raise ex_org_havent_rights;
    end if;
  end Check_Orgs_Enabled;
  ----------------------------------------------------------------------
  -- Получение текущей сессии пользователя
  --
  -- возвращаемые коды ошибок:
  --  0 (null) - Нет ошибок
  -- -999 - Система работает в режиме ограничений (обновление)
  --  -1 - Внутренняя ошибка (расшифровка - в тексте)
  --  1403 - данные сессии не найдены
  --  2200 - Организация заблокирована
  function Get_User_Session(pi_hash_key  in t_sessions.hash_key%type,
                            pi_system_id in number,
                            po_err_num   out pls_integer,
                            po_err_msg   out varchar2) return sys_refcursor is
    res         sys_refcursor;
    l_worker_id pls_integer := -1;
    l_sys_val   number := 0;
    l_cnt_priv  pls_integer; --для определения,привилегированная ли уч. запись
  begin
    -- Удаляем все старые сессии всех пользователей
    --Kill_Old_User_Sessions();
  
    select max(VALUE)
      into l_sys_val
      from T_SYSTEM_NEEDS
     where TYPE = decode(pi_system_id, 13, 13, 1);
  
    select s.id_worker
      into l_worker_id
      from t_sessions s
     where s.hash_key = pi_hash_key;
  
    update t_sessions s
       set s.reg_date = sysdate
     where s.hash_key = pi_hash_key;
  
    select count(*)
      into l_cnt_priv
      from t_user_org uo, t_organizations o, t_roles r
     where uo.usr_id = l_worker_id
       and uo.org_id = o.org_id
       and o.is_enabled = 1
       and uo.role_id = r.role_id
       and r.is_privilege = 1;
  
    -- Система работает в режиме ограничения доступа (обновление)
    if (l_sys_val = 1 and not l_worker_id in (24793, 958, 1008, 777, 19135) and
       -- 44818
       not Security_pkg.Check_User_Right_str('ADMIN.SYSTEM.USE_DURING_TEST',
                                              l_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      po_err_num := 11;
      po_err_msg := 'worker_id: ' || l_worker_id;
      -- возвращаем пустой курсор
      return null;
    end if;
  
    Check_Orgs_Enabled(l_worker_id);
  
    open res for
      select s.id_worker,
             s.reg_date,
             s.worker_ip,
             s.id_cur_org,
             access_parent_org,             
             s.org_way,
             pi_hash_key session_hash,
             (case
               when l_cnt_priv > 0 then
                1
               else
                0
             end) is_privilege,
             u.date_login_to,
             u.date_pswd_to,
             u.is_temp_passwd,
             l.user_principal_name,
             s.org_way_multiset,
             s.cache_date
        from t_sessions s
        join t_users u
          on u.usr_id = s.id_worker
        left join t_ldapusers l
          on s.id_worker = l.usr_id
       where s.hash_key = pi_hash_key;
  
    return res;
  exception
    when ex_org_havent_rights then
      po_err_num := 2200;
      po_err_msg := 'Организация заблокирована';
      return null;
    when no_data_found then
      po_err_num := sqlcode;
      po_err_msg := 'key: ' || pi_hash_key;
      return null;
    when others then
      po_err_num := -1;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_User_Session;

  ---------------
  --Перевызов, добавлен курсор с правами. для мпз при переходе на дистрибьюцию
  -----------
  function Get_User_Session(pi_hash_key  in t_sessions.hash_key%type,
                            pi_system_id in number,
                            po_right     out sys_refcursor,
                            po_err_num   out pls_integer,
                            po_err_msg   out varchar2) return sys_refcursor is
    --l_user_rigths    num_tab;
    --l_dogovor_rights num_tab;
    --l_rights         num_tab;
    l_is_org_rtmob number;
    l_is_org_rt    number;
    l_worker_id    number;
  begin
    select s.id_worker
      into l_worker_id
      from t_sessions s
     where s.hash_key = pi_hash_key;
  
    select max(case
                 when is_org_rtmob = 1 or is_org_rtmob is null then
                  1
                 else
                  0
               end) is_org_rtmob,
           max(case
                 when is_org_rtmob = 0 or is_org_rtmob is null then
                  1
                 else
                  0
               end) is_org_rt
      into l_is_org_rtmob, l_is_org_rt
      from (select rt.is_org_rtm is_org_rtmob
              from t_user_org t
              join t_org_is_rtmob rt
                on rt.org_id = t.org_id
             where t.usr_id = l_worker_id);
  
    open po_right for
      select right_id, right_string_id
        from t_rights r
        join table(Get_User_Mask_By_Orgs(l_worker_id)) t
          on t.column_value = r.right_id     
      union
      select rr.right_id, rr.right_string_id
        from t_rights rr
       where rr.right_string_id = 'EISSD.IS_ORG_RTM'
         and l_is_org_rtmob = 1
      union
      select rr.right_id, rr.right_string_id
        from t_rights rr
       where rr.right_string_id = 'EISSD.IS_ORG_USI'
         and l_is_org_rt = 1;
  
    return Get_User_Session(pi_hash_key  => pi_hash_key,
                            pi_system_id => pi_system_id,
                            po_err_num   => po_err_num,
                            po_err_msg   => po_err_msg);
  
  exception
    when no_data_found then
      po_err_num := 1;
      po_err_msg := 'Сессия истекла';
      return null;
    when others then
      po_err_num := -1;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  -------------------------------------------------------------------------
  -- Проверка прав (pi_right) пользователя pi_worker_id в организации
  -- pi_org_id (вызывается клиентским приложением)
  function Check_Rights2(pi_right        in T_RIGHTS.RIGHT_ID%type, -- идентификатор права
                         pi_org_id       in T_ORGANIZATIONS.ORG_ID%type, -- идентификатор организации
                         pi_worker_id    in pls_integer, -- идентификатор пользователя
                         pi_is_up        in number, -- ограничивать права пользователя правами родительских или курирующих организаций, если пользователь в них работает
                         pi_is_recursive in number, -- учитывать все права пользователя в курирующих организациях 2-го и более уровней
                         po_err_num      out pls_integer,
                         po_err_msg      out varchar2) return number is
    l_err_code number;
    l_err_msg  varchar2(2000);
  begin
  
    return sys.diutil.bool_to_int(Check_Rights(pi_right,
                                               pi_org_id,
                                               pi_worker_id,
                                               l_err_code,
                                               l_err_msg,
                                               sys.diutil.int_to_bool(pi_is_up),
                                               sys.diutil.int_to_bool(pi_is_recursive)));
  exception
      when others then
        po_err_num := sqlcode;
        po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
        logging_pkg.error(po_err_num || ', ' || po_err_msg ,
                          c_package || '.' || 'Check_Rights2');
        return 0;                                                                 
  
  end;
  ------------------------------------------------------------------------------------
  -- Проверка прав (pi_right) пользователя pi_worker_id в организации pi_org_id
  ------------------------------------------------------------------------------------
  function Check_Rights_str2(pi_str_right_id in T_RIGHTS.RIGHT_STRING_ID%type, -- идентификатор права
                             pi_org_id       in T_ORGANIZATIONS.ORG_ID%type, -- идентификатор организации
                             pi_worker_id    in pls_integer, -- идентификатор пользователя
                             pi_is_up        in number, -- ограничивать права пользователя правами родительских или курирующих организаций, если пользователь в них работает
                             pi_is_recursive in number, -- учитывать все права пользователя в курирующих организациях 2-го и более уровней                             
                             po_err_num      out pls_integer,
                             po_err_msg      out varchar2) return number is
    l_right_id T_RIGHTS.RIGHT_ID%type;
  begin
    l_right_id := get_right_id_by_str_id(pi_str_right_id);
  
    return Check_Rights2(l_right_id,
                         pi_org_id,
                         pi_worker_id,
                         nvl(pi_is_up, 0),
                         nvl(pi_is_recursive, 0),
                         po_err_num,
                         po_err_msg);
  
  end Check_Rights_str2;
  
  function check_has_all_right(pi_tab1 in num_tab, pi_tab2 in num_tab) 
    return number is
    l_intersect_res num_tab;
  begin
    if pi_tab1 is null or pi_tab2 is null then
      return 0;
    end if;
    
    l_intersect_res := pi_tab1 multiset intersect pi_tab2;
    
    if l_intersect_res.count = pi_tab1.count then
      return 1;
    else 
      return 0;
    end if;      
    
  end;
  
  ---------------------------------------------------------
  --Проверка наличия всех перечисленных прав у пользователя
  ---------------------------------------------------------
  function Check_All_Rights_Dog_str(pi_dog_id in number,
                                pi_rights    in string_tab,
                                po_err_num   out number,
                                po_err_msg   out varchar2) return boolean is
    l_str varchar2(4000);
    l_cnt number;
  begin
    select count(*), listagg(q.column_value, ',') WITHIN GROUP(order by 1)
      into l_cnt, l_str
      from table(pi_rights) q
      left join t_rights r
        on r.right_string_id = q.column_value
      left join table(get_dog_rights(pi_dog_id)) t
        on t.column_value = r.right_id
     where t.column_value is null
       and rownum < 39;
  
    if l_cnt = 0 then
      po_err_num:=0;
      return true;
    else
      po_err_num := 1;
      po_err_msg := 'Отсутствуют права: ' || l_str;
      return false;
    end if;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ', ' || po_err_msg,
                        c_package || '.' || 'check_all_rights_str');
      return false;
  end;  
  
  -------------------------------------------------------------
  -- определяет доступные списки нарядов по роли
  -------------------------------------------------------------
  function Get_list_order_by_role(pi_role_id  in number,
                                  po_is_allow out number, -- Признак разрешен/запрещен список: 0 - все кроме; 1 - только в
                                  po_err_num  out number,
                                  po_err_msg  out varchar2) return num_tab is
    res  num_tab;
    l_id number;
  begin
  
    select rloa.id, rloa.is_allow
      into l_id, po_is_allow
      from t_role_list_order_allow rloa
     where rloa.role_id = pi_role_id;
  
    select rlo.list_order_id bulk collect
      into res
      from t_role_list_order rlo
     where rlo.id = l_id;
  
    return res;
  exception
    when no_data_found then
      -- по пользователю отсутствуют доступные списки нарядов
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
    
end security_pkg;
/
