CREATE OR REPLACE PACKAGE BODY ORGS is

  c_package constant varchar2(30) := 'ORGS.';

  c_work_start constant interval day to second := interval '0 00:00:00' day to
                                                  second;
  c_work_end   constant interval day to second := interval '0 23:59:00' day to
                                                  second;
  ex_org_id_invalid exception;
  ex_invalid_argument exception;
  ex_rel_not_found exception;
  ex_wrong_order exception; -- если задан неверный порядок организаций (для add_dogovor)
  ex_wrong_rel exception; -- при попытке содать договор по связи 1001
  ex_wrong_rel_dog exception; -- у курирующей организации должна быть хотя бы одна входящая связь 1004 (кроме подразделений УСИ)
  ex_rel_not_found_dog exception; -- между организациями нет ни одной связи курирования
  ex_cycle exception; -- Нельзя допускать циклические договоры
  ex_usi exception; -- нельзя закючить договор с подразделением принципала
  ex_not_enough_dog_rights exception;
  ex_acc_denied exception; -- договор не найден в области видимости пользователя
  ex_duplicate_phones exception; -- в одной Организации 2 одинаковых телефона
  ex_max_priority exception; -- Превышено значение приоритета
  ex_big_prior exception; -- Введенное значение приоритета идет в отрыв с существующими
  -----------------------------------------------------------------------------
  -- Получение максимального приоритета по организациям в регионе
  -----------------------------------------------------------------------------
  function get_max_priority(pi_org_id in number, pi_max_org in out number)
    return number is
    res number;
  begin

    /*select org_id, priority
      into pi_max_org, res
      from (select o.org_id, osc.priority
              from t_organizations cpo
              join t_address acpo
                on cpo.adr2_id = acpo.addr_id
              join t_address a
                on acpo.addr_code_city = a.addr_code_city
              join t_organizations o
                on a.addr_id = o.adr2_id
              join t_org_ss_center osc
                on osc.org_id = o.org_id
             where cpo.org_id = PI_ORG_ID
               and o.is_ss_center = 1
               and o.is_enabled = 1
               and osc.priority is not null
             order by osc.priority desc)
     where rownum <= 1;*/
     select org_id, priority
       into pi_max_org, res
       from (select o.org_id, osc.priority
               from t_organizations cpo
               join t_address acpo
                 on cpo.adr2_id = acpo.addr_id
               join t_address_object ao
                 on ao.id = acpo.addr_obj_id
               join t_address_object aop
                 on aop.id = case
                      when ao.is_street = 1 then
                       ao.parent_id
                      else
                       ao.id
                    end
                 or aop.parent_id = case
                      when ao.is_street = 1 then
                       ao.parent_id
                      else
                       ao.id
                    end
               join t_address a
                 on a.addr_obj_id = aop.id
               join t_organizations o
                 on a.addr_id = o.adr2_id
                and o.is_ss_center = 1
               join t_org_ss_center osc
                 on osc.org_id = o.org_id
              where cpo.org_id = PI_ORG_ID
                and o.is_enabled = 1
                and osc.priority is not null
              order by osc.priority desc)
      where rownum <= 1;
    return res;
  exception
    when no_data_found then
      -- Нет организаций в регионе => все ок
      return 0;
  end;

  function Change_Org_Relation(pi_org_id   in T_ORGANIZATIONS.ORG_ID%type,
                               pi_org_pid  in T_ORGANIZATIONS.ORG_ID%type,
                               pi_rel_type in T_ORG_RELATIONS.ORG_RELTYPE%type)
    return T_ORG_RELATIONS.ID%type;

  ----------------------------------------------------------------------
  -- 83745. Приватная процедура обновления информации об Организации-ЦПО
  ----------------------------------------------------------------------
  PROCEDURE Update_Ssc_Info(PI_ORG_ID               IN T_ORGANIZATIONS.ORG_ID%type,
                            PI_SSC_PHONES           IN SSC_PHONES_tab,
                            PI_SSC_TIMETABLE        IN ORG_TIMETABLE_TAB,
                            PI_SSC_EMAIL            IN T_ORG_SS_CENTER.email%type,
                            PI_SSC_LATITUDE         IN T_ORG_SS_CENTER.LATITUDE%type,
                            PI_SSC_LONGITUDE        IN T_ORG_SS_CENTER.LONGITUDE%type,
                            PI_SSC_CLOSE_DATE       IN T_ORG_SS_CENTER.CLOSE_DATE%type,
                            PI_SSC_SEGM_SERVICE     IN T_ORG_SS_CENTER.SEGMENT_SERVICE%type,
                            PI_SSC_URL              IN T_ORG_SS_CENTER.URL%type,
                            PI_SS_SERVICE           in SS_SERVICE_TAB,
                            pi_OWNERSHIP            in t_org_ss_center.ownership%type,
                            pi_SQUARE_METER         in t_org_ss_center.square_meter%type,
                            pi_WORKERS_NUMBER       in t_org_ss_center.workers_number%type,
                            PI_METRO                in t_org_ss_center.METRO%type,
                            PI_PRIORITY             in t_org_ss_center.PRIORITY%type,
                            PI_COMMENTS             in t_org_ss_center.COMMENTS%type,
                            pi_worker_id            in number,
                            pi_ssc_cluster          in number, --кластер цпо
                            pi_ssc_fact_district    in varchar2, --регион области 40
                            pi_ssc_temp_close       in org_temp_close_tab, --периоды временного закрытия
                            pi_ssc_date_open        in date, --дата открытия
                            pi_ssc_contact_phone    in string_tab, --Контактный телефон ЦПО
                            pi_ssc_CNT_CASHBOX      in number, --Количество касс (ЕКМ)
                            pi_ssc_employee         in ORG_employee_TAB, --информация о сотрудниках
                            pi_FULL_NAME_SSC        in varchar2, --полное наименование ЦПО
                            pi_ssc_CNT_TERM_RTC     in t_org_ss_center.CNT_TERM_RTC%type, --Количество терминалов РТК
                            pi_ssc_CNT_TERM_AGENT   in t_org_ss_center.Cnt_Term_Agent%type, --Количество терминалов агентов
                            pi_ssc_IS_ELECTRO_QUEUE in t_org_ss_center.is_electro_queue%type, --Признак наличия электронной очереди: 0 - нет, 1 - есть
                            pi_ssc_IS_GOLD_POOL     in t_org_ss_center.is_gold_pool%type --Признак принадлежности к Золотому пулу: 0 - нет, 1 - есть
                            ) IS
    l_count        number;
    l_max_priority number;
    ex_max_priority1 exception;
    ex_big_prior1    exception;
    l_org_id    number;
    l_priority  number;
    l_city_orgs num_tab;

    l_err_num   pls_integer;
    l_err_msg   varchar2(4000);
    l_hst_id    number;
    l_region_id number;
  BEGIN
    -- Приоритеты
    -- Получаем массив ЦПО организаций данного населенного пункта
    /*select o.org_id bulk collect
     into l_city_orgs
     from t_organizations o,
          t_address       a,
          t_organizations cpo,
          t_address       acpo
    where cpo.adr2_id = acpo.addr_id
      and cpo.org_id = PI_ORG_ID
      and acpo.addr_code_city = a.addr_code_city
      and a.addr_id = o.adr2_id
      and o.is_ss_center = 1;*/
    select o.org_id bulk collect
      into l_city_orgs
      from t_organizations cpo
      join t_address acpo
        on cpo.adr2_id = acpo.addr_id
      join t_address_object ao
        on ao.id = acpo.addr_obj_id
      join t_address_object aop
        on aop.id = case
             when ao.is_street = 1 then
              ao.parent_id
             else
              ao.id
           end
        or aop.parent_id = case
             when ao.is_street = 1 then
              ao.parent_id
             else
              ao.id
           end
      join t_address a
        on a.addr_obj_id = aop.id
      join t_organizations o
        on a.addr_id = o.adr2_id
       and o.is_ss_center = 1
     where cpo.org_id = PI_ORG_ID;
    -- Проверяем, изменился ли приоритет организации
    select count(*)
      into l_count
      from T_ORG_SS_CENTER t
     where t.org_id = PI_ORG_ID
       and nvl(t.priority, -1) = PI_PRIORITY;
    -- Обновление приоритета
    if l_count = 0 then
      -- Получаем макимальный приоритет населенного пункта
      l_max_priority := get_max_priority(PI_ORG_ID, l_org_id);
      -- Типа максимальный приоритет самой организации можно менять, а других уже нет,
      -- т.к. тогда к этой прибавится 1 и будет больше 99
      if l_max_priority = 99 and l_org_id <> PI_ORG_ID then
        raise ex_max_priority1;
      end if;
      -- Получаем текущий приоритет организации
      begin
        select osc.priority
          into l_priority
          from t_org_ss_center osc
         where osc.org_id = PI_ORG_ID;
      exception
        when no_data_found then
          l_priority := 0; -- Если организация только создается
      end;
      -- Проверка, что новый приоритет не идет в отрыв с максимальным
      if (l_priority = 0 and PI_PRIORITY > l_max_priority + 1) or
         (l_priority <> 0 and PI_PRIORITY > l_max_priority) then
        raise ex_big_prior1;
      end if;

      if l_priority = 0 then
        -- Создание новой организации
        UPDATE T_ORG_SS_CENTER t
           SET T.PRIORITY = T.PRIORITY + 1
         WHERE T.ORG_ID IN (SELECT * FROM TABLE(L_CITY_ORGS))
           AND T.ORG_ID <> PI_ORG_ID
           AND (PI_PRIORITY = 1 OR
               (PI_PRIORITY > 1 AND T.PRIORITY >= PI_PRIORITY));
      else
        -- Редактирование
        UPDATE T_ORG_SS_CENTER T
           SET T.PRIORITY = T.PRIORITY + 1
         WHERE T.ORG_ID IN (SELECT * FROM TABLE(L_CITY_ORGS))
           AND T.ORG_ID <> PI_ORG_ID
           AND ((PI_PRIORITY = 1 AND T.PRIORITY <= L_PRIORITY) OR
               (PI_PRIORITY > 1 AND T.PRIORITY >= PI_PRIORITY AND
               T.PRIORITY <= L_PRIORITY));
        UPDATE T_ORG_SS_CENTER T
           SET T.PRIORITY = T.PRIORITY - 1
         WHERE T.PRIORITY <= PI_PRIORITY
           AND T.PRIORITY >= L_PRIORITY
           AND T.ORG_ID IN (SELECT * FROM TABLE(L_CITY_ORGS))
           AND T.ORG_ID <> PI_ORG_ID;
      end if;
    END IF;

    if nvl(PI_SSC_CLOSE_DATE, to_date('01.12.2999')) < sysdate then
      --если дата закрытия уже наступила нужно заблокировать организацию
      update t_organizations o
         set o.is_enabled = 0
       where o.org_id = PI_ORG_ID;
    else
      update t_organizations o
         set o.is_enabled = 1
       where o.org_id in
             (select org_id
                from T_ORG_SS_CENTER c
               where c.org_id = PI_ORG_ID
                 and nvl(c.close_date, to_date('01.01.1990')) < sysdate);
    end if;

    -- Обновить запись о доп. информации ЦПО
    UPDATE T_ORG_SS_CENTER
       SET LATITUDE           = PI_SSC_LATITUDE,
           LONGITUDE          = PI_SSC_LONGITUDE,
           CLOSE_DATE         = PI_SSC_CLOSE_DATE,
           SEGMENT_SERVICE    = PI_SSC_SEGM_SERVICE,
           EMAIL              = PI_SSC_EMAIL,
           URL                = PI_SSC_URL,
           OWNERSHIP          = pi_OWNERSHIP,
           SQUARE_METER       = pi_SQUARE_METER,
           WORKERS_NUMBER     = pi_WORKERS_NUMBER,
           METRO              = PI_METRO,
           PRIORITY           = PI_PRIORITY,
           COMMENTS           = PI_COMMENTS,
           open_date          = pi_ssc_date_open,
           cnt_cashbox        = pi_ssc_CNT_CASHBOX,
           addr_fact_district = pi_ssc_fact_district,
           cluster_id         = pi_ssc_cluster,
           FULL_NAME_SSC      = pi_FULL_NAME_SSC,
           IS_ELECTRO_QUEUE   = pi_ssc_IS_ELECTRO_QUEUE,
           IS_GOLD_POOL       = pi_ssc_IS_GOLD_POOL,
           CNT_TERM_RTC       = pi_ssc_CNT_TERM_RTC,
           CNT_TERM_AGENT     = pi_ssc_CNT_TERM_AGENT
     WHERE ORG_ID = PI_ORG_ID;

    -- Проверить, не нужно ли вставить новую запись
    IF SQL%ROWCOUNT = 0 THEN
      -- Вставить запись с доп. информацией о ЦПО
      INSERT INTO T_ORG_SS_CENTER
        (ORG_ID,
         LATITUDE,
         LONGITUDE,
         CLOSE_DATE,
         SEGMENT_SERVICE,
         EMAIL,
         URL,
         IS_ENABLED,
         OWNERSHIP,
         SQUARE_METER,
         WORKERS_NUMBER,
         METRO,
         PRIORITY,
         COMMENTS,
         open_DATE,
         CNT_CASHBOX,
         ADDR_FACT_DISTRICT,
         cluster_id,
         FULL_NAME_SSC,
         IS_ELECTRO_QUEUE,
         IS_GOLD_POOL,
         CNT_TERM_RTC,
         CNT_TERM_AGENT)
      VALUES
        (PI_ORG_ID,
         PI_SSC_LATITUDE,
         PI_SSC_LONGITUDE,
         PI_SSC_CLOSE_DATE,
         PI_SSC_SEGM_SERVICE,
         PI_SSC_EMAIL,
         PI_SSC_URL,
         1,
         pi_OWNERSHIP,
         pi_SQUARE_METER,
         pi_WORKERS_NUMBER,
         PI_METRO,
         PI_PRIORITY,
         PI_COMMENTS,
         pi_ssc_date_open,
         pi_ssc_CNT_CASHBOX,
         pi_ssc_fact_district,
         pi_ssc_cluster,
         pi_FULL_NAME_SSC,
         pi_ssc_IS_ELECTRO_QUEUE,
         pi_ssc_IS_GOLD_POOL,
         pi_ssc_CNT_TERM_RTC,
         pi_ssc_CNT_TERM_AGENT);
    END IF;

    -- Обновить информацию в таблице T_SSC_TIMETABLE
    IF PI_SSC_TIMETABLE IS NOT NULL AND PI_SSC_TIMETABLE.COUNT != 0 THEN
      insert_TIMETABLE(PI_ORG_ID,
                       PI_SSC_TIMETABLE,
                       pi_worker_id,
                       l_err_num,
                       l_err_msg);
    END IF;

    IF PI_SSC_PHONES IS NOT NULL AND PI_SSC_PHONES.count != 0 THEN
      --контактный телефон ЦПО для портала
      DELETE FROM T_ORG_SSC_PHONE oph WHERE oph.ORG_ID = PI_ORG_ID;
      INSERT INTO T_ORG_SSC_PHONE
        (ORG_ID, PHONE_NUMBER, SEGMENT_SERVICE)
        SELECT PI_ORG_ID, ph.PHONE_NUMBER, ph.SEGMENT_SERVICE
          FROM TABLE(PI_SSC_PHONES) ph;
    END IF;

    IF pi_ss_service IS NOT NULL AND pi_ss_service.count != 0 THEN
      -- Обновить информацию
      DELETE FROM t_Org_Ss_Service t WHERE t.ORG_ID = PI_ORG_ID;

      INSERT INTO t_Org_Ss_Service
        (ORG_ID, Ss_Service, ss_service_pos, SEGMENT_SERVICE)
        SELECT PI_ORG_ID, t.SERVICE_ID, t.SERVICE_POS, t.SEGMENT_SERVICE
          FROM TABLE(pi_ss_service) t;
    END IF;

    --контактный телефон ЦПО
    if pi_ssc_contact_phone is not null then
      delete from t_org_ssc_contact_phone p where p.org_id = PI_ORG_ID;
      insert into t_org_ssc_contact_phone
        (org_id, phone)
        select PI_ORG_ID, t.column_value
          from table(pi_ssc_contact_phone) t;
    end if;

    --периоды временного закрытия
    if pi_ssc_temp_close is not null then
      --опеределим регион для формирования пермского времени
      select o.region_id
        into l_region_id
        from t_organizations o
       where o.org_id = PI_ORG_ID;

      delete from t_org_ssc_temp_close t where t.org_id = PI_ORG_ID;
      insert into t_org_ssc_temp_close
        (org_id, date_start, date_end, reason_close)
        select PI_ORG_ID,
               t.date_start +
               regions.get_region_offset_time(l_region_id) / 24,
               t.date_end +
               regions.get_region_offset_time(l_region_id) / 24,
               t.reason_close
          from table(pi_ssc_temp_close) t;
    end if;

    --информация о сотрудниках
    if pi_ssc_employee is not null then
      delete from t_org_ssc_employee s where s.org_id = PI_ORG_ID;
      insert into t_org_ssc_employee
        (org_id, type_employee, name, phone)
        select PI_ORG_ID, t.type_position, t.people_name, t.people_phone
          from table(pi_ssc_employee) t;
    end if;
    --Запишем в историю
    /* l_hst_id := save_org_hst(pi_worker_id => pi_worker_id,
    pi_org_id    => PI_ORG_ID);*/

  EXCEPTION
    when ex_max_priority1 then
      raise ex_max_priority;
    when ex_big_prior1 then
      raise ex_big_prior;
    WHEN DUP_VAL_ON_INDEX THEN
      logging_pkg.error(sqlcode || ' ' || sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'Update_Ssc_Info');
      RAISE ex_duplicate_phones;
  END;
  -----------------------------------------------------------------------------
  --Добавление режима работы
  -----------------------------------------------------------------------------
  procedure insert_TIMETABLE(PI_ORG_ID        IN T_ORGANIZATIONS.ORG_ID%type,
                             PI_SSC_TIMETABLE IN ORG_TIMETABLE_TAB,
                             pi_worker_id     in number,
                             po_err_num       out pls_integer,
                             po_err_msg       out varchar2) is
  begin
    MERGE INTO T_ORG_SSC_TIMETABLE tt
    USING (SELECT p.DAY_NUMBER,
                  p.WORK_START,
                  p.WORK_END,
                  p.FULLTIME,
                  p.BREAK_START,
                  p.BREAK_END,
                  p.WITHOUT_BREAK,
                  p.IS_ENABLED
             FROM TABLE(PI_SSC_TIMETABLE) p) ptt
    ON (tt.DAY_NUMBER = ptt.DAY_NUMBER AND tt.ORG_ID = PI_ORG_ID)
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
                              END WHEN NOT MATCHED THEN INSERT(tt.ORG_ID, tt.IS_ENABLED, tt.DAY_NUMBER, tt.WORK_START, tt.WORK_END, tt.BREAK_START, tt.BREAK_END) VALUES(PI_ORG_ID, ptt.IS_ENABLED, ptt.DAY_NUMBER,CASE
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
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'orgs.insert_TIMETABLE');
  end;
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
    return T_ORGANIZATIONS.ORG_ID%type is
    l_addr_jur_id      number;
    l_addr_fact_id     number;
    l_resp_id          number;
    l_touch_id         number;
    l_count            pls_integer;
    l_org_id           T_ORGANIZATIONS.ORG_ID%type := -1;
    dummy              T_ORG_RELATIONS.ID%type := -1;
    l_root_region      t_organizations.region_id%type;
    l_root_is_pay_espp number;
    ex_is_pay_espp exception;
    parent_is_ssc  exception;
    l_org_region      number;
    l_org_fact_region number;
    ex_pi_priority exception;
    L_MAX_PRIORITY NUMBER;
    l_max_org_id   number;
  begin
    savepoint sp_Ins_Org;
    if PI_IS_SS_CENTER = 1 then
      if (not Security_pkg.Check_User_Right_str('EISSD.CPO.EDIT.ORG',
                                                pi_worker_id,
                                                po_err_num,
                                                po_err_msg)) then
        return null;
      end if;
    elsif (not Security_pkg.Check_User_Right_str('EISSD.ORGS.CREATE',
                                                 pi_worker_id,
                                                 po_err_num,
                                                 po_err_msg)) then
      return null;
    end if;

    if pi_is_pay_espp = -1 then
      raise ex_is_pay_espp;
    end if;

    if PI_PRIORITY is null and PI_IS_SS_CENTER = 1 then
      raise ex_pi_priority;
    end if;

    -- 83754. Проверим, что Организация-родитель - не ЦПО
    IF pi_parent_id is not null THEN
      select count(*)
        into l_count
        from T_ORGANIZATIONS org
       WHERE org.org_id = pi_parent_id
         AND org.IS_SS_CENTER = 1;

      if l_count != 0 then
        raise parent_is_ssc;
      end if;
    END IF;

    begin
      SELECT dr.reg_id
        INTO l_org_region
        FROM T_DIC_REGION dr
       WHERE dr.KL_REGION = pi_org_address.KL_region;
    exception
      when no_data_found then
        l_org_region := null;
    end;

    begin
      SELECT dr.reg_id
        INTO l_org_fact_region
        FROM T_DIC_REGION dr
       WHERE dr.KL_REGION = pi_org_fact_address.KL_region;
    exception
      when no_data_found then
        l_org_fact_region := null;
    end;

    -- add juristic address
    l_addr_jur_id := addresse_pkg.Ins_Address(pi_country        => null,
                                              pi_city           => pi_org_address.addr_city.ADDR_NAME,
                                              pi_index          => pi_org_post_index,
                                              pi_street         => pi_org_address.addr_street.ADDR_NAME,
                                              pi_building       => pi_org_address.addr_house.house_number,
                                              pi_office         => pi_org_address.ADDR_OFFICE,
                                              pi_corp           => null,
                                              pi_city_code      => null,
                                              pi_city_lvl       => null,
                                              pi_street_code    => null,
                                              pi_street_lvl     => null,
                                              pi_house_code     => null,
                                              pi_addr_Oth       => null,
                                              pi_region         => l_org_region,
                                              pi_addr_block     => null,
                                              pi_addr_structure => null,
                                              pi_addr_fraction  => null,
                                              pi_addr_obj_id    => nvl(pi_org_address.addr_street.addr_local_code,
                                                                       pi_org_address.addr_city.addr_local_code),
                                              pi_house_obj_id   => pi_org_address.addr_house.house_local_code);

    -- add fact address
    l_addr_fact_id := addresse_pkg.Ins_Address(pi_country        => null,
                                               pi_city           => pi_org_fact_address.addr_city.ADDR_NAME,
                                               pi_index          => pi_org_fact_post_index,
                                               pi_street         => pi_org_fact_address.addr_street.ADDR_NAME,
                                               pi_building       => pi_org_fact_address.addr_house.house_number,
                                               pi_office         => pi_org_fact_address.ADDR_OFFICE,
                                               pi_corp           => null,
                                               pi_city_code      => null,
                                               pi_city_lvl       => null,
                                               pi_street_code    => null,
                                               pi_street_lvl     => null,
                                               pi_house_code     => null,
                                               pi_addr_Oth       => null,
                                               pi_region         => l_org_fact_region,
                                               pi_addr_block     => null,
                                               pi_addr_structure => null,
                                               pi_addr_fraction  => null,
                                               pi_addr_obj_id    => nvl(pi_org_fact_address.addr_street.addr_local_code,
                                                                        pi_org_fact_address.addr_city.addr_local_code),
                                               pi_house_obj_id   => pi_org_fact_address.addr_house.house_local_code);

    -- insert responsible person
    l_resp_id := client_pkg.save_person(pi_worker_id     => pi_worker_id,
                                        pi_first_name    => pi_resp.first_name,
                                        pi_last_name     => pi_resp.last_name,
                                        pi_middle_name   => pi_resp.middle_name,
                                        pi_sex           => pi_resp.sex,
                                        pi_is_resident   => null,
                                        pi_birth_day     => pi_resp.birth_day,
                                        pi_birthplace    => pi_resp.birthplace,
                                        pi_email         => pi_resp.email,
                                        pi_phone         => pi_resp.phone,
                                        pi_phone_home    => null,
                                        pi_document_type => pi_resp.document_type,
                                        pi_reg_address   => null,
                                        pi_fact_address  => null,
                                        pi_DLV_ACC_TYPE  => null,
                                        pi_inn           => pi_resp_inn,
                                        po_err_num       => po_err_num,
                                        po_err_msg       => po_err_msg);
    if nvl(po_err_num, 0) != 0 then
      po_err_num := 1;
      po_err_msg := 'Ошибка при сохранении ответственного лица';
      return null;
    end if;

    -- insert contact person
    l_touch_id := client_pkg.save_person(pi_worker_id     => pi_worker_id,
                                         pi_first_name    => pi_touch.first_name,
                                         pi_last_name     => pi_touch.last_name,
                                         pi_middle_name   => pi_touch.middle_name,
                                         pi_sex           => pi_touch.sex,
                                         pi_is_resident   => null,
                                         pi_birth_day     => null,
                                         pi_birthplace    => null,
                                         pi_email         => pi_touch.email,
                                         pi_phone         => pi_touch.phone,
                                         pi_phone_home    => null,
                                         pi_document_type => null,
                                         pi_reg_address   => null,
                                         pi_fact_address  => null,
                                         pi_DLV_ACC_TYPE  => null,
                                         pi_inn           => null,
                                         po_err_num       => po_err_num,
                                         po_err_msg       => po_err_msg);
    if nvl(po_err_num, 0) != 0 then
      po_err_num := 1;
      po_err_msg := 'Ошибка при сохранении контактного лица';
      return null;
    end if;

    -- insert organization
    insert into T_ORGANIZATIONS
      (ORG_TYPE,
       ORG_NAME,
       ORG_OGRN,
       ADR1_ID,
       ADR2_ID,
       RESP_ID,
       TOUCH_ID,
       ORG_SETTL_ACCOUNT,
       ORG_CON_ACCOUNT,
       ORG_KPP,
       ORG_BIK,
       ORG_OKPO,
       ORG_OKONX,
       REGION_ID,
       ORG_COMMENT,
       EMAIL,
       ORG_INN,
       V_LICE,
       NA_OSNOVANII,
       worker_id_create,
       org_full_name,
       v_lice_podkl,
       na_osnovanii_podkl,
       is_pay_espp,
       boss_name,
       type_org,
       is_stamp,
       is_with_rekv,
       is_with_ip,
       IS_WITH_PERSONAL_INFO,
       start_date,
       USE_CHILD_REQ,
       org_buy,
       BANK_NAME,
       RASH_INN,
       RASH_BIK,
       RASH_SETTL_ACCOUNT,
       RASH_CON_ACCOUNT,
       RASH_BANK_NAME,
       RASH_OKPO,
       RASH_OKONX,
       RASH_KPP,
       IS_SS_CENTER,
       --UNRESERVED_TMC,
       ERP_R12_NUM)
    values
      (pi_org_type,
       pi_org_name,
       pi_org_ogrn,
       l_addr_jur_id,
       l_addr_fact_id,
       l_resp_id,
       l_touch_id,
       pi_income_account.Checking_account,
       pi_income_account.Corr_account,
       pi_income_account.KPP,
       pi_income_account.BIK,
       pi_income_account.OKPO,
       pi_income_account.OKONX,
       pi_org_region_id,
       pi_comment,
       pi_org_email,
       pi_income_account.INN,
       pi_v_lice,
       pi_na_osnovanii,
       pi_worker_id,
       pi_org_full_name,
       pi_v_lice_podkl,
       pi_na_osnovanii_podkl,
       pi_is_pay_espp,
       pi_boss_name,
       pi_type_org,
       pi_is_stamp,
       pi_is_with_rekv,
       pi_is_with_ip,
       pi_IS_WITH_PERSONAL_INFO,
       pi_start_date,
       PI_USE_CHILD_REQ,
       pi_org_buy,
       pi_income_account.BANK_NAME,
       pi_expense_account.INN,
       pi_expense_account.BIK,
       pi_expense_account.Checking_account,
       pi_expense_account.Corr_account,
       pi_expense_account.BANK_NAME,
       pi_expense_account.OKPO,
       pi_expense_account.OKONX,
       pi_expense_account.KPP,
       PI_IS_SS_CENTER,
       --PI_UNRESERVED_TMC,
       pi_erp_r12_num)
    returning ORG_ID into l_org_id;

    if PI_UNRESERVED_TMC is not null and PI_UNRESERVED_TMC.Count != 0 and PI_UNRESERVED_TMC(1)
      .number_1 is not null then
      insert into t_org_tmc_unreserv
        (org_id, tmc_type, cou)
        select l_org_id, number_1, number_2 from table(PI_UNRESERVED_TMC);
      --записываем в историю
      insert into t_org_tmc_unreserv_hst
        (org_id, tmc_type, cou)
        select t.org_id, t.tmc_type, t.cou
          from t_org_tmc_unreserv t
         where t.org_id = l_org_id;
    end if;

    -- 83745. Если Организация - ЦПО, создадим записи в соответствующих таблицах
    IF PI_IS_SS_CENTER = 1 THEN
      UPDATE_SSC_INFO(PI_ORG_ID               => l_org_id,
                      PI_SSC_PHONES           => PI_SSC_PHONES,
                      PI_SSC_TIMETABLE        => PI_SSC_TIMETABLE,
                      PI_SSC_EMAIL            => PI_SSC_EMAIL,
                      PI_SSC_LATITUDE         => PI_SSC_LATITUDE,
                      PI_SSC_LONGITUDE        => PI_SSC_LONGITUDE,
                      PI_SSC_CLOSE_DATE       => PI_SSC_CLOSE_DATE,
                      PI_SSC_SEGM_SERVICE     => PI_SSC_SEGM_SERVICE,
                      PI_SSC_URL              => PI_SSC_URL,
                      pi_ss_service           => pi_ss_service,
                      pi_OWNERSHIP            => pi_OWNERSHIP,
                      pi_SQUARE_METER         => pi_SQUARE_METER,
                      pi_WORKERS_NUMBER       => pi_WORKERS_NUMBER,
                      PI_METRO                => PI_METRO,
                      PI_PRIORITY             => PI_PRIORITY,
                      PI_COMMENTS             => PI_COMMENTS,
                      pi_worker_id            => pi_worker_id,
                      pi_ssc_cluster          => pi_ssc_cluster,
                      pi_ssc_fact_district    => pi_ssc_fact_district,
                      pi_ssc_temp_close       => pi_ssc_temp_close,
                      pi_ssc_date_open        => pi_ssc_date_open,
                      pi_ssc_contact_phone    => pi_ssc_contact_phone,
                      pi_ssc_CNT_CASHBOX      => pi_ssc_CNT_CASHBOX,
                      pi_ssc_employee         => pi_ssc_employee,
                      pi_FULL_NAME_SSC        => pi_FULL_NAME_SSC,
                      pi_ssc_CNT_TERM_RTC     => pi_ssc_CNT_TERM_RTC,
                      pi_ssc_CNT_TERM_AGENT   => pi_ssc_CNT_TERM_AGENT,
                      pi_ssc_IS_ELECTRO_QUEUE => pi_ssc_IS_ELECTRO_QUEUE,
                      pi_ssc_IS_GOLD_POOL     => pi_ssc_IS_GOLD_POOL);
    elsif PI_IS_SS_CENTER <> 1 and PI_SSC_TIMETABLE.Count > 0 and PI_SSC_TIMETABLE(1)
         .day_number is not null then
      insert_TIMETABLE(l_org_id,
                       PI_SSC_TIMETABLE,
                       pi_worker_id,
                       po_err_num,
                       po_err_msg);
    END IF;

    -- Проставляем и у дочерних PI_USE_CHILD_REQ
    if PI_USE_CHILD_REQ is not null then
      update t_organizations o
         set o.use_child_req = PI_USE_CHILD_REQ
       where o.org_id in (select /*+ PRECOMPUTE_SUBQUERY */
                           tor.org_id
                            from mv_org_tree tor
                          connect by prior tor.org_id = tor.org_pid
                           start with tor.org_id = l_org_id);
    end if;

    -- add relation
    dummy := Add_Org_Relation(l_org_id, pi_parent_id, c_rel_tp_parent);

    -- флаг is_pay_espp курирующей организации 22734
    begin
      select distinct o.is_pay_espp
        into l_root_is_pay_espp
        from mv_org_tree m, t_organizations o
       where o.org_id = m.root_org_id
         and m.org_id = l_org_id;
    exception
      when others then
        l_root_is_pay_espp := 0;
    end;

    update t_organizations o
       set o.root_org_id =
           (Select max(org_id)
              from (Select tor.org_id, tor.org_pid, tor.org_reltype
                      from t_org_relations tor
                    Connect by prior tor.org_pid = tor.org_id
                           and tor.org_reltype = 1001
                     Start with tor.org_id = l_org_id
                            and tor.org_reltype = 1001)
             where org_pid = -1),
           o.is_pay_espp = decode(l_root_is_pay_espp,
                                  1,
                                  l_root_is_pay_espp,
                                  pi_is_pay_espp) -- 22734
     where o.org_id = l_org_id;

    -- e.komissarov 12/10/2009 Прверим регион создаваемой и рутовой организации
    select root_org.region_id
      into l_root_region
      from t_organizations root_org
     where root_org.org_id =
           (select org.root_org_id
              from t_organizations org
             where org.org_id = l_org_id);

    if l_root_region <> -1 and l_root_region <> pi_org_region_id then
      po_err_num := -1002;
      po_err_msg := 'Регион организации не совпадает с регионом родительской организации';
      rollback to sp_Ins_Org;
      return - 1;
    end if;

    -- add roles
    if ((pi_parent_id is null) or (pi_parent_id = -1)) then
      Security.Create_Roles_From_Tpl(l_org_id);
    end if;

    -- add calc centers
    if pi_org_region_id is not null then
      insert into T_ORG_CALC_CENTER
        (ORG_ID, CC_ID)
        (select distinct l_org_id, CC.CC_ID
           from t_CALC_CENTER CC
           join t_dic_mvno_region r
             on r. id = cc.cc_region_id
          where --cc.cc_region_id = pi_org_region_id
          r.reg_id = pi_org_region_id
      and cc.cc_is_ps = 1);
    end if;

    -- 45194 Передача ИД организации в CRM CMS
    -- Раскомментировать, когда CRM CMS реализуют протокол
    --    CMS_OBR.sync_orgs(l_org_id, pi_org_name, pi_parent_id);
    --56130
    if pi_channel_tab is not null then

      insert into t_org_channels
        (org_id, channel_id, default_type)
        select l_org_id, number_1, number_2 from table(pi_channel_tab);

      insert into t_org_channels_hst
        select seq_ORG_CHANNELS_HST.nextval, pi_worker_id, sysdate, ch.*
          from t_org_channels ch
         where ch.org_id = l_org_id;
    end if;
    -- Нужны на боевой (69654)
    logging_pkg.info('l_org_id' || l_org_id, 'Ins_Org');
    return l_org_id;
  exception
    when ex_is_pay_espp then
      po_err_num := -1;
      po_err_msg := 'is_pay_espp = -1';
      rollback to sp_Ins_Org;
      return null;
    when dup_val_on_index then
      po_err_num := SQLCODE;
      po_err_msg := 'Нарушение уникальности идеинтификатора организации в таблице связей.';
      rollback to sp_Ins_Org;
      -- Нужны на боевой (69654)
      logging_pkg.error('l_org_id=' || l_org_id || 'pi_parent_id=' ||
                        pi_parent_id || sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'Ins_Org');
      return l_org_id;
    when parent_is_ssc then
      po_err_num := 1002;
      po_err_msg := 'Для Организаций-ЦПО регистрация нижестоящих Организаций запрещена.';
      rollback to sp_Ins_Org;
      return null;
    WHEN ex_duplicate_phones then
      po_err_num := 1003;
      po_err_msg := 'В одной Организации-ЦПО зарегистрировано 2 одинаковых номера телефона.';
      rollback to sp_Ins_Org;
      return null;
    when ex_invalid_argument then
      po_err_num := 1001;
      po_err_msg := 'Неверное значение идеинтификатора организации (org_id=' ||
                    l_org_id || ').';
      rollback to sp_Ins_Org;
      -- Нужны на боевой (69654)
      logging_pkg.error('l_org_id' || l_org_id, 'Ins_Org');
      return l_org_id;
    when ex_pi_priority then
      po_err_num := 1004;
      po_err_msg := 'Поле "Приоритет" должно быть заполнено.';
      rollback to sp_Change_Org;
      return null;
    when ex_max_priority then
      po_err_num := 1005;
      po_err_msg := 'Превышено максимальное значение приоритета.';
      rollback to sp_Ins_Org;
      return null;
    when ex_big_prior then
      L_MAX_PRIORITY := get_max_priority(l_org_id, l_max_org_id) + 1;
      po_err_num     := 1006;
      po_err_msg     := 'Значение приоритета не должно превышать ' ||
                        L_MAX_PRIORITY;
      rollback to sp_Ins_Org;
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      rollback to sp_Ins_Org;
      -- Нужны на боевой (69654)
      logging_pkg.error('l_org_id' || l_org_id, 'Ins_Org');
      return l_org_id;
  end;
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
    return T_ORGANIZATIONS.ORG_ID%type is
    res            T_ORGANIZATIONS.ORG_ID%type;
    l_addr_jur_id  number;
    l_addr_fact_id number;
    l_resp_id      pls_integer;
    l_touch_id     pls_integer;
    l_count        pls_integer;
    cou            pls_integer := 0;
    dummy          T_ORG_RELATIONS.ID%type := -1;
    ex_is_pay_espp exception;
    parent_is_ssc exception;
    ssc_have_children exception;
    l_org_region      number;
    l_org_fact_region number;
    ex_pi_priority exception;
    L_MAX_PRIORITY NUMBER;
    l_max_org_id   number;
    l_is_fedagent  number;
    tab_org_change num_tab;
  begin
    savepoint sp_Change_Org;
    logging_pkg.debug(pi_org_id || ';' || pi_org_name || ';' ||
                      pi_parent_id || ';' || pi_erp_r12_num,
                      'Change_Org');

    if PI_IS_SS_CENTER = 1 then
      if (not Security_pkg.Check_Rights_str('EISSD.CPO.EDIT.ORG',
                                            pi_org_id,
                                            pi_worker_id,
                                            po_err_num,
                                            po_err_msg,
                                            true)) then
        return null;
      end if;

    elsif (pi_org_id is not null) then
      if (not Security_pkg.Check_Rights_str('EISSD.ORGS.EDIT',
                                            pi_org_id,
                                            pi_worker_id,
                                            po_err_num,
                                            po_err_msg,
                                            true)) then
        return null;
      end if;

    else
      if (not Security_pkg.Check_Rights_str('EISSD.ORGS.CREATE',
                                            pi_org_id,
                                            pi_worker_id,
                                            po_err_num,
                                            po_err_msg)) then
        return null;
      end if;
    end if;

    if pi_org_id is not null then
      select COUNT(*)
        into cou
        from T_ORGANIZATIONS O
       where O.ORG_ID = pi_org_id;
      if cou = 0 then
        raise ex_invalid_argument;
      end if;
    end if;

    if pi_is_pay_espp = -1 then
      raise ex_is_pay_espp;
    end if;

    if PI_PRIORITY is null and PI_IS_SS_CENTER = 1 then
      raise ex_pi_priority;
    end if;

    -- 83754. Проверим, что Организация-родитель - не ЦПО
    IF pi_parent_id is not null THEN
      select count(*)
        into l_count
        from T_ORGANIZATIONS org
       WHERE org.org_id = pi_parent_id
         AND org.IS_SS_CENTER = 1;

      if l_count != 0 then
        raise parent_is_ssc;
      end if;
    END IF;

    -- 83754. Проверим, что у Организации-ЦПО нет нижележащих Организаций.
    IF PI_IS_SS_CENTER = 1 THEN
      SELECT COUNT(*)
        INTO L_COUNT
        FROM T_ORG_RELATIONS rel
       WHERE rel.ORG_PID = pi_org_id
         AND rel.ORG_RELTYPE = c_rel_tp_parent;

      IF L_COUNT != 0 THEN
        raise ssc_have_children;
      END IF;
    END IF;

    begin
      SELECT dr.reg_id
        INTO l_org_region
        FROM T_DIC_REGION dr
       WHERE dr.KL_REGION = pi_org_address.KL_region;
    exception
      when no_data_found then
        l_org_region := null;
    end;

    begin
      SELECT dr.reg_id
        INTO l_org_fact_region
        FROM T_DIC_REGION dr
       WHERE dr.KL_REGION = pi_org_fact_address.KL_region;
    exception
      when no_data_found then
        l_org_fact_region := null;
    end;
    --проверка на федагента
    select count(*)
      into l_is_fedagent
      from t_org_relations t
      join t_dic_mrf m
        on m.org_id = t.org_pid
     where t.org_id = pi_org_id
       and exists
     (select 1
              from t_org_relations r2
             where r2.org_reltype = 1006
               and r2.org_id in (select r1.org_id
                                   from t_org_relations r1
                                  where r1.org_reltype = 1001
                                    and r1.org_pid = t.org_id))
       and t.org_id > 2000100;
    -- add juristic address
    l_addr_jur_id := addresse_pkg.Ins_Address(pi_country        => null,
                                              pi_city           => pi_org_address.addr_city.ADDR_NAME,
                                              pi_index          => pi_org_post_index,
                                              pi_street         => pi_org_address.addr_street.ADDR_NAME,
                                              pi_building       => pi_org_address.addr_house.house_number,
                                              pi_office         => pi_org_address.ADDR_OFFICE,
                                              pi_corp           => null,
                                              pi_city_code      => null,
                                              pi_city_lvl       => null,
                                              pi_street_code    => null,
                                              pi_street_lvl     => null,
                                              pi_house_code     => null,
                                              pi_addr_Oth       => null,
                                              pi_region         => l_org_region,
                                              pi_addr_block     => null,
                                              pi_addr_structure => null,
                                              pi_addr_fraction  => null,
                                              pi_addr_obj_id    => nvl(pi_org_address.addr_street.addr_local_code,
                                                                       pi_org_address.addr_city.addr_local_code),
                                              pi_house_obj_id   => pi_org_address.addr_house.house_local_code);

    -- add fact address
    l_addr_fact_id := addresse_pkg.Ins_Address(pi_country        => null,
                                               pi_city           => pi_org_fact_address.addr_city.ADDR_NAME,
                                               pi_index          => pi_org_fact_post_index,
                                               pi_street         => pi_org_fact_address.addr_street.ADDR_NAME,
                                               pi_building       => pi_org_fact_address.addr_house.house_number,
                                               pi_office         => pi_org_fact_address.ADDR_OFFICE,
                                               pi_corp           => null,
                                               pi_city_code      => null,
                                               pi_city_lvl       => null,
                                               pi_street_code    => null,
                                               pi_street_lvl     => null,
                                               pi_house_code     => null,
                                               pi_addr_Oth       => null,
                                               pi_region         => l_org_fact_region,
                                               pi_addr_block     => null,
                                               pi_addr_structure => null,
                                               pi_addr_fraction  => null,
                                               pi_addr_obj_id    => nvl(pi_org_fact_address.addr_street.addr_local_code,
                                                                        pi_org_fact_address.addr_city.addr_local_code),
                                               pi_house_obj_id   => pi_org_fact_address.addr_house.house_local_code);

    -- insert responsible person
    l_resp_id := client_pkg.save_person(pi_worker_id     => pi_worker_id,
                                        pi_first_name    => pi_resp.first_name,
                                        pi_last_name     => pi_resp.last_name,
                                        pi_middle_name   => pi_resp.middle_name,
                                        pi_sex           => pi_resp.sex,
                                        pi_is_resident   => null,
                                        pi_birth_day     => pi_resp.birth_day,
                                        pi_birthplace    => pi_resp.birthplace,
                                        pi_email         => pi_resp.email,
                                        pi_phone         => pi_resp.phone,
                                        pi_phone_home    => null,
                                        pi_document_type => pi_resp.document_type,
                                        pi_reg_address   => null,
                                        pi_fact_address  => null,
                                        pi_DLV_ACC_TYPE  => null,
                                        pi_inn           => pi_resp_inn,
                                        po_err_num       => po_err_num,
                                        po_err_msg       => po_err_msg);
    if nvl(po_err_num, 0) != 0 then
      po_err_num := 1;
      po_err_msg := 'Ошибка при сохранении ответственного лица';
      return null;
    end if;

    -- insert contact person
    l_touch_id := client_pkg.save_person(pi_worker_id     => pi_worker_id,
                                         pi_first_name    => pi_touch.first_name,
                                         pi_last_name     => pi_touch.last_name,
                                         pi_middle_name   => pi_touch.middle_name,
                                         pi_sex           => pi_touch.sex,
                                         pi_is_resident   => null,
                                         pi_birth_day     => null,
                                         pi_birthplace    => null,
                                         pi_email         => pi_touch.email,
                                         pi_phone         => pi_touch.phone,
                                         pi_phone_home    => null,
                                         pi_document_type => null,
                                         pi_reg_address   => null,
                                         pi_fact_address  => null,
                                         pi_DLV_ACC_TYPE  => null,
                                         pi_inn           => null,
                                         po_err_num       => po_err_num,
                                         po_err_msg       => po_err_msg);
    if nvl(po_err_num, 0) != 0 then
      po_err_num := 1;
      po_err_msg := 'Ошибка при сохранении контактного лица';
      return null;
    end if;

    update T_ORGANIZATIONS O
       set O.ORG_TYPE            = pi_org_type,
           O.ORG_NAME = case
                          when o.org_id = pi_org_id then
                           pi_org_name
                          else
                           O.ORG_NAME
                        end,
           O.ORG_OGRN            = pi_org_ogrn,
           O.ADR1_ID             = l_addr_jur_id,
           O.ADR2_ID             = l_addr_fact_id,
           O.RESP_ID             = l_resp_id,
           O.TOUCH_ID            = l_touch_id,
           O.ORG_SETTL_ACCOUNT   = pi_income_account.Checking_account,
           O.ORG_CON_ACCOUNT     = pi_income_account.Corr_account,
           O.ORG_KPP             = pi_income_account.KPP,
           O.ORG_BIK             = pi_income_account.BIK,
           O.ORG_OKPO            = pi_income_account.OKPO,
           O.ORG_OKONX           = pi_income_account.OKONX,
           O.REGION_ID = case
                           when o.org_id = pi_org_id then
                            pi_org_region_id
                           else
                            O.REGION_ID
                         end,
           O.ORG_COMMENT         = pi_comment,
           O.Email               = pi_org_email,
           O.ORG_INN             = pi_income_account.INN,
           O.V_LICE              = pi_v_lice,
           O.NA_OSNOVANII        = pi_na_osnovanii,
           o.worker_id_change    = pi_worker_id,
           o.change_date         = sysdate,
           o.org_full_name = case
                               when o.org_id = pi_org_id then
                                pi_org_full_name
                               else
                                O.org_full_name
                             end,
           o.v_lice_podkl        = pi_v_lice_podkl,
           o.na_osnovanii_podkl  = pi_na_osnovanii_podkl,
           o.is_pay_espp         = pi_is_pay_espp,
           o.boss_name           = pi_boss_name,
           o.type_org            = pi_type_org,
           is_stamp              = pi_is_stamp,
           is_with_rekv          = pi_is_with_rekv,
           is_with_ip            = pi_is_with_ip,
           IS_WITH_PERSONAL_INFO = PI_IS_WITH_PERSONAL_INFO,
           start_date            = pi_start_date,
           use_child_req         = decode(pi_use_child_req,
                                          null,
                                          use_child_req,
                                          pi_use_child_req),
           org_buy               = pi_org_buy,
           BANK_NAME             = pi_income_account.BANK_NAME,
           RASH_INN              = pi_expense_account.INN,
           RASH_BIK              = pi_expense_account.BIK,
           RASH_SETTL_ACCOUNT    = pi_expense_account.Checking_account,
           RASH_CON_ACCOUNT      = pi_expense_account.Corr_account,
           RASH_BANK_NAME        = pi_expense_account.BANK_NAME,
           RASH_OKPO             = pi_expense_account.OKPO,
           RASH_OKONX            = pi_expense_account.OKONX,
           RASH_KPP              = pi_expense_account.KPP,
           IS_SS_CENTER          = PI_IS_SS_CENTER,
           --UNRESERVED_TMC        = PI_UNRESERVED_TMC,
           erp_r12_num = pi_erp_r12_num
     where O.ORG_ID = pi_org_id
        or (l_is_fedagent > 0 and
           o.org_id in
           (select distinct r2.org_id
               from t_org_relations r2
              where r2.org_reltype = 1006
                and r2.org_id in
                    (select r1.org_id
                       from t_org_relations r1
                      where r1.org_reltype = 1001
                        and r1.org_pid = pi_org_id)))
    returning ORG_ID bulk collect into tab_org_change;
    if l_is_fedagent > 0 then
      update t_organizations t
         set t.org_name      = pi_org_name || '/' ||
                               (select r.kl_name
                                  from t_dic_region r
                                 where r.reg_id = t.region_id),
             t.org_full_name = pi_org_full_name || '/' ||
                               (select r.kl_name
                                  from t_dic_region r
                                 where r.reg_id = t.region_id)
       where t.org_id in (select * from table(tab_org_change))
         and t.org_id <> pi_org_id;
    end if;
    --
    if PI_UNRESERVED_TMC is not null and PI_UNRESERVED_TMC.Count != 0 and PI_UNRESERVED_TMC(1)
      .number_1 is not null then
      delete from t_org_tmc_unreserv t where t.org_id = pi_org_id;
      insert into t_org_tmc_unreserv
        (org_id, tmc_type, cou)
        select pi_org_id, number_1, number_2 from table(PI_UNRESERVED_TMC);
      --записываем в историю
      insert into t_org_tmc_unreserv_hst
        (org_id, tmc_type, cou)
        select t.org_id, t.tmc_type, t.cou
          from t_org_tmc_unreserv t
         where t.org_id = pi_org_id;
    end if;

    -- 83745. Если Организация сменила статус ЦПО - обновим информацию в соответствующих таблицах.
    IF PI_IS_SS_CENTER = 1 THEN
      -- 83745. Обновим информацию в таблицах ЦПО
      Update_ssc_Info(PI_ORG_ID            => pi_org_id,
                      PI_SSC_PHONES        => PI_SSC_PHONES,
                      PI_SSC_TIMETABLE     => PI_SSC_TIMETABLE,
                      PI_SSC_EMAIL         => PI_SSC_EMAIL,
                      PI_SSC_LATITUDE      => PI_SSC_LATITUDE,
                      PI_SSC_LONGITUDE     => PI_SSC_LONGITUDE,
                      PI_SSC_CLOSE_DATE    => PI_SSC_CLOSE_DATE,
                      PI_SSC_SEGM_SERVICE  => PI_SSC_SEGM_SERVICE,
                      PI_SSC_URL           => PI_SSC_URL,
                      pi_ss_service        => pi_ss_service,
                      pi_OWNERSHIP         => pi_OWNERSHIP,
                      pi_square_meter      => pi_square_meter,
                      pi_workers_number    => pi_workers_number,
                      PI_METRO             => PI_METRO,
                      PI_PRIORITY          => PI_PRIORITY,
                      PI_COMMENTS          => PI_COMMENTS,
                      pi_worker_id         => pi_worker_id,
                      pi_ssc_cluster       => pi_ssc_cluster,
                      pi_ssc_fact_district => pi_ssc_fact_district,
                      pi_ssc_temp_close    => pi_ssc_temp_close,
                      pi_ssc_date_open     => pi_ssc_date_open,
                      pi_ssc_contact_phone => pi_ssc_contact_phone,
                      pi_ssc_CNT_CASHBOX   => pi_ssc_CNT_CASHBOX,
                      pi_ssc_employee      => pi_ssc_employee,
                      pi_FULL_NAME_SSC     => pi_FULL_NAME_SSC,
                      pi_ssc_CNT_TERM_RTC     => pi_ssc_CNT_TERM_RTC,
                      pi_ssc_CNT_TERM_AGENT   => pi_ssc_CNT_TERM_AGENT,
                      pi_ssc_IS_ELECTRO_QUEUE => pi_ssc_IS_ELECTRO_QUEUE,
                      pi_ssc_IS_GOLD_POOL     => pi_ssc_IS_GOLD_POOL);
    elsif PI_IS_SS_CENTER <> 1 and PI_SSC_TIMETABLE.Count > 0 and PI_SSC_TIMETABLE(1)
         .day_number is not null then
      insert_TIMETABLE(nvl(res, pi_org_id),
                       PI_SSC_TIMETABLE,
                       pi_worker_id,
                       po_err_num,
                       po_err_msg);
      if l_is_fedagent > 0 then
        for ora_1 in (select column_value org_id
                        from table(tab_org_change) t
                       where t.column_value <> nvl(res, pi_org_id)) loop
          insert_TIMETABLE(ora_1.org_id,
                           PI_SSC_TIMETABLE,
                           pi_worker_id,
                           po_err_num,
                           po_err_msg);
        end loop;
      end if;
    END IF;

    /*    IF l_was_ssc = 1 AND PI_IS_SS_CENTER = 0 THEN
      -- 83745. Организация была ЦПО, а теперь нет - надо удалить информацию
      Delete_Ssc_Info(pi_org_id);
    END IF;*/

    -- Проставляем и у дочерних PI_USE_CHILD_REQ
    if PI_USE_CHILD_REQ is not null then
      update t_organizations o
         set o.use_child_req = PI_USE_CHILD_REQ
       where o.org_id in (select /*+ PRECOMPUTE_SUBQUERY */
                           tor.org_id
                            from t_org_relations tor
                          connect by prior tor.org_id = tor.org_pid
                           start with tor.org_id = pi_org_id);
    end if;
    res := pi_org_id;

    -- Признак "неполный месяц"
    if pi_start_date is not null and
       trunc(pi_start_date, 'month') <> pi_start_date then
      merge into t_part_month t
      using (select res as org_id,
                    trunc(pi_start_date, 'month') as part_month_date,
                    1 as IS_PART_MONTH
               from dual) tmp
      on (tmp.org_id = t.org_id and tmp.part_month_date = t.part_month_date)
      when not matched then
        insert
          (t.org_id, t.part_month_date, t.is_part_month)
        values
          (res, trunc(pi_start_date, 'month'), 1)
      when matched then
        update
           set t.is_part_month = 1
         where t.part_month_date = tmp.part_month_date
           and t.org_id = tmp.org_id;
    end if;

    -- change relation
    dummy := Change_Org_Relation(res, pi_parent_id, c_rel_tp_parent);

    update t_organizations o
       set o.root_org_id =
           (Select max(org_id)
              from (Select tor.org_id, tor.org_pid, tor.org_reltype
                      from t_org_relations tor
                    Connect by prior tor.org_pid = tor.org_id
                           and tor.org_reltype = 1001
                     Start with tor.org_id = res
                            and tor.org_reltype = 1001)
             where org_pid = -1)
     where o.org_id = res;

    if pi_channel_tab is not null then
      delete from t_org_channels t
       where t.org_id = pi_org_id
          or (l_is_fedagent > 0 and
             t.org_id in
             (select distinct r2.org_id
                 from t_org_relations r2
                where r2.org_reltype = 1006
                  and r2.org_id in
                      (select r1.org_id
                         from t_org_relations r1
                        where r1.org_reltype = 1001
                          and r1.org_pid = pi_org_id)));
      insert into t_org_channels
        (org_id, channel_id, default_type)
        select pi_org_id, number_1, number_2
          from table(pi_channel_tab)
        union
        select tab.org_id, number_1, number_2
          from table(pi_channel_tab)
          join (select distinct r2.org_id
                  from t_org_relations r2
                 where r2.org_reltype = 1006
                   and r2.org_id in
                       (select r1.org_id
                          from t_org_relations r1
                         where r1.org_reltype = 1001
                           and r1.org_pid = pi_org_id)) tab
            on l_is_fedagent > 0;

      insert into t_org_channels_hst
        select seq_ORG_CHANNELS_HST.nextval, pi_worker_id, sysdate, ch.*
          from t_org_channels ch
         where ch.org_id = pi_org_id;

      l_count := is_org_usi(pi_org_id);
      if l_count = 0 then
        change_org_channel_dog(pi_org_id,
                               pi_worker_id,
                               po_err_num,
                               po_err_msg);
      end if;
    end if;
    return pi_org_id;
  exception
    when ex_is_pay_espp then
      po_err_num := SQLCODE;
      po_err_msg := 'is_pay_espp = -1';
      rollback to sp_Change_Org;
      return null;
    when dup_val_on_index then
      po_err_num := SQLCODE;
      po_err_msg := 'Нарушение уникальности идеинтификатора организации в таблице связей.';
      rollback to sp_Change_Org;
      return null;
    when parent_is_ssc then
      po_err_num := 1002;
      po_err_msg := 'Для Организаций-ЦПО регистрация нижестоящих Организаций запрещена.';
      rollback to sp_Change_Org;
      return null;
    when ssc_have_children then
      po_err_num := 1002;
      po_err_msg := 'Организация имеет нижестоящие Организации и не может быть ЦПО.';
      rollback to sp_Change_Org;
      return null;
    WHEN ex_duplicate_phones then
      po_err_num := 1003;
      po_err_msg := 'В одной Организации-ЦПО зарегистрировано 2 одинаковых номера телефона.';
      rollback to sp_Change_Org;
      return null;
    when ex_invalid_argument then
      po_err_num := 1001;
      po_err_msg := 'Неверное значение идеинтификатора организации (org_id=' ||
                    pi_org_id || ').';
      rollback to sp_Change_Org;
      return null;
    when ex_pi_priority then
      po_err_num := 1004;
      po_err_msg := 'Поле "Приоритет" должно быть заполнено.';
      rollback to sp_Change_Org;
      return null;
    when ex_max_priority then
      po_err_num := 1005;
      po_err_msg := 'Превышено максимальное значение приоритета.';
      rollback to sp_Change_Org;
      return null;
    when ex_big_prior then
      L_MAX_PRIORITY := get_max_priority(pi_org_id, l_max_org_id);
      po_err_num     := 1006;
      po_err_msg     := 'Значение приоритета не должно превышать ' ||
                        L_MAX_PRIORITY;
      rollback to sp_Change_Org;
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      rollback to sp_Change_Org;
      return null;
  end;
  ------------------------------------------------------------------------------------
  function Add_Org_Rel_outRecreate(pi_org_id   in T_ORGANIZATIONS.ORG_ID%type,
                                   pi_dog_id   in number,
                                   pi_org_pid  in T_ORGANIZATIONS.ORG_ID%type,
                                   pi_rel_type in T_ORG_RELATIONS.ORG_RELTYPE%type)
    return T_ORG_RELATIONS.ID%type is
    tmp_rel_id     T_ORG_RELATIONS.ID%type := -1;
    l_ROOT_ORG_ID2 number;
  begin
    if (pi_org_id = -1) then
      raise ex_invalid_argument;
    end if;

    insert into T_ORG_RELATIONS
      (ORG_ID, ORG_PID, ORG_RELTYPE, dog_id)
    values
      (pi_org_id, pi_org_pid, pi_rel_type, pi_dog_id)
    returning ID into tmp_rel_id;
    -- 120265 Определить данную организацию до пересчета дерева нельзя
    /*l_ROOT_ORG_ID2 := GET_PARENT_ORG_ID(pi_org_id);
    -- Посчитаем головную организацию для отчётов.
    UPDATE T_ORGANIZATIONS o
       SET o.ROOT_ORG_ID2 = l_ROOT_ORG_ID2
     WHERE o.org_id = pi_org_id;*/
    return tmp_rel_id;
  end;
  ------------------------------------------------------------------------------------
  function Add_Org_Relation(pi_org_id   in T_ORGANIZATIONS.ORG_ID%type,
                            pi_org_pid  in T_ORGANIZATIONS.ORG_ID%type,
                            pi_rel_type in T_ORG_RELATIONS.ORG_RELTYPE%type)
    return T_ORG_RELATIONS.ID%type is
    tmp_rel_id     T_ORG_RELATIONS.ID%type := -1;
    l_ROOT_ORG_ID2 number;
    l_check        number;
  begin
    if (pi_org_id = -1) then
      raise ex_invalid_argument;
    end if;

    insert into T_ORG_RELATIONS
      (ORG_ID, ORG_PID, ORG_RELTYPE)
    values
      (pi_org_id, pi_org_pid, pi_rel_type)
    returning ID into tmp_rel_id;
    if pi_rel_type = 1001 then
      select count(tor.org_id)
        into l_check
        from t_org_relations tor
       where tor.org_reltype = 999
      connect by prior tor.org_pid = tor.org_id
       start with tor.org_id = pi_org_pid;
      if l_check > 0 then
        update t_organizations o
           set o.is_ss_center = 1
         where o.org_id = pi_org_id;
      end if;
    end if;

    /*select tt.id
     into org_rel_lock
     from T_ORG_RELATIONS tt
    where tt.id = tmp_rel_id
      For Update nowait;*/
    select count(*)
      into l_check
      from (select distinct tor.org_id, tor.org_pid
              from t_org_relations tor
            connect by prior tor.org_pid = tor.org_id
             start with tor.org_id in (pi_org_id));
    if l_check < 10 then
      for ora_1 in (select tor.org_id, tor.org_pid
                      from t_org_relations tor
                    connect by prior tor.org_pid = tor.org_id
                     start with tor.org_id in (pi_org_id)) loop
        -- 19.10.2010
        ReCreateTree_for_org(ora_1.org_id, ora_1.org_pid);
      end loop;
    else
      recreatetree;
    end if;
    l_ROOT_ORG_ID2 := GET_PARENT_ORG_ID(pi_org_id);
    -- Посчитаем головную организацию для отчётов.
    UPDATE T_ORGANIZATIONS o
       SET o.ROOT_ORG_ID2 = l_ROOT_ORG_ID2
     WHERE o.org_id = pi_org_id;

    return tmp_rel_id;
    /*exception
    when ex_locked then
      return null;*/
  end Add_Org_Relation;
  -----------------------------------------------------------------------------------
  function Change_Org_Relation(pi_org_id   in T_ORGANIZATIONS.ORG_ID%type,
                               pi_org_pid  in T_ORGANIZATIONS.ORG_ID%type,
                               pi_rel_type in T_ORG_RELATIONS.ORG_RELTYPE%type)
    return T_ORG_RELATIONS.ID%type is
    tmp_rel_id T_ORG_RELATIONS.ID%type := -1;
  begin
    if (pi_org_id = -1) then
      raise ex_invalid_argument;
    end if;

    begin
      select O.ID
        into tmp_rel_id
        from T_ORG_RELATIONS O
       where O.ORG_ID = pi_org_id
         and O.ORG_PID = pi_org_pid
         and O.ORG_RELTYPE = pi_rel_type;
    exception
      when others then
        DBMS_OUTPUT.Put(sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace);
    end;

    if (tmp_rel_id = -1) then
      insert into T_ORG_RELATIONS
        (ORG_ID, ORG_PID, ORG_RELTYPE)
      values
        (pi_org_id, pi_org_pid, pi_rel_type)
      returning ID into tmp_rel_id;
    end if;

    ReCreateTree_for_org(pi_org_id, pi_org_pid);

    -- Посчитаем головную организацию для отчётов для организации и всего, что лежит ниже её.
    MERGE INTO T_ORGANIZATIONS o
    USING (select org_id, min(root_org_id) ROOT_ORG_ID
             from (select distinct org.org_id,
                                   COALESCE(org1.org_id,
                                            orgX.org_id,
                                            orgR.org_id,
                                            org.org_id) ROOT_ORG_ID
                     from t_organizations org

                   -- Регион (филиалы)
                     join mv_org_tree treeX
                       on treeX.ORG_ID = org.org_id
                     left join t_dic_region regX
                       on regX.reg_id = org.region_id
                     left join t_organizations orgX
                       on (orgX.Org_Id = treeX.root_org_id and
                          treeX.root_org_id > 2)
                       or (org.region_id > 0 and orgX.org_id = regX.org_id and
                          treeX.root_org_id <= 2)

                     left join mv_org_tree rel
                       on rel.org_id = org.org_id
                      and rel.org_pid <> -1
                         --       and rel.root_org_pid <> -1
                      and rel.org_reltype not in (1005, 1006, 1009)
                     left join t_dic_region reg1
                       on reg1.org_id in (rel.org_id, rel.root_org_id)
                     left join t_dic_mrf mrf
                       on mrf.org_id in (rel.org_id, rel.org_pid)
                     left join t_org_ignore_right ir
                       on ir.org_id = org.org_id
                      and ir.type_org = 1

                   -- МРФ
                     left join t_organizations orgR
                       on orgR.Org_Id in (mrf.org_id, reg1.org_id)
                       or (orgR.Org_Id = rel.root_org_id and
                          mrf.org_id is null and reg1.org_id is null)
                       or (ir.org_id is not null and orgr.org_id = 2001269)
                     left join mv_org_tree tree
                       on tree.org_id = org.org_id
                      and tree.root_reltype in (1004, 999)

                   -- Агент
                     left join t_organizations org1
                       on tree.root_org_id = org1.org_id

                    WHERE org.ORG_ID IN
                          (SELECT ORG_ID
                             FROM T_ORG_RELATIONS
                           CONNECT BY PRIOR ORG_ID = ORG_PID
                                  AND ORG_RELTYPE != 1009
                            START WITH ORG_ID = pi_org_id))
            group by org_id) roots
    ON (roots.org_id = o.org_id)
    WHEN MATCHED THEN
      UPDATE SET o.ROOT_ORG_ID2 = roots.ROOT_ORG_ID;

    return tmp_rel_id;
  end Change_Org_Relation;
  ------------------------------------------------------------------------
  function Get_Curator_Orgs(pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                            pi_is_up     in pls_integer,
                            pi_with_lic  in number, --получать помимо всего лиц. счет ли нет
                            pi_worker_id in T_USERS.USR_ID%type,
                            po_err_num   out pls_integer,
                            po_err_msg   out t_Err_Msg) return sys_refcursor is
    res sys_refcursor;
  begin
    logging_pkg.debug('pi_org_id=' || pi_org_id || ' pi_is_up=' ||
                      pi_is_up || ' pi_with_lic=' || pi_with_lic ||
                      ' pi_worker_id=' || pi_worker_id,
                      'Get_Curator_Orgs');
    if nvl(pi_with_lic, 0) = 1 then
      if (pi_is_up = 1) then
        open res for
          select org_id,
                 org_pid,
                 org_name,
                 dog_id,
                 is_enabled,
                 dog_number,
                 dog_class_id,
                 is_org_rtm,
                 m2m_type,
                 max(acc_id) acc_id
            from (select distinct o.org_id,
                                  r.org_pid,
                                  (case
                                    when m.name_mrf is not null then
                                     m.name_mrf || '/'
                                    else
                                     ''
                                  end) || (case
                                    when reg.kl_name is not null then
                                     reg.kl_name || '/'
                                    else
                                     ''
                                  end) || o.org_name org_name,
                                  d.dog_id,
                                  d.is_enabled,
                                  d.dog_number,
                                  d.dog_class_id,
                                  /*max*/
                                  (ta.acc_id) acc_id,
                                  rtmob.is_org_rtm,
                                  d.m2m_type
                    from t_organizations o
                    join (Select tor.id, tor.org_pid, tor.org_id
                           from mv_org_tree tor
                          Where tor.org_reltype in (1004, 1007, 1008, 999)
                         Connect by prior tor.org_pid = Tor.Org_Id
                         --and tor.root_reltype in (1004, 1007, 1008, 999)
                          Start With tor.org_id = pi_org_id
                                 and tor.org_reltype <> 1009) r
                      on o.org_id = r.org_id
                    join t_dogovor d
                      on r.id = d.org_rel_id
                    left join t_acc_owner tao
                      on tao.owner_ctx_id = d.dog_id
                    left join t_accounts ta
                      on ta.acc_owner_id = tao.owner_id
                     and ta.acc_type = ACC_OPERATIONS.c_acc_type_lic
                    left join t_org_is_rtmob rtmob
                      on rtmob.org_id = o.org_id
                    left join t_dic_region reg
                      on reg.reg_id = o.region_id
                      or reg.org_id in (r.org_id, r.org_pid)
                    left join t_dic_mrf m
                      on m.id = reg.mrf_id
                      or m.org_id in (r.org_id, r.org_pid)) tab
           group by org_id,
                    org_pid,
                    org_name,
                    dog_id,
                    is_enabled,
                    dog_number,
                    dog_class_id,
                    is_org_rtm,
                    m2m_type
           order by org_name asc, org_id asc, org_pid asc;
      else
        open res for
          select org_id,
                 org_pid,
                 org_name,
                 dog_id,
                 is_enabled,
                 dog_number,
                 dog_class_id,
                 is_org_rtm,
                 m2m_type,
                 max(acc_id) acc_id
            from (select o.org_id,
                         r.org_pid,
                         (case
                           when m.name_mrf is not null then
                            m.name_mrf || '/'
                           else
                            ''
                         end) || (case
                           when reg.kl_name is not null then
                            reg.kl_name || '/'
                           else
                            ''
                         end) || o.org_name org_name,
                         (case
                           when o.org_id in
                                (select *
                                   from TABLE(get_user_orgs_tab(pi_worker_id,
                                                                0))) then
                            d.dog_id
                           else
                            null
                         end) dog_id,
                         d.is_enabled,
                         d.dog_number,
                         d.dog_class_id,
                         /*max*/
                         (ta.acc_id) acc_id,
                         rtmob.is_org_rtm,
                         d.m2m_type
                    from t_organizations o
                    join t_org_relations r
                      on o.org_id = r.org_id
                    join t_dogovor d
                      on r.id = d.org_rel_id
                    left join t_acc_owner tao
                      on tao.owner_ctx_id = d.dog_id
                    left join t_accounts ta
                      on ta.acc_owner_id = tao.owner_id
                     and ta.acc_type = ACC_OPERATIONS.c_acc_type_lic
                    left join t_org_is_rtmob rtmob
                      on rtmob.org_id = o.org_id
                    left join t_dic_region reg
                      on reg.reg_id = o.region_id
                      or reg.org_id in (r.org_id, r.org_pid)
                    left join t_dic_mrf m
                      on m.id = reg.mrf_id
                      or m.org_id in (r.org_id, r.org_pid)
                   where r.org_pid = pi_org_id
                     and (r.org_reltype <> 1001)
                     and o.org_id in
                         (select *
                            from table(orgs.Get_User_Orgs_Tab_With_Param1(pi_worker_id)))) tab
           group by org_id,
                    org_pid,
                    org_name,
                    org_id,
                    dog_id,
                    is_enabled,
                    dog_number,
                    dog_class_id,
                    is_org_rtm,
                    m2m_type
           order by org_name asc, org_id asc, org_pid asc;
      end if;
    else
      if (pi_is_up = 1) then
        open res for
          select *
            from (select o.org_id,
                         r.org_pid,
                         (case
                           when m.name_mrf is not null then
                            m.name_mrf || '/'
                           else
                            ''
                         end) || (case
                           when reg.kl_name is not null then
                            reg.kl_name || '/'
                           else
                            ''
                         end) || o.org_name org_name,
                         d.dog_id,
                         d.is_enabled,
                         d.dog_number,
                         d.dog_class_id,
                         null acc_id,
                         rtmob.is_org_rtm,
                         d.m2m_type
                    from t_organizations o
                    join (Select tor.id, tor.org_pid, tor.org_id
                           from mv_org_tree tor
                          Where tor.org_reltype in (1004, 1007, 1008, 999)
                         Connect by prior tor.org_pid = Tor.Org_Id
                                and tor.root_reltype in
                                    (1004, 1007, 1008, 999)
                          Start With tor.org_id = pi_org_id
                                 and tor.org_reltype <> 1009) r
                      on o.org_id = r.org_pid
                    join t_dogovor d
                      on r.id = d.org_rel_id
                    left join t_org_is_rtmob rtmob
                      on rtmob.org_id = o.org_id
                    left join t_dic_region reg
                      on reg.reg_id = o.region_id
                      or reg.org_id in (r.org_id, r.org_pid)
                    left join t_dic_mrf m
                      on m.id = reg.mrf_id
                      or m.org_id in (r.org_id, r.org_pid))
           order by org_name asc, org_id asc, org_pid asc;
      else
        open res for
          select *
            from (select o.org_id,
                         r.org_pid,
                         (case
                           when m.name_mrf is not null then
                            m.name_mrf || '/'
                           else
                            ''
                         end) || (case
                           when reg.kl_name is not null then
                            reg.kl_name || '/'
                           else
                            ''
                         end) || o.org_name org_name,
                         (case
                           when o.org_id in
                                (select *
                                   from TABLE(get_user_orgs_tab(pi_worker_id,
                                                                0))) then
                            d.dog_id
                           else
                            null
                         end) dog_id,
                         d.is_enabled,
                         d.dog_number,
                         d.dog_class_id,
                         null acc_id,
                         rtmob.is_org_rtm,
                         d.m2m_type
                    from t_organizations o
                    join t_org_relations r
                      on o.org_id = r.org_id
                    join t_dogovor d
                      on r.id = d.org_rel_id
                    left join t_org_is_rtmob rtmob
                      on rtmob.org_id = o.org_id
                    left join t_dic_region reg
                      on reg.reg_id = o.region_id
                      or reg.org_id in (r.org_id, r.org_pid)
                    left join t_dic_mrf m
                      on m.id = reg.mrf_id
                      or m.org_id in (r.org_id, r.org_pid)
                   where r.org_pid = pi_org_id
                     and (r.org_reltype <> 1001)
                     and o.org_id in
                         (select *
                            from table(orgs.Get_User_Orgs_Tab_With_Param(pi_worker_id)))) tab
           order by org_name asc, org_id asc, org_pid asc;
      end if;
    end if;
    return res;
  exception
    when ex_invalid_argument then
      po_err_num := 1001;
      po_err_msg := 'Неверное значение идеинтификатора организации (org_id=' ||
                    pi_org_id || ').';
      return null;
  end Get_Curator_Orgs;
  ------------------------------------------------------------------------
  function Get_Channels_Org(pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                            pi_worker_id in T_USERS.USR_ID%type,
                            po_err_num   out pls_integer,
                            po_err_msg   out varchar2) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select t.channel_id,
             d.name channel_name,
             nvl(t.default_type, 0) default_type
        from t_org_channels t
        join t_dic_channels d
          on d.channel_id = t.channel_id
       where t.org_id = pi_org_id;
    return res;
  exception
    when others then
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      po_err_num := sqlcode;
      return null;
  end;
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
    return sys_refcursor is
    res   sys_refcursor;
    parId pls_integer := -1;
    l_cnt number;
  begin
    if (pi_org_id is not null) then

      if pi_is_check = 1 and
         (not Security_pkg.Check_Rights_str('EISSD.ORGS.LIST',
                                            pi_org_id,
                                            pi_worker_id,
                                            po_err_num,
                                            po_err_msg,
                                            null,
                                            null)) then
        --для заблокированных договоров добавим доп проверку
        select count(*)
          into l_cnt
          from t_org_relations T
         where t.org_id = pi_org_id
         START WITH T.ORG_ID in
                    (select uo.org_id
                       from t_user_org uo
                       join t_roles_perm rp
                         on rp.rp_role_id = uo.role_id
                       join t_perm_rights pr
                         on pr.pr_prm_id = rp.rp_perm_id
                       join t_rights rr
                         on rr.right_id = pr.pr_right_id
                      where uo.usr_id = Pi_Worker_id
                        and rr.right_string_id = 'EISSD.ORGS.LIST')
        cONNECT BY PRIOR t.ORG_ID = t.ORG_PID;

        if l_cnt = 0 then
          return null;
        end if;
        po_err_num := 0;
        po_err_msg := '';
      end if;

      begin
        select r.ORG_PID
          into parId
          from T_ORG_RELATIONS R
         where r.ORG_RELTYPE = c_rel_tp_parent
           and r.ORG_ID = pi_org_id;
      exception
        when others then
          parId := -1;
      end;

      open res for
        select distinct O.ORG_ID,
                        O.ORG_NAME ORG_NAME,
                        O.ORG_OGRN,
                        ORG_TYPE,
                        O.REGION_ID,
                        O.ORG_SETTL_ACCOUNT,
                        O.ORG_CON_ACCOUNT,
                        O.ORG_KPP,
                        O.ORG_BIK,
                        O.ORG_OKPO,
                        O.ORG_OKONX,
                        A1.ADDR_ID adr1_id,
                        A1.ADDR_COUNTRY adr1_country,
                        A1.ADDR_INDEX adr1_index,
                        A1.ADDR_CITY adr1_city,
                        A1.ADDR_CODE_CITY adr1_code_city,
                        A1.ADDR_STREET adr1_street,
                        A1.ADDR_CODE_STREET adr1_code_street,
                        A1.ADDR_BUILDING adr1_building,
                        A1.ADDR_OFFICE adr1_office,
                        a1.region_id adr1_region_id,
                        NVL(ao1_p.id, ao1_c.id) adr1_obj_city_id,
                        ao1.ID adr1_obj_street_id,
                        a1.ADDR_HOUSE_OBJ_ID adr1_obj_house_id,
                        dr1.kl_region adr1_kl_region,
                        A2.ADDR_ID adr2_id,
                        A2.ADDR_COUNTRY adr2_country,
                        A2.ADDR_INDEX adr2_index,
                        A2.ADDR_CITY adr2_city,
                        A2.ADDR_CODE_CITY adr2_code_city,
                        A2.ADDR_STREET adr2_street,
                        A2.ADDR_CODE_STREET adr2_code_street,
                        A2.ADDR_BUILDING adr2_building,
                        A2.ADDR_OFFICE adr2_office,
                        a2.region_id adr2_region_id,
                        NVL(ao2_p.id, ao2_c.id) adr2_obj_city_id,
                        ao2.ID adr2_obj_street_id,
                        a2.ADDR_HOUSE_OBJ_ID adr2_obj_house_id,
                        dr2.kl_region adr2_kl_region,
                        RESP.PERSON_ID resp_id,
                        RESP.PERSON_PHONE resp_phone,
                        RESP.PERSON_EMAIL resp_email,
                        RESP.PERSON_LASTNAME resp_lastname,
                        RESP.PERSON_FIRSTNAME resp_firstname,
                        RESP.PERSON_MIDDLENAME resp_middlename,
                        RESP.PERSON_INN resp_inn,
                        RESP.PERSON_BIRTHDAY resp_birthday,
                        RESP.PERSON_SEX resp_sex,
                        D1.DOC_ID resp_doc_id,
                        D1.DOC_SERIES resp_doc_series,
                        D1.DOC_NUMBER resp_doc_number,
                        D1.DOC_REGDATE resp_doc_date,
                        D1.DOC_EXTRAINFO resp_doc_info,
                        D1.DOC_TYPE resp_doc_type,
                        TOUCH.PERSON_ID touch_id,
                        TOUCH.PERSON_PHONE touch_phone,
                        TOUCH.PERSON_EMAIL touch_email,
                        TOUCH.PERSON_LASTNAME touch_lastname,
                        TOUCH.PERSON_FIRSTNAME touch_firstname,
                        TOUCH.PERSON_MIDDLENAME touch_middlename,
                        TOUCH.PERSON_INN touch_inn,
                        TOUCH.PERSON_BIRTHDAY touch_birthday,
                        TOUCH.PERSON_SEX touch_sex,
                        D2.DOC_ID touch_doc_id,
                        D2.DOC_SERIES touch_doc_series,
                        D2.DOC_NUMBER touch_doc_number,
                        D2.DOC_REGDATE touch_doc_date,
                        D2.DOC_EXTRAINFO touch_doc_info,
                        D2.DOC_TYPE touch_doc_type,
                        parId ORG_PID,
                        O.ORG_COMMENT,
                        O.Email,
                        O.ORG_INN INN,
                        O.V_LICE,
                        O.NA_OSNOVANII,
                        O.ROOT_ORG_ID,
                        NVL(o.org_full_name, O.ORG_NAME) org_full_name,
                        o.v_lice_podkl,
                        o.na_osnovanii_podkl,
                        o.is_pay_espp,
                        o.boss_name,
                        tree.org_reltype,
                        tree.root_reltype,
                        o.type_org,
                        resp.birthplace,
                        o.is_stamp,
                        o.is_with_rekv,
                        nvl(reg.mrf_id, mrf.id) mrf_id,
                        o.is_with_ip,
                        o.IS_WITH_PERSONAL_INFO,
                        o.start_date,
                        oc.org_id child_id,
                        o.USE_CHILD_REQ,
                        o.org_buy,
                        o.BANK_NAME,
                        is_org_rtmob(o.org_id) is_org_rtmob,
                        o.RASH_INN,
                        o.RASH_BIK,
                        o.RASH_SETTL_ACCOUNT,
                        o.RASH_CON_ACCOUNT,
                        o.RASH_BANK_NAME,
                        o.RASH_OKPO,
                        o.RASH_OKONX,
                        o.RASH_KPP,
                        o.IS_SS_CENTER,
                        ssc.LATITUDE as SSC_LATITUDE,
                        ssc.LONGITUDE as SSC_LONGITUDE,
                        ssc.CLOSE_DATE as SSC_CLOSE_DATE,
                        ssc.SEGMENT_SERVICE as SSC_SEGMENT_SERVICE,
                        ssc.EMAIL as SSC_EMAIL,
                        ssc.URL as SSC_URL,
                        CASE
                          WHEN EXISTS
                           (SELECT 1
                                  FROM T_ORG_RELATIONS rel
                                 WHERE rel.ORG_PID = pi_org_id
                                   AND rel.ORG_RELTYPE = c_rel_tp_parent) THEN
                           1
                          ELSE
                           0
                        END as have_children,
                        ssc.ownership,
                        ssc.square_meter,
                        ssc.workers_number,
                        ssc.metro,
                        ssc.priority,
                        ssc.comments,
                        --o.unreserved_tmc,
                        o.erp_r12_num,
                        ssc.open_date,
                        ssc.cnt_cashbox,
                        ssc.addr_fact_district,
                        ssc.cluster_id,
                        ssc.full_name_ssc,
                        ssc.is_electro_queue,
                        ssc.is_gold_pool,
                        ssc.cnt_term_rtc,
                        ssc.cnt_term_agent
          from T_ORGANIZATIONS O
          left join T_PERSON RESP
            on RESP.PERSON_ID = O.RESP_ID
          left join T_PERSON TOUCH
            on TOUCH.PERSON_ID = O.TOUCH_ID
          left join T_ADDRESS A1
            on A1.ADDR_ID = O.ADR1_ID
        -- вытаскиваем город
          left join t_address_object ao1
            on a1.ADDR_OBJ_ID = ao1.ID
           and ao1.IS_STREET = 1
           and nvl(ao1.is_deleted, 0) = 0
          left join t_address_object ao1_p
            on ao1_p.ID = ao1.PARENT_ID
           and nvl(ao1_p.is_deleted, 0) = 0
          left join t_address_object ao1_c
            on a1.ADDR_OBJ_ID = ao1_c.ID
           and nvl(ao1_c.is_deleted, 0) = 0
        --
          left join t_dic_region dr1
            on dr1.reg_id = a1.region_id
          left join T_ADDRESS A2
            on A2.ADDR_ID = O.ADR2_ID
        -- вытаскиваем город
          left join t_address_object ao2
            on a2.ADDR_OBJ_ID = ao2.ID
           and ao2.IS_STREET = 1
           and nvl(ao2.is_deleted, 0) = 0
          left join t_address_object ao2_p
            on ao2_p.ID = ao2.PARENT_ID
           and nvl(ao2_p.is_deleted, 0) = 0
          left join t_address_object ao2_c
            on a2.ADDR_OBJ_ID = ao2_c.ID
           and nvl(ao2_c.is_deleted, 0) = 0
        --
          left join t_dic_region dr2
            on dr2.reg_id = a2.region_id
          left join T_DOCUMENTS D1
            on D1.DOC_ID = RESP.DOC_ID
          left join T_DOCUMENTS D2
            on D2.DOC_ID = TOUCH.DOC_ID
          left join mv_org_tree tree
            on tree.org_id = o.org_id
          left join t_dic_region reg
            on reg.reg_id = o.region_id
          left join t_dic_mrf mrf
            on mrf.org_id in (o.org_id, tree.org_pid)
          left join t_organizations_child oc
            on oc.org_pid = o.org_id
          left join T_ORG_SS_CENTER ssc
            ON ssc.ORG_ID = o.ORG_ID
           AND o.IS_SS_CENTER = 1
         where O.ORG_ID = pi_org_id;
    else
      open res for
        select 1 from dual where 1 <> 1;
    end if;

    open po_channels for
      select t.org_id, t.channel_id, t.default_type
        from t_org_channels t
       where t.org_id = pi_org_id;

    open po_ssc_phones for
      select p.org_id, p.PHONE_NUMBER, p.segment_service
        from T_ORG_SSC_PHONE p
        JOIN T_ORG_SS_CENTER ssc
          ON ssc.ORG_ID = p.ORG_ID
       where p.org_id = pi_org_id
         AND ssc.IS_ENABLED = 1;

    open po_ssc_timetable for
      select t.org_id,
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
        from T_ORG_SSC_TIMETABLE t
        left JOIN T_ORG_SS_CENTER ssc
          ON ssc.ORG_ID = t.ORG_ID
       where t.org_id = pi_org_id
         AND (ssc.IS_ENABLED = 1 or ssc.org_id is null);

    open po_ss_service for
      select t.org_id,
             t.ss_service ss_service_id,
             d.ss_service,
             t.ss_service_pos,
             t.segment_service
        from t_org_ss_service t
        JOIN T_ORG_SS_CENTER ssc
          ON ssc.ORG_ID = t.ORG_ID
        join t_dic_org_ss_service d
          on d.id = t.ss_service
         and d.is_actual = 1
       where t.org_id = pi_org_id
         AND ssc.IS_ENABLED = 1
       order by t.ss_service_pos;

    --Сотрудники организации
    open po_ssc_employee for
      select e.type_employee, e.name, e.phone
        from t_org_ssc_employee e
       where e.org_id = pi_org_id;

    --Периоды временного закрытия
    open po_ssc_temp_close for
      select t.date_start -
             regions.get_region_offset_time(o.region_id) / 24 date_start,
             t.date_end - regions.get_region_offset_time(o.region_id) / 24 date_end,
             t.reason_close
        from t_org_ssc_temp_close t
        join t_organizations o
          on o.org_id = t.org_id
       where t.org_id = pi_org_id;

    --Контактный телефон ЦПО
    open po_ssc_contact_phone for
      select c.phone
        from t_org_ssc_contact_phone c
       where c.org_id = pi_org_id;

    --небронируемый остаток
    open po_tmc_unreserved for
      select t.tmc_type, t.cou from t_org_tmc_unreserv t where t.org_id = pi_org_id;

    return res;
  exception
    when others then
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      po_err_num := sqlcode;
      return null;
  end Get_Org_By_Id;

  ------------------------------------------------------------------------
  procedure Fix_Org_Tree is
  begin
    ReCreateTree;
    return;
  end Fix_Org_Tree;
  ---------------------------------------------------------------------------
  function Get_Reg_by_Org(pi_org_id    in number, --1 -выводить для РТ Мобайл
                          pi_worker_id in T_USERS.USR_ID%type) return num_tab is
  begin
    return Get_Reg_by_Org(pi_org_id, null, pi_worker_id);
  end;
  ---------------------------------------------------------------------------
  -- Получение инфы по регионам для использования в SQL.
  -- Отличие от Get_Region_by_Org - возвращает num_tab (а не sys_refcursor) + нет out-параметров
  ---------------------------------------------------------------------------
  function Get_Reg_by_Org(pi_org_id    in number, --1 -выводить для РТ Мобайл
                          pi_org_pid   in number,
                          pi_worker_id in T_USERS.USR_ID%type) return num_tab is
    user_reg_tab num_tab;

  begin
    if pi_org_pid is null then
      with tabs as
       (select *
          from (select tor.org_id, min(level) OVER() lvlo, level lvl
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
                 start with tor.org_id in (pi_org_id)) tt
         where tt.lvlo = tt.lvl)
      select distinct t.reg_id bulk collect
        into user_reg_tab
        from t_dic_region t
        join (select tor.org_id
                from t_org_relations tor
              --join tabs on tabs.lvlo = tabs.lvl
              connect by prior tor.org_id = tor.org_pid
               start with tor.org_id in (select org_id from tabs)) tab
          on tab.org_id = t.org_id;

    else
      with tab as
       (select tor.org_id, min(level) mmin
          from t_org_relations tor
          join t_organizations o
            on o.org_id = tor.org_id
          left join t_dic_mrf m
            on m.org_id = tor.org_id
          left join t_dic_region r
            on r.org_id = tor.org_id
            or r.mrf_id = m.id
          left join dual d
            on tor.org_id in (0, 1, 2)
         where nvl(r.reg_id, nvl(m.id, decode(d.dummy, null, null, 1))) is not null
        connect by prior tor.org_pid = tor.org_id
         start with tor.org_id = pi_org_id
         group by tor.org_id),
      tab2 as
       (select tor.org_id, min(level) mmin
          from t_org_relations tor
          join t_organizations o
            on o.org_id = tor.org_id
          left join t_dic_mrf m
            on m.org_id = tor.org_id
          left join t_dic_region r
            on r.org_id = tor.org_id
            or r.mrf_id = m.id
          left join dual d
            on tor.org_id in (0, 1, 2)
         where nvl(r.reg_id, nvl(m.id, decode(d.dummy, null, null, 1))) is not null
        connect by prior tor.org_pid = tor.org_id
         start with tor.org_id = pi_org_pid
         group by tor.org_id)
      select distinct region_id bulk collect
        into user_reg_tab
        from (select distinct r.reg_id region_id
                from (select min(mmin) mmin from tab2) t2
                join tab2
                  on tab2.mmin = t2.mmin
                left join t_dic_mrf m
                  on m.org_id = tab2.org_id
                join t_dic_region r
                  on r.org_id = tab2.org_id
                  or r.mrf_id = m.id
                join (select distinct r.reg_id region_id
                       from (select min(mmin) mmin from tab) t1
                       join tab
                         on tab.mmin = t1.mmin
                       left join t_dic_mrf m
                         on m.org_id = tab.org_id
                       join t_dic_region r
                         on r.org_id = tab.org_id
                         or r.mrf_id = m.id) tt
                  on tt.region_id = r.reg_id
                 and r.org_id is not null);
    end if;
    return user_reg_tab;
  end;
  ---------------------------------------------------------------------------
  -- Получение инфы по регионам
  ---------------------------------------------------------------------------
  function Get_Regions_by_Org(pi_org_id    in number, --1 -выводить для РТ Мобайл
                              pi_worker_id in T_USERS.USR_ID%type,
                              po_err_num   out pls_integer,
                              po_err_msg   out varchar2) return sys_refcursor is
  begin
    return Get_Regions_by_Org(pi_org_id,
                              null,
                              pi_worker_id,
                              po_err_num,
                              po_err_msg);
  end;
  ---------------------------------------------------------------------------
  -- Получение инфы по регионам
  ---------------------------------------------------------------------------
  function Get_Regions_by_Org(pi_org_id    in number, --1 -выводить для РТ Мобайл
                              pi_org_pid   in number,
                              pi_worker_id in T_USERS.USR_ID%type,
                              po_err_num   out pls_integer,
                              po_err_msg   out varchar2) return sys_refcursor is
    res          sys_refcursor;
    user_reg_tab num_tab;
  begin
    user_reg_tab := Get_Reg_by_Org(pi_org_id, pi_org_pid, pi_worker_id);
    open res for
      select t.reg_id,
             t.kl_name         REG_NAME,
             t.kl_region       REG_CODE,
             null              EXT_ID,
             t.org_id,
             org.org_name,
             org.org_full_name,
             i.rtmob_org_id
        from t_dic_region t
        left join t_organizations org
          on org.org_id = t.org_id
        join t_dic_region_info i
          on i.reg_id = t.reg_id
        join t_dic_region_data rd
          on rd.reg_id = t.reg_id
         and rd.visible = 1
       where t.reg_id in (select column_value from table(user_reg_tab))
         and t.reg_id < 94
       order by t.kl_name;
    return res;
  end;
  ---------------------------------------------------------------------------
  --получение инфы по регионам
  ---------------------------------------------------------------------------
  function Get_Regions(pi_only_ural    in number,
                       pi_only_filial  in number, --если не 1 то достаются все филиалы внутри мрф
                       pi_is_org_rtmob in number, --1 -выводить для РТ Мобайл
                       pi_worker_id    in T_USERS.USR_ID%type,
                       po_err_num      out pls_integer,
                       po_err_msg      out t_Err_Msg) return sys_refcursor is
    res          sys_refcursor;
    l_cnt        number;
    l_cnt_rt     number;
    l_cnt_rtm    number;
    user_reg_tab num_tab;
    l_check      number;
  begin
    if Pi_worker_id is not null then
      -- Даем доступ ЕЦОВ`у
      Select Count(*)
        into l_cnt
        from t_user_org t
       where t.usr_id = Pi_worker_id
         and t.org_id in
             (select org_id from t_org_ignore_right where TYPE_ORG = 1);

      Select Count(*)
        into l_cnt_rt
        from t_user_org t
       where t.usr_id = Pi_worker_id
         and t.org_id in (0, 1, 3); --РТ,КЦ + РРС
      if l_cnt_rt > 0 then
        select t.reg_id bulk collect
          into user_reg_tab
          from t_dic_region t
          left join t_dic_region_info tt
            on tt.reg_id = t.reg_id
         where nvl(pi_is_org_rtmob, 0) <> 1
            or tt.rtmob_org_id is not null;
      elsif l_cnt > 0 then
        select t.reg_id bulk collect
          into user_reg_tab
          from t_dic_region t
         where t.reg_id between 1 and 7;
      else
        Select Count(*)
          into l_cnt_rtm
          from t_user_org t
         where t.usr_id = Pi_worker_id
           and t.org_id in (2); --РТМ
        if pi_only_filial = 1 then
          select count(*)
            into l_check
            from t_org_relations tor
            left join t_dic_region r
              on r.org_id = tor.org_id
            left join t_dic_region_info i
              on i.rtmob_org_id = tor.org_id
           where r.reg_id is not null
              or i.reg_id is not null
          connect by prior tor.org_pid = tor.org_id
           start with tor.org_id in
                      ((Select tu.org_id
                         from t_user_org tu
                        where tu.usr_id = Pi_worker_id));
          if l_check > 0 then
            select distinct t.reg_id bulk collect
              into user_reg_tab
              from (select tor.org_id, tor.org_pid, level lvl
                      from t_org_relations tor
                    connect by prior tor.org_pid = tor.org_id
                     start with tor.org_id in
                                (Select tu.org_id
                                   from t_user_org tu
                                  where tu.usr_id = Pi_worker_id)) tab
              left join t_dic_mrf m
                on m.org_id = tab.org_id
               and tab.lvl = 1
              left join t_dic_region_info i
                on i.rtmob_org_id = tab.org_id
              join t_dic_region t
                on tab.org_id = t.org_id
                or t.mrf_id = m.id
                or i.reg_id = t.reg_id;
          else
            select distinct t.reg_id bulk collect
              into user_reg_tab
              from (select tor.org_id, tor.org_pid, level lvl
                      from t_org_relations tor
                    connect by prior tor.org_pid = tor.org_id
                     start with tor.org_id in
                                (Select tu.org_id
                                   from t_user_org tu
                                  where tu.usr_id = Pi_worker_id)) tab
              left join t_dic_mrf m
                on m.org_id = tab.org_id
              left join t_dic_region_info i
                on i.rtmob_org_id = tab.org_id
              join t_dic_region t
                on tab.org_id = t.org_id
                or t.mrf_id = m.id
                or i.reg_id = t.reg_id;
          end if;
        else
          select distinct t.reg_id bulk collect
            into user_reg_tab
            from t_dic_region t
            join t_dic_region_info i
              on i.reg_id = t.reg_id
            join t_dic_mrf m
              on m.id = t.mrf_id
            join (select tor.org_id
                    from t_org_relations tor
                  connect by prior tor.org_pid = tor.org_id
                   start with tor.org_id in
                              (Select t.org_id
                                 from t_user_org t
                                where t.usr_id = Pi_worker_id
                                  and (nvl(pi_is_org_rtmob, 0) <> 1 or
                                      is_org_rtmob(t.org_id) = 1))) tab
              on tab.org_id = t.org_id
              or tab.org_id = m.org_id
              or tab.org_id = i.rtmob_org_id
              or (nvl(l_cnt_rtm, 0) > 0 and i.rtmob_org_id is not null);
        end if;
      end If;
    end if;
    open res for
      select t.reg_id,
             t.kl_name         REG_NAME,
             t.kl_region       REG_CODE,
             null              EXT_ID,
             t.org_id,
             org.org_name,
             org.org_full_name,
             i.rtmob_org_id,
             t.mrf_id,
             i.get_pay_m2m,
             t.source_type,
             t.terminal_level,
             t.gmt
        from t_dic_region t
        join t_organizations org
          on org.org_id = t.org_id
        join t_dic_region_info i
          on i.reg_id = t.reg_id
       where (nvl(pi_only_ural, 0) = 0 or (t.reg_id between 1 and 7))
         and (pi_worker_id is null or
             t.reg_id in (select column_value from table(user_reg_tab)))
       order by t.kl_name;
    return res;
  end Get_Regions;
  ---------------------------------------------------------------------------
  --50703(46892)Получение расширенной инфы по регионам
  ---------------------------------------------------------------------------
  function Get_Regions_Info(pi_region_id in t_dic_region.reg_id%type,
                            pi_worker_id in T_USERS.USR_ID%type,
                            -- 51465
                            pi_doc_flag in number,
                            po_err_num  out pls_integer,
                            po_err_msg  out t_Err_Msg) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select t.reg_id,
             t.mask_phone,
             t.mask_nshd,
             t.mask_iptv,
             t.id_reg_crm,
             t.id_centr_crm,
             t.id_utks_crm,
             reg.name          reg_name,
             t.centr,
             dr.kl_region      kod_centr,
             org.org_id,
             org.org_name,
             org.org_full_name,
             -- 51465
             decode(pi_doc_flag, 1, rd.document, null) document,
             t.id_utks_tar_uno,
             t.actual_location as actual_location,
             decode(pi_doc_flag, 1, rd.rtm_document, null) child_document,
             t.is_reg_tele2,
             dr.SOURCE_TYPE,
             t.get_pay_m2m,
             dr.mrf_id,
             dr.terminal_level,
             mvno.search_by_request
        from t_dic_region_info t
        join t_dic_region_data reg
          on reg.reg_id = t.reg_id
        join t_dic_region dr
          on dr.reg_id = t.reg_id
        left join t_organizations org
          on org.org_id = dr.org_id
        left join t_region_documents rd
          on rd.reg_id = reg.reg_id
        left join T_DIC_MVNO_SETT_BY_REG mvno
          on mvno.reg_id = t.reg_id
       where (pi_region_id is null or t.reg_id = pi_region_id)
       order by reg_name;
    return res;
  end Get_Regions_Info;
  ------------------------------------------------------------------------
  -- Изменение договора
  ------------------------------------------------------------------------

  ------------------------------------------------------------------------
  -- добавление орг-ций дублирующих главную на регионы
  ------------------------------------------------------------------------
  procedure Add_subOrgs(pi_org_id     in number,
                        pi_dog_id     in number,
                        pi_region_tab in string_tab,
                        pi_worker_id  in number,
                        po_err_num    out pls_integer,
                        po_err_msg    out varchar2) is
    l_id     number;
    l_rel_id NUMBER;
    --l_region_tab string_tab;
    l_check        number := 0;
    l_hst_id       number;
    l_mrf_id       number;
    L_ROOT_ORG_ID2 number;
  begin
    savepoint save_point;
    select max(r.mrf_id)
      into l_mrf_id
      from t_dic_region r
     where r.kl_region in (select * from table(pi_region_tab));
    for ora_2 in (select o.org_id
                    from t_organizations o
                    join t_dic_region r
                      on r.reg_id = o.region_id
                   where o.org_id in
                         (select tor.org_id
                            from mv_org_tree tor
                           where tor.dog_id = pi_dog_id
                          connect by prior tor.org_id = tor.org_pid
                           start with tor.org_id = pi_org_id)
                     and o.is_enabled = 1
                     and r.mrf_id = l_mrf_id
                     and nvl(o.region_id, -1) <> -1
                     and o.region_id not in
                         (select reg.reg_id
                            from table(pi_region_tab) tab
                            join t_dic_region reg
                              on reg.kl_region = tab.column_value)) loop
      update t_organizations r
         set r.is_enabled = 0
       where r.org_id = ora_2.org_id;
    end loop;
    for ora_1 in (select t.reg_id,
                         t.KL_NAME,
                         t.KL_SOCR,
                         ORG_TYPE,
                         ORG_NAME,
                         ADR1_ID,
                         ADR2_ID,
                         RESP_ID,
                         TOUCH_ID,
                         ORG_SETTL_ACCOUNT,
                         ORG_CON_ACCOUNT,
                         ORG_KPP,
                         ORG_BIK,
                         ORG_OKPO,
                         ORG_OKONX,
                         ORG_OGRN,
                         ORG_COMMENT,
                         EMAIL,
                         PREFIX,
                         ROOT_ORG_ID,
                         V_LICE,
                         NA_OSNOVANII,
                         ORG_INN,
                         NEW_ORG_ID,
                         ROOT_ORG_ID2,
                         WORKER_ID_CREATE,
                         WORKER_ID_CHANGE,
                         ORG_FULL_NAME,
                         V_LICE_PODKL,
                         NA_OSNOVANII_PODKL,
                         BOSS_NAME,
                         TYPE_ORG,
                         IS_WITH_REKV,
                         START_DATE,
                         USE_CHILD_REQ,
                         ORG_BUY,
                         BANK_NAME,
                         RASH_INN,
                         RASH_BIK,
                         RASH_SETTL_ACCOUNT,
                         RASH_CON_ACCOUNT,
                         RASH_BANK_NAME,
                         RASH_OKPO,
                         RASH_OKONX,
                         RASH_KPP,
                         IS_SS_CENTER,
                         T.ORG_ID REG_ORG_ID,
                         tt.is_pay_espp
                    from t_dic_region t
                    JOIN T_ORGANIZATIONS TT
                      ON TT.ORG_ID = pi_org_id
                   where t.kl_region in (select * from table(pi_region_tab))
                     and t.org_id is not null
                     and t.reg_id not in
                         (select o.region_id
                            from mv_org_tree tor
                            join t_organizations o
                              on o.org_id = tor.org_id
                           where o.is_enabled = 1
                             and tor.dog_id = pi_dog_id
                          connect by prior tor.org_id = tor.org_pid
                           start with tor.org_id = pi_org_id)) loop
      insert into t_organizations
        (ORG_TYPE,
         ORG_NAME,
         ADR1_ID,
         ADR2_ID,
         RESP_ID,
         TOUCH_ID,
         ORG_SETTL_ACCOUNT,
         ORG_CON_ACCOUNT,
         ORG_KPP,
         ORG_BIK,
         ORG_OKPO,
         ORG_OKONX,
         REGION_ID,
         ORG_OGRN,
         ORG_COMMENT,
         EMAIL,
         PREFIX,
         ROOT_ORG_ID,
         V_LICE,
         NA_OSNOVANII,
         ORG_INN,
         NEW_ORG_ID,
         ROOT_ORG_ID2,
         WORKER_ID_CREATE,
         WORKER_ID_CHANGE,
         ORG_FULL_NAME,
         V_LICE_PODKL,
         NA_OSNOVANII_PODKL,
         BOSS_NAME,
         TYPE_ORG,
         IS_WITH_REKV,
         START_DATE,
         USE_CHILD_REQ,
         ORG_BUY,
         BANK_NAME,
         RASH_INN,
         RASH_BIK,
         RASH_SETTL_ACCOUNT,
         RASH_CON_ACCOUNT,
         RASH_BANK_NAME,
         RASH_OKPO,
         RASH_OKONX,
         RASH_KPP,
         IS_SS_CENTER,
         is_pay_espp)
      VALUES
        (ora_1.ORG_TYPE,
         ora_1.ORG_NAME || '/' || ORA_1.KL_NAME,
         ora_1.ADR1_ID,
         ora_1.ADR2_ID,
         ora_1.RESP_ID,
         ora_1.TOUCH_ID,
         ora_1.ORG_SETTL_ACCOUNT,
         ora_1.ORG_CON_ACCOUNT,
         ora_1.ORG_KPP,
         ora_1.ORG_BIK,
         ora_1.ORG_OKPO,
         ora_1.ORG_OKONX,
         ORA_1.reg_id,
         ora_1.ORG_OGRN,
         ora_1.ORG_COMMENT,
         ora_1.EMAIL,
         ora_1.PREFIX,
         ora_1.ROOT_ORG_ID,
         ora_1.V_LICE,
         ora_1.NA_OSNOVANII,
         ora_1.ORG_INN,
         ora_1.NEW_ORG_ID,
         ora_1.ROOT_ORG_ID2,
         pi_worker_id,
         pi_worker_id,
         ora_1.ORG_FULL_NAME || '/' || ORA_1.KL_NAME,
         ora_1.V_LICE_PODKL,
         ora_1.NA_OSNOVANII_PODKL,
         ora_1.BOSS_NAME,
         ora_1.TYPE_ORG,
         ora_1.IS_WITH_REKV,
         ora_1.START_DATE,
         ora_1.USE_CHILD_REQ,
         ora_1.ORG_BUY,
         ora_1.BANK_NAME,
         ora_1.RASH_INN,
         ora_1.RASH_BIK,
         ora_1.RASH_SETTL_ACCOUNT,
         ora_1.RASH_CON_ACCOUNT,
         ora_1.RASH_BANK_NAME,
         ora_1.RASH_OKPO,
         ora_1.RASH_OKONX,
         ora_1.RASH_KPP,
         ora_1.IS_SS_CENTER,
         ora_1.is_pay_espp)
      returning org_id into l_id;
    
      insert into t_org_tmc_unreserv
        (org_id, tmc_type, cou)
        select l_id, t.tmc_type, t.cou
          from t_org_tmc_unreserv t
         where t.org_id = pi_org_id;
      --записываем в историю
      insert into t_org_tmc_unreserv_hst
        (org_id, tmc_type, cou)
        select t.org_id, t.tmc_type, t.cou
          from t_org_tmc_unreserv t
         where t.org_id = l_id;
    
      l_rel_id := Add_Org_Rel_outRecreate(pi_org_id   => l_id,
                                          pi_dog_id   => pi_dog_id,
                                          pi_org_pid  => pi_org_id,
                                          pi_rel_type => '1001');
      l_rel_id := Add_Org_Rel_outRecreate(pi_org_id   => l_id,
                                          pi_dog_id   => pi_dog_id,
                                          pi_org_pid  => ORA_1.REG_ORG_ID,
                                          pi_rel_type => '1006');
    
      insert into t_org_channels
        (org_id, channel_id, default_type)
        select l_id, ch. channel_id, ch.default_type
          from t_org_channels ch
         where ch.org_id = pi_org_id;
    
      insert into t_org_channels_hst
        select seq_ORG_CHANNELS_HST.nextval, pi_worker_id, sysdate, ch.*
          from t_org_channels ch
         where ch.org_id = l_id;
    
      -- Добавляем региональный расчетный центр
      insert into T_ORG_CALC_CENTER
        (ORG_ID, CC_ID)
        (select distinct l_id, CC.CC_ID
           from t_CALC_CENTER CC
           join t_dic_mvno_region r
             on r. id=cc.cc_region_id
          where --cc.cc_region_id = ORA_1.reg_id
          r.reg_id = ORA_1.reg_id
      and cc.cc_is_ps = 1);
    
      l_hst_id := save_org_state(pi_worker_id => pi_worker_id,
                                 pi_org_id    => l_id);
      --проверка на наличие расписания у глав-орг
      select count(*)
        into l_check
        from t_org_ssc_timetable t
       where t.org_id = pi_org_id
         and t.is_enabled = 1;
      if l_check > 0 then
        insert into t_org_ssc_timetable
          (org_id,
           day_number,
           work_start,
           work_end,
           break_start,
           break_end,
           is_enabled)
          select l_id,
                 day_number,
                 work_start,
                 work_end,
                 break_start,
                 break_end,
                 1
            from t_org_ssc_timetable t
           where t.org_id = pi_org_id
             and t.is_enabled = 1;
      end if;
    end loop;
    update t_org_relations t
       set t.dog_id = pi_dog_id
     where t.id =
           (select d.org_rel_id from t_dogovor d where d.dog_id = pi_dog_id);
    recreatetree;
    -- 120265 Определить данную организацию до пересчета дерева нельзя, поэтому делаем это здесь
    L_ROOT_ORG_ID2 := GET_PARENT_ORG_ID(pi_org_id);
    UPDATE T_ORGANIZATIONS o
       SET o.ROOT_ORG_ID2 = L_ROOT_ORG_ID2
     WHERE o.org_id = pi_org_id;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      rollback to save_point;
  end;
  ------------------------------------------------------------------------
  -- Изменение договора
  ------------------------------------------------------------------------
  function Add_Dogovor(pi_dogovor_number in T_DOGOVOR.DOG_NUMBER%type,
                       pi_dogovor_date   in T_DOGOVOR.DOG_DATE%type,
                       pi_org_id1        in T_ORGANIZATIONS.ORG_ID%type,
                       pi_org_id2        in T_ORGANIZATIONS.ORG_ID%type,
                       --pi_cc_id            in NUM_TAB,
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
                       pi_org_name_sprav      in t_dogovor.org_name_sprav%type,
                       pi_dog_number_sprav    in t_dogovor.dog_number_sprav%type,
                       pi_org_legal_form      in t_dogovor.org_legal_form%type,
                       pi_type_report_form    in num_date_tab,
                       pi_percent_stb         in t_dogovor.percent_stb%type,
                       pi_PRIZNAK_STB         in t_dogovor.PRIZNAK_STB%type,
                       pi_payment_type        in t_dogovor.payment_type%type,
                       pi_m2m_type            in t_dogovor.m2m_type%type,
                       pi_region_tab          in string_tab,
                       pi_temporary_indent    in t_dogovor.temporary_indent%type,
                       pi_max_sim_count       in t_dogovor.max_sim_count%type,
                       pi_required_emp_num       t_dogovor.is_employee_number_required%TYPE,
                       po_err_num             out pls_integer,
                       po_err_msg             out t_Err_Msg)
    return T_DOGOVOR.DOG_ID%type is
    res            T_DOGOVOR.DOG_ID%type := -1;
    dog_date       T_DOGOVOR.DOG_DATE%type := sysdate;
    l_count_prm    number := 0; -- количество разрешений договора
    l_rel_type     number;
    l_rel_id       number;
    l_is_cycle     number;
    l_org_rel_type number;

    l_chang_class number;
    --l_count_dog   number;
    l_check_dog  number;
    l_region_tab string_tab;
    l_is_org_usi number;
  begin
    logging_pkg.info('pi_dogovor_number ' || pi_dogovor_number || '
               pi_dogovor_date   ' ||
                     To_Char(pi_dogovor_date, 'dd.mm.yyyy') || '
               pi_org_id1        ' || pi_org_id1 || '
               pi_org_id2        ' || pi_org_id2 || '
               pi_is_auto_vis    ' || pi_is_auto_vis || '
               pi_worker_id      ' || pi_worker_id || '
               pi_with_nds       ' || pi_with_nds || '
               pi_pr_schema      ' || pi_pr_schema || '
               pi_v_lice         ' || pi_v_lice || '
               pi_na_osnovanii   ' || pi_na_osnovanii || '
               pi_list_prm       ' ||
                     Get_STR_BY_NUM_TAB(pi_list_prm) || '
               pi_acc_schema     ' || pi_acc_schema || '
               pi_overdraft      ' || pi_overdraft || '
               pi_org_list       ' ||
                     Get_STR_BY_NUM_TAB(pi_org_list) || '
               pi_codes          ' || pi_codes || '
               pi_dogovor_class_id' || pi_dogovor_class_id || '
               pi_max_sim_count' || pi_max_sim_count || '
               pi_required_emp_num=' || pi_required_emp_num);
    savepoint save_point;
    if pi_dogovor_class_id = 8 then
      l_org_rel_type := 1007;
    elsif pi_dogovor_class_id = 9 then
      l_org_rel_type := 1008;
    elsif pi_dogovor_class_id = 12 then
      l_org_rel_type := 999;
    else
      l_org_rel_type := 1004;
    end if;
    if Is_Dog_Permitted(pi_org_id1,
                        pi_org_id2,
                        pi_worker_id,
                        pi_list_prm,
                        po_err_num,
                        po_err_msg) = 1 then
      -- ищем связь 1004 между 2 организациями
      select max(tor.org_reltype)
        into l_rel_type
        from t_org_relations tor
       where tor.org_id = pi_org_id2
         and tor.org_pid = pi_org_id1
         and tor.org_reltype = l_org_rel_type;
      select count(*)
        into l_check_dog
        from t_org_relations r
        join t_dogovor d
          on r.id = d.org_rel_id
       where r.org_id = pi_org_id2
         and r.org_pid = pi_org_id1
         and nvl(d.is_enabled, 0) = 1
         and d.dog_class_id = pi_dogovor_class_id;
      if l_check_dog != 0 and pi_dogovor_class_id not in (1, 8, 9) then
        po_err_num := 1;
        po_err_msg := 'Регистрация договора с данной организацией невозможна, т. к. с организацией уже зарегистрирован договор с указанным классом договора.';
        rollback to save_point;
        return null;
      end if;

      select count(*)
        into l_chang_class
        from t_org_relations o
        join t_dogovor d
          on o.id = d.org_rel_id
       where o.org_id = pi_org_id2
         and ((o.org_reltype != 1008 and pi_dogovor_class_id = 9) or
             (o.org_reltype = 1008 and pi_dogovor_class_id != 9))
         and d.is_enabled = 1;

      if l_chang_class != 0 and pi_dogovor_class_id = 9 then
        po_err_num := 1;
        po_err_msg := 'Регистрация договора «Телемаркетинг» с данной организацией невозможна, т. к. с организацией зарегистрирован договор Агента.';
        rollback to save_point;
        return null;
      elsif l_chang_class != 0 and pi_dogovor_class_id != 9 then
        po_err_num := 1;
        po_err_msg := 'Регистрация договора Агента с данной организацией невозможна, т. к. с организацией зарегистрирован договор «Телемаркетинг».';
        rollback to save_point;
        return null;
      end if;
      --проверка что мы не создаем договора под договором
      select count(tor.org_id)
        into l_check_dog
        from t_org_relations tor
       where tor.org_reltype in (1004, 999)
      connect by prior tor.org_pid = tor.org_id
       start with tor.org_id in (pi_org_id1);
      if l_check_dog > 0 then
        l_is_org_usi := is_org_usi(pi_org_id => pi_org_id1);
        if l_is_org_usi = 0 then
          po_err_num := 1;
          po_err_msg := 'Регистрация договора под зарегистрированным договором невозможна!';
          rollback to save_point;
          return null;
        end if;
      end if;

      --проверка на наличие связи другого типа с этой организацией
      select count(r.id)
        into l_chang_class
        from t_org_relations r
       where r.org_id = pi_org_id2
         and r.org_pid = pi_org_id1
         and r.org_reltype != l_org_rel_type
         and r.org_reltype in (1002, 1004, 1006, 1007, 1008, 999);

      if l_chang_class != 0 then
        po_err_num := 1;
        po_err_msg := 'Невозможно выполнить операцию. Организация уже имеет связь с данным подразделением';
        rollback to save_point;
        return null;
      end if;
      --согласно постановке по РРС:исполнение договора должно выполняться на уровне МРФ
      if pi_dogovor_class_id = 12 then
        select count(*)
          into l_check_dog
          from t_dic_mrf t
         where t.org_id = pi_org_id1;
        if l_check_dog = 0 then
          po_err_num := 1;
          po_err_msg := 'Невозможно выполнить операцию. Исполнение договора «Аутсорсинг ЦПО» должно выполняться на уровне МРФ';
          rollback to save_point;
          return null;
        end if;
      end if;
      if l_rel_type is null then
        --если такой связи нет, создаём её
        l_rel_id := ORGS.Add_Org_Relation(pi_org_id2,
                                          pi_org_id1,
                                          l_org_rel_type /*1004*/);

        if pi_dogovor_class_id in (9) then
          --добавим доп связи на филиалы  ( Задача № 68638 )
          if pi_org_id1 = 0 then
            insert into T_ORG_RELATIONS
              (ORG_ID, ORG_PID, ORG_RELTYPE)
              select distinct pi_org_id2, r.org_id, 1009
                from t_dic_region r
               where r.org_id is not null
                 and not exists (select null
                        from t_org_relations o
                       where o.org_id = pi_org_id2
                         and o.org_pid = r.org_id
                         and o.org_reltype = 1009);
          else
            insert into T_ORG_RELATIONS
              (ORG_ID, ORG_PID, ORG_RELTYPE)
              select distinct pi_org_id2, r.org_id, 1009
                from t_dic_region r
                join t_dic_mrf m
                  on m.id = r.mrf_id
               where m.org_id = pi_org_id1
                 and r.org_id is not null
                 and not exists (select null
                        from t_org_relations o
                       where o.org_id = pi_org_id2
                         and o.org_pid = r.org_id
                         and o.org_reltype = 1009);

          end if;
        end if;

        -- 07.10.09
        Select Count(*)
          into l_is_cycle
          from (select distinct rel.org_id, CONNECT_BY_ISCYCLE cbi
                  from t_org_relations rel
                connect by nocycle prior rel.org_id = rel.org_pid
                 start with rel.org_id = 0)
         Where cbi > 0;

        if l_is_cycle > 0 then
          raise ex_wrong_rel;
        end if;

      else
        -- если новую связь не создали (она уже была), то находим её id
        select max(tor.id)
          into l_rel_id
          from t_org_relations tor
         where tor.org_pid = pi_org_id1
           and tor.org_id = pi_org_id2
           and tor.org_reltype = l_org_rel_type /*1004*/
        ;
      end if;

      if (pi_dogovor_date is not null) then
        dog_date := pi_dogovor_date;
      end if;

      insert into T_DOGOVOR
        (ORG_REL_ID,
         DOG_NUMBER,
         DOG_DATE,
         IS_VIS_AUTO,
         PREMIA_SCHEMA,
         WITH_NDS,
         V_LICE,
         NA_OSNOVANII,
         CODES,
         dog_class_id,
         is_accept,
         -- 33537 olia_serg
         org_name_sprav,
         dog_number_sprav,
         org_legal_form,
         percent_stb,
         PRIZNAK_STB,
         payment_type,
         m2m_type,
         temporary_indent,
         max_sim_count,
         is_employee_number_required)
      values
        (l_rel_id,
         pi_dogovor_number,
         dog_date,
         pi_is_auto_vis,
         pi_pr_schema,
         pi_with_nds,
         pi_v_lice,
         pi_na_osnovanii,
         pi_codes,
         pi_dogovor_class_id,
         pi_is_accept,
         -- 33537 olia_serg
         pi_org_name_sprav,
         pi_dog_number_sprav,
         pi_org_legal_form,
         pi_percent_stb,
         pi_PRIZNAK_STB,
         pi_payment_type,
         pi_m2m_type,
         pi_temporary_indent,
         pi_max_sim_count,
         pi_required_emp_num)
      returning DOG_ID into res;

      if l_rel_type is null then
        if pi_dogovor_class_id in (12) then
          select distinct r.kl_region bulk collect
            into l_region_tab
            from t_dic_region r
            join t_dic_mrf m
              on m.id = r.mrf_id
           where m.org_id = pi_org_id1
             and r.org_id is not null
             and r.reg_id <= 90;
          Add_subOrgs(pi_org_id2,
                      res,
                      l_region_tab,
                      pi_worker_id,
                      po_err_num,
                      po_err_msg);
        end if;
      end if;
      if pi_region_tab is not null and pi_region_tab.count <> 0 and
         pi_region_tab.first is not null then
        Add_subOrgs(pi_org_id2,
                    res,
                    pi_region_tab,
                    pi_worker_id,
                    po_err_num,
                    po_err_msg);
      end if;

      insert into t_dogovor_report_form
        (dog_id, dog_type, date_dog_type)
        select res, num1, dates from table(pi_type_report_form);

      if ((pi_style_file1 is not null or pi_style_file2 is not null) and
         res is not null) then
        insert into t_dogovor_details
          (dog_id, url, url2)
        values
          (res, pi_style_file1, pi_style_file2);
      end if;
      Acc_Operations.Create_Accounts(res, l_rel_id); -- ИЗМЕНЕНИЯ: передаём номер договора
      /*23.06.09 (задача №8519)*/
      -- Patrick: для договора мерчанта попытка создать схему вызывает ошибку
      if pi_dogovor_class_id <> ORGS.c_dog_class_merchant then
        acc_operations.Add_Acc_Schema(res,
                                      pi_acc_schema,
                                      pi_overdraft,
                                      po_err_num,
                                      po_err_msg);
      end if;

      select count(*) into l_count_prm from table(pi_list_prm);
      -- если для договора задан список возможностей, заносим его в таблицу t_dogovor_prm
      if l_count_prm > 0 then
        insert into t_dogovor_prm tdp
          (dp_dog_id, dp_prm_id, dp_is_enabled)
          (select res, plp.column_value, 1 from table(pi_list_prm) plp);
      end if;

      -- 19/10/2009 Добавляем доверенности (Задача № 11710)
      if pi_org_list is not null then
        Add_Warrant(res, pi_org_id2, pi_org_list, po_err_num, po_err_msg);
      end if;

    else
      -- нельзя создать договор
      rollback to save_point;
      return null;
    end if;
    -- Пересчитаем таблицу T_ORG_RELTYPE_DOWN
    recreateorgreltypedown_for_org(pi_org_id2, null);
    return res;
  exception
    when dup_val_on_index then
      po_err_num := SQLCODE;
      po_err_msg := 'Нарушение уникальности идентификатора организации в таблице связей.';
      rollback to save_point;
      return null;
    when ex_invalid_argument then
      po_err_num := 1001;
      po_err_msg := 'Неверное значение идентификатора дочерней организации (org_id=' ||
                    pi_org_id2 || ').';
      rollback to save_point;
      return null;
    when ex_wrong_order then
      po_err_num := SQLCODE;
      po_err_msg := 'Идентификаторы организаций заданы в неверном порядке.';
      rollback to save_point;
      return null;
    when ex_wrong_rel then
      po_err_num := SQLCODE;
      po_err_msg := 'Нельзя создать договор между данными организациями.';
      rollback to save_point;
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      rollback to save_point;
      return null;
  end Add_Dogovor;
  ------------------------------------------------------------------------
  function Get_Org_Level(pi_org_id in pls_integer) return pls_integer is
    res pls_integer := -1;
  begin
    select ORG_TYPE
      into res
      from T_ORGANIZATIONS o
     where o.org_id = pi_org_id;
    return res;
  end Get_Org_Level;
  ------------------------------------------------------------------------
  function Get_Org_By_OGRN(pi_org_ogrn  in T_ORGANIZATIONS.ORG_OGRN%type,
                           pi_worker_id in T_USERS.USR_ID%type,
                           po_err_num   out pls_integer,
                           po_err_msg   out t_Err_Msg) return sys_refcursor is
    res   sys_refcursor;
    parId pls_integer := -1;
  begin
    begin
      select R.ORG_PID
        into parId
        from T_ORG_RELATIONS R, T_ORGANIZATIONS O
       where R.ORG_RELTYPE = c_rel_tp_parent
         and R.ORG_ID = O.ORG_ID
         and O.ORG_OGRN = pi_org_ogrn;
    exception
      when others then
        parId := -1;
    end;

    open res for
      select O.ORG_ID,
             O.ORG_NAME              ORG_NAME,
             O.ORG_OGRN,
             ORG_TYPE,
             O.REGION_ID,
             ORG_SETTL_ACCOUNT,
             ORG_CON_ACCOUNT,
             ORG_KPP,
             ORG_BIK,
             ORG_OKPO,
             ORG_OKONX,
             A1.ADDR_ID              adr1_id,
             A1.ADDR_COUNTRY         adr1_country,
             A1.ADDR_INDEX           adr1_index,
             A1.ADDR_CITY            adr1_city,
             A1.ADDR_STREET          adr1_street,
             A1.ADDR_BUILDING        adr1_building,
             A1.ADDR_OFFICE          adr1_office,
             A2.ADDR_ID              adr2_id,
             A2.ADDR_COUNTRY         adr2_country,
             A2.ADDR_INDEX           adr2_index,
             A2.ADDR_CITY            adr2_city,
             A2.ADDR_STREET          adr2_street,
             A2.ADDR_BUILDING        adr2_building,
             A2.ADDR_OFFICE          adr2_office,
             RESP.PERSON_ID          resp_id,
             RESP.PERSON_PHONE       resp_phone,
             RESP.PERSON_EMAIL       resp_email,
             RESP.PERSON_LASTNAME    resp_lastname,
             RESP.PERSON_FIRSTNAME   resp_firstname,
             RESP.PERSON_MIDDLENAME  resp_middlename,
             RESP.PERSON_INN         resp_inn,
             RESP.PERSON_BIRTHDAY    resp_birthday,
             RESP.PERSON_SEX         resp_sex,
             D1.DOC_ID               resp_doc_id,
             D1.DOC_SERIES           resp_doc_series,
             D1.DOC_NUMBER           resp_doc_number,
             D1.DOC_REGDATE          resp_doc_date,
             D1.DOC_EXTRAINFO        resp_doc_info,
             D1.DOC_TYPE             resp_doc_type,
             TOUCH.PERSON_ID         touch_id,
             TOUCH.PERSON_PHONE      touch_phone,
             TOUCH.PERSON_EMAIL      touch_email,
             TOUCH.PERSON_LASTNAME   touch_lastname,
             TOUCH.PERSON_FIRSTNAME  touch_firstname,
             TOUCH.PERSON_MIDDLENAME touch_middlename,
             TOUCH.PERSON_INN        touch_inn,
             TOUCH.PERSON_BIRTHDAY   touch_birthday,
             TOUCH.PERSON_SEX        touch_sex,
             D2.DOC_ID               touch_doc_id,
             D2.DOC_SERIES           touch_doc_series,
             D2.DOC_NUMBER           touch_doc_number,
             D2.DOC_REGDATE          touch_doc_date,
             D2.DOC_EXTRAINFO        touch_doc_info,
             D2.DOC_TYPE             touch_doc_type,
             parId                   ORG_PID,
             O.ORG_COMMENT,
             O.Email,
             O.ORG_INN               INN,
             O.V_LICE,
             O.NA_OSNOVANII,
             O.ROOT_ORG_ID,
             -- задача № 11601
             o.v_lice_podkl,
             o.na_osnovanii_podkl,
             o.boss_name
        from T_ORGANIZATIONS O,
             T_PERSON        RESP,
             T_PERSON        TOUCH,
             T_ADDRESS       A1,
             T_ADDRESS       A2,
             T_DOCUMENTS     D1,
             T_DOCUMENTS     D2
       where O.ORG_OGRN = pi_org_ogrn
         and A1.ADDR_ID(+) = O.ADR1_ID
         and A2.ADDR_ID(+) = O.ADR2_ID
         and RESP.PERSON_ID(+) = O.RESP_ID
         and TOUCH.PERSON_ID(+) = O.TOUCH_ID
         and D1.DOC_ID(+) = RESP.DOC_ID
         and D2.DOC_ID(+) = TOUCH.DOC_ID;
    return res;
  end Get_Org_By_OGRN;
  ------------------------------------------------------------------------
  -- Функция возвращающая список организаций вне зависимости от принадлежности к конкретному пользователю
  function Get_Orgs_By_Type(pi_org_id  in T_ORGANIZATIONS.ORG_ID%type, -- начиная от конкретной организации
                            pi_org2_id in T_ORGANIZATIONS.ORG_ID%type) /* не включая ветку начиная с                                                                                                                                                                                               конкретной организации организации */
   return num_tab is
    res num_tab := null;
  begin
    Select distinct tor.org_id bulk collect
      into res
      from Mv_Org_Tree tor
    Connect By Prior tor.org_id = tor.org_pid
           and tor.org_id <> NVL(pi_org2_id, 3.1415)
     Start with tor.org_id = NVL(pi_org_id, 0);
    return res;
  Exception
    when Others then
      return Null;
  end Get_Orgs_By_Type;
  ------------------------------------------------------------------------
  function Get_Orgs(pi_worker_id         in T_USERS.USR_ID%type, -- пользователь
                    pi_org_id            in T_ORGANIZATIONS.ORG_ID%type := null, -- от конкретной организации
                    pi_parents_include   in pls_integer := 0, -- включать ли родителей найденных организаций
                    pi_self_include      in pls_integer := 1, -- включать pi_org_id
                    pi_childrens_include in pls_integer := 1, -- включать детей
                    pi_curated_include   in pls_integer := 1, -- включать курируемых
                    pi_curators_include  in pls_integer := 0, -- включать кураторов
                    pi_incl_only_act_dog in pls_integer := 1) -- включать только активные договоры
   return num_tab is
    -- Функция возвращающая массив организаций видимых пользователю
    res        num_tab := null;
    l_table_00 num_tab := num_tab();
    l_table_0  num_tab := num_tab();
    l_table_1  num_tab := num_tab();
    l_table_01 num_tab := num_tab();
    l_table_02 num_tab := num_tab();
  begin
    -- Организации, видимые пользователю
    l_table_0 := Get_User_Orgs_Tab(pi_worker_id, pi_incl_only_act_dog);

    -- Организации с параметрами, начиная с pi_org_id или для pi_org_id is null
    if (pi_org_id is null) then
      l_table_1 := Get_User_Orgs_Tab_With_Param(pi_worker_id,
                                                null,
                                                pi_self_include,
                                                pi_childrens_include,
                                                pi_curated_include);
    else
      l_table_1 := Get_User_Orgs_Tab_With_Param(null,
                                                pi_org_id,
                                                pi_self_include,
                                                pi_childrens_include,
                                                pi_curated_include);
    end if;
    -- Пересечение множеств организации, которые видны пользователю и связаны с указанной организацией
    select distinct ORG_ID bulk collect
      into l_table_01
      from (select /*+ PRECOMPUTE_SUBQUERY */
             column_value ORG_ID
              from TABLE(l_table_0)
            intersect
            select /*+ PRECOMPUTE_SUBQUERY */
             column_value ORG_ID
              from TABLE(l_table_1));

    -- Организации, которые связаны с l_table_01 отношениями типа подчиненности (родители для l_table_01)
    if (pi_parents_include = 1) then
      select distinct ORH.ORG_PID bulk collect
        into l_table_00
        from mv_org_tree ORH
       where (ORH.ORG_ID in (select /*+ PRECOMPUTE_SUBQUERY */
                              *
                               from TABLE(l_table_01)))
         and ORH.ORG_RELTYPE = 1001
         and (not ORH.ORG_PID = -1);
    end if;
    -- Организации, которые связаны с l_table_01 и l_table_00 отношениями типа подчиненности (кураторы для l_table_01 и l_table_00)
    if (pi_curators_include = 1) then
      select distinct ORH.Root_Org_pId bulk collect
        into l_table_02
        from mv_org_tree ORH
       where (ORH.ORG_ID in
             (select *
                 from TABLE(l_table_01)
               union all
               select * from TABLE(l_table_00)))
         and (ORH.ORG_RELTYPE <> 1001)
         and (not ORH.ORG_PID = -1);
    end if;
    -- Результат
    select distinct ORG_ID bulk collect
      into res
      from (select /*+ PRECOMPUTE_SUBQUERY */
             column_value ORG_ID
              from TABLE(l_table_01)
            union
            select /*+ PRECOMPUTE_SUBQUERY */
             column_value ORG_ID
              from TABLE(l_table_00)
            union
            select /*+ PRECOMPUTE_SUBQUERY */
             column_value ORG_ID
              from TABLE(l_table_02));
    return res;
  end Get_Orgs;
  ------------------------------------------------------------------------
  -- Функция возвращающая дерево организаций видимое пользователю
  ------------------------------------------------------------------------
  function Get_Orgs_Tree(pi_org_id    in t_organizations.org_id%type := 1, --организация, от котрой строится дерево
                         pi_org_type  in num_tab, --типы организаций, возвращаемых в результате
                         pi_block_org in pls_integer := 0, -- показывать ли заблокированные организации
                         pi_block_dog in pls_integer := 0, -- показывать ли организации с заблокированными договрами
                         pi_worker_id in number, -- пользователь
                         po_err_num   out pls_integer,
                         po_err_msg   out varchar2) return sys_refcursor is
    res        sys_refcursor;
    l_org_type num_tab := pi_org_type;
    l_Rel_Id   num_tab;
    l_org_id   t_organizations.org_id%type := pi_org_id;
  begin
    if (pi_org_id is null) then
      l_org_id := 1;
    end if;

    if (pi_org_type is null or pi_org_type.count = 0) then
      l_org_type := num_tab(1001, 1002, 1003, 1004, 999);
    end if;

    select t.id bulk collect
      into l_Rel_Id
      from (select tor.*, sys_connect_by_path(tor.org_reltype, '/') lvl2
              from t_org_relations tor
              join t_organizations org
                on org.org_id = tor.org_id
              left join t_dogovor dog
                on dog.org_rel_id = tor.id
             where (tor.org_id <> l_org_id or l_org_id = 1)
            connect by prior tor.org_id = tor.org_pid
                      -- показывать ли заблокированные организации
                   and ((pi_block_org = 0 and NVL(org.is_enabled, 0) <> 0) or
                       pi_block_org = 1)
                      -- показывать ли организации с заблокированными договорами
                   and ((pi_block_Dog = 0 and NVL(dog.is_enabled, 1) <> 0) or
                       pi_block_Dog = 1)
             start with ((tor.org_id = l_org_id and l_org_id != 1 and
                        tor.org_pid <> -1) or
                        (l_org_id = 1 and tor.org_id = 1 and
                        tor.org_reltype = 1002))) t
     where
    /*структуру - всегда выводим*/
     (instr(t.lvl2, '1004') = 0 or instr(t.lvl2, '999') = 0
     /*1001 - точки продаж*/
     or
     (1001 in (select * from table(l_org_type)) and t.org_reltype = 1001)
     /*1002 - структуры принципала*/
     or (1002 in (select * from table(l_org_type)) and
     is_org_usi(t.org_id) = 1)
     /*1004 - дилеры и СП*/
     or ((1004 in (select * from table(l_org_type)) or
     999 in (select * from table(l_org_type))) and
     t.org_reltype in (1004, 1006, 999))
     /*1003 - сервис-провайдеры*/
     or (1003 in (select * from table(l_org_type)) and
     (t.org_reltype = 1004 and orgs.Is_Make_Dog_Perm(t.org_id) = 1)))
     and t.org_id in (select distinct tor.org_id
                    from t_org_relations tor
                  connect by prior tor.org_id = tor.org_pid
                   start with tor.org_id in
                              (select tuo.org_id
                                 from t_user_org tuo
                                where tuo.usr_id = pi_worker_id)
                  union all
                  select distinct tor2.org_pid
                    from t_org_relations tor2
                   where tor2.org_id in
                         (select tuo.org_id
                            from t_user_org tuo
                           where tuo.usr_id = pi_worker_id)
                     and tor2.org_pid >= 0);
    open res for
      select distinct tor.org_id,
                      tor.org_pid,
                      org.org_name ORG_NAME,
                      tor.org_reltype,
                      (case
                        when dog.dog_id is not null then
                         1
                        else
                         0
                      end) h_dog,
                      (case
                        when (exists
                              (select tor.org_id
                                 from t_org_relations tt
                                where tt.org_pid = tor.org_id
                                  and tt.org_reltype = 1001
                                  and tt.id in
                                      (Select column_Value from table(l_Rel_Id)))) then
                         1
                        else
                         0
                      end) h_sub,
                      (case
                        when (exists
                              (select tor.org_id
                                 from t_org_relations tt
                                where tt.org_pid = tor.org_id
                                  and tt.id in
                                      (Select column_Value from table(l_Rel_Id)))) then
                         1
                        else
                         0
                      end) h_any_child,
                      (case
                        when ((select count(occ.cc_id)
                                 from t_org_calc_center OCC
                                where occ.org_id = tor.org_id) = 1) then
                         (orgs.concat_cc_names(tor.org_id))
                        else
                         Null
                      end) cc_names,
                      org.region_id,
                      (case
                        when tor.org_pid = -1 -- тип связи - курирование
                         then
                         0
                        else
                         1
                      end) TIP,
                      vot.lvl
        from table(l_Rel_Id) tt
        Join t_org_relations tor
          on tor.Id = tt.column_value
        Join t_Organizations org
          on org.Org_Id = tor.org_id
        join V_Org_Tree vot
          on vot.id = tor.id
        left join t_dogovor dog
          on dog.org_rel_id = tor.id
      Connect by prior tor.org_id = tor.org_pid
       start with tor.org_pid = l_org_id
              and l_org_id != 1
               or (tor.org_id in
                  (select distinct Decode(tor.org_pid,
                                           -1,
                                           tor.org_id,
                                           tor.org_pid)
                      from t_org_relations tor
                     where tor.org_id in
                           (select tuo.org_id
                              from t_user_org tuo
                             where tuo.usr_id = pi_worker_id)
                       and tor.org_pid >= 0
                        or tor.org_reltype = 1002) and
                  (tor.org_pid >= 0 or tor.org_reltype = 1002))
       order by /*tor.org_reltype,*/ org.region_id, vot.lvl, org.org_name;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Orgs_Tree;

  ------------------------------------------------------------------------
  -- Функция возвращающая массив организаций видимых пользователю
  function Get_User_Orgs_Tab_With_Param(pi_worker_id         in T_USERS.USR_ID%type, -- пользователь
                                        pi_org_id            in T_ORGANIZATIONS.ORG_ID%type := null, -- организация с которой начинаем
                                        pi_self_include      in pls_integer := 1, -- включать pi_org_id
                                        pi_childrens_include in pls_integer := 1, -- включать детей
                                        pi_curated_include   in pls_integer := 1, -- включать курируемых
                                        pi_curators_include  in pls_integer := 0) -- включать кураторов
   return num_tab is
    res        num_tab := null;
    l_table_0  num_tab := num_tab();
    l_table_1  num_tab := num_tab();
    l_table_2  num_tab := num_tab();
    l_table_02 num_tab := num_tab();
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
      select distinct UO.ORG_ID bulk collect
        into l_table_0
        from T_USER_ORG UO
       where UO.USR_ID = pi_worker_id
         and pi_org_id = UO.ORG_ID;
    end if;

    -- Организации, которые связаны с l_table_0 отношениями типа подчиненности (дети для l_table_0)
    if (pi_childrens_include = 1) then
      select distinct ORH.ORG_ID bulk collect
        into l_table_1
        from mv_org_tree ORH
       where (ORH.ORG_PID in (select * from TABLE(l_table_0)))
         and ORH.ORG_RELTYPE = 1001;
    end if;

    -- Организации, которые связаны с l_table_0 и l_table_1 отношениями типа курирование (где l_table_0 и l_table_1 - кураторы)
    if (pi_curated_include = 1) then
      select distinct ORH.ORG_ID bulk collect
        into l_table_2
        from mv_org_tree ORH
       where ((ORH.ROOT_ORG_pID in (select * from TABLE(l_table_0))) or
             (ORH.ROOT_ORG_pID in (select * from TABLE(l_table_1))))
         and (ORH.ROOT_RELTYPE In (1003, 1004, 1008, 999));
    end if;

    -- Если не надо включать pi_org_id
    if ((pi_org_id is not null) and (pi_self_include <> 1)) then
      l_table_0 := new num_tab();
    end if;
    -- Организации, которые связаны с l_table_0, l_table_1 и l_table_2  отношениями типа
    -- подчиненности (кураторы для l_table_0, l_table_1 и l_table_2)
    if (pi_curators_include = 1) then
      select distinct ORH.ROOT_ORG_PID bulk collect
        into l_table_02
        from mv_org_tree ORH
       where (ORH.ORG_ID in (select *
                               from TABLE(l_table_0)
                             union all
                             select *
                               from TABLE(l_table_1)
                             union all
                             select * from TABLE(l_table_2)));
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
                        select * from TABLE(l_table_02));
    return res;
  end Get_User_Orgs_Tab_With_Param;
  ------------------------------------------------------------------
  function concat_cc_names(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return t_vc200 is
    res         t_vc200 := '';
    tmp_cc_name T_CALC_CENTER.CC_NAME%type;
    cur1        sys_refcursor;
    count_0     pls_integer := 0;
  begin
    open cur1 for
      select CC.CC_NAME
        from T_ORG_CALC_CENTER OCC, T_CALC_CENTER CC
       where OCC.ORG_ID = pi_org_id
         and OCC.CC_ID = CC.CC_ID
       order by CC.CC_NAME asc;
    loop
      fetch cur1
        into tmp_cc_name;
      exit when cur1%notfound;
      if (count_0 > 0) then
        res := res || '$';
      end if;
      res     := res || tmp_cc_name;
      count_0 := count_0 + 1;
    end loop;
    close cur1;
    return res;
  end concat_cc_names;
  ----------------------------------------------------------------------------
  function Get_Calc_Center_List(pi_org_region_id in T_ORGANIZATIONS.ORG_ID%type, -- организация по региону которой берем CC
                                pi_region_id     in t_dic_region.REG_ID%type, -- регион в котором берем CC
                                pi_kladr_code    in KLADR.CODE%type, -- код населенного пункта в КЛАДР для которого берем CC
                                pi_org_id        in T_ORGANIZATIONS.ORG_ID%type, -- организация (МРК) для которой берем CC
                                pi_worker_id     in T_USERS.USR_ID%type, -- ограничение по пользователю(>организации>регионы)
                                po_err_num       out pls_integer,
                                po_err_msg       out t_Err_Msg)
    return sys_refcursor is
    res     sys_refcursor;
    org_tab num_tab := num_tab();
  begin
    logging_pkg.debug(':pi_org_region_id:=' || pi_org_region_id || ';
                :pi_region_id    :=' || pi_region_id || ';
                :pi_kladr_code   :=' || pi_kladr_code || ';
                :pi_org_id       :=' || pi_org_id || ';
                :pi_worker_id    :=' || pi_worker_id || ';',
                      'Get_Calc_Center_List');
    if (pi_org_region_id is null) then
      org_tab := Get_User_Orgs_Tab(pi_worker_id);
    else
      org_tab := Orgs.Get_Orgs(pi_worker_id, pi_org_region_id);
    end if;
    open res for
      select distinct CC.CC_ID,
                      CC.CC_NAME,
                      CC.CC_REGION_ID,
                      CC.KLADR_CODE,
                      CC.MRK_ORG_ID ORG_ID
        from T_CALC_CENTER CC, T_ORGANIZATIONS O
       where (pi_org_id is null or pi_org_id = CC.MRK_ORG_ID)
         and (CC.CC_REGION_ID = O.REGION_ID)
         and (O.ORG_ID in (select * from TABLE(org_tab)))
         and (pi_kladr_code is null or pi_kladr_code = CC.KLADR_CODE)
         and (pi_region_id is null or pi_region_id = CC.CC_REGION_ID)
       order by CC.CC_NAME;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace || ' (' ||
                    dbms_utility.format_error_backtrace || ')';
      return null;
  end Get_Calc_Center_List;
  ----------------------------------------------------------------------------
  function Get_Calc_Center_List2(pi_org_rel_id in T_ORGANIZATIONS.ORG_ID%type, -- организация по связи которой через T_ORG_CALC_CENTER берем CC
                                 pi_region_id  in t_dic_region.REG_ID%type, -- регион в котором берем CC
                                 pi_tariff_id  in T_TARIFF2.ID%type, -- тариф, по типу и региону которого ограничим РЦ
                                 pi_kladr_code in KLADR.CODE%type, -- код населенного пункта в КЛАДР для которого берем CC
                                 pi_org_id     in T_ORGANIZATIONS.ORG_ID%type, -- организация (МРК) для которой берем CC
                                 pi_org_id2    in T_ORGANIZATIONS.ORG_ID%type, -- организация, для которой надо взять родителей и кураторов + ее саму и взять их CC через T_ORG_CALC_CENTER
                                 pi_worker_id  in T_USERS.USR_ID%type, -- ограничение по пользователю(>организации>регионы)
                                 po_err_num    out pls_integer,
                                 po_err_msg    out t_Err_Msg)
    return sys_refcursor is
    res        sys_refcursor;
    org_tab    num_tab := num_tab();
    org_tab2   num_tab := num_tab();
    cc_tab     num_tab;
    region_tab num_tab;
  begin
    logging_pkg.debug(':pi_org_rel_id :=' || pi_org_rel_id || ';
                :pi_region_id  :=' || pi_region_id || ';
                :pi_tariff_id  :=' || pi_tariff_id || ';
                :pi_kladr_code :=' || pi_kladr_code || ';
                :pi_org_id     :=' || pi_org_id || ';
                :pi_org_id2    :=' || pi_org_id2 || ';
                :pi_worker_id  :=' || pi_worker_id || ';',
                      'Get_Calc_Center_List');

    if (pi_org_rel_id is not null) then
      org_tab := Orgs.Get_Orgs(pi_worker_id, pi_org_rel_id, 0, 1, 0, 0);
    end if;

    if (pi_org_id2 is not null) then
      select distinct O.ORG_ID bulk collect
        into org_tab2
        from T_ORGANIZATIONS O
       where O.ORG_ID in (select *
                            from TABLE(num_tab(pi_org_id2))
                          union all
                          select distinct ORE.ORG_PID
                            from T_ORG_RELATIONS ORE
                           where ORE.ORG_ID = pi_org_id2);
    end if;

    if (pi_org_id2 = 0 and pi_org_rel_id is null) then
      if pi_tariff_id is not null then
        select cc.cc_id bulk collect
          into cc_tab
          from t_abstract_tar at
          join t_tariff2 t
            on at.at_id = t.at_id
          JOIN T_DIC_MVNO_REGION DMR
            ON DMR.ID = at.at_region_id
          join t_calc_center cc
            on DMR.REG_ID = cc.cc_region_id
         where pi_tariff_id = t.id
           and cc_type_is(cc.cc_types, t.type_vdvd_id) = 1;
      end if;

      open res for
        select distinct cc.cc_id,
                        concat(cc.cc_remote_id, concat(' ', cc.cc_name)) cc_name,
                        cc.cc_region_id,
                        cc.kladr_code,
                        cc.mrk_org_id org_id,
                        cc.cc_remote_id
          from t_calc_center cc
         where (pi_org_id is null or pi_org_id = cc.mrk_org_id)
           and (pi_kladr_code is null or pi_kladr_code = cc.kladr_code)
           and (pi_region_id is null or pi_region_id = cc.cc_region_id)
           and (pi_tariff_id is null or
               cc.cc_id in (select column_value from table(cc_tab)))
         order by cc.cc_region_id, cc.cc_remote_id;
    else
      if (pi_region_id is not null or pi_kladr_code is not null) and
         pi_tariff_id is not null then
        select DMR.REG_ID bulk collect
          into region_tab
          from t_abstract_tar t
          JOIN T_DIC_MVNO_REGION DMR
            ON DMR.ID = T.at_region_id
         where t.at_id = pi_tariff_id
        intersect
        select r.reg_id
          from t_dic_region r
         where r.reg_id = pi_region_id
            or r.kl_region = pi_kladr_code;
      elsif pi_tariff_id is not null then
        select DMR.REG_ID bulk collect
          into region_tab
          from t_abstract_tar t
          JOIN T_DIC_MVNO_REGION DMR
            ON DMR.ID = T.at_region_id
         where t.at_id = pi_tariff_id;
      elsif pi_region_id is not null or pi_kladr_code is not null then
        select R.REG_ID bulk collect
          into region_tab
          from t_dic_region r
         where r.reg_id = pi_region_id
            or r.kl_region = pi_kladr_code;
      else
        null;
      end if;

      select cc.cc_id bulk collect
        into cc_tab
        from t_org_calc_center cc
       where cc.org_id in (select *
                             from TABLE(org_tab)
                           union all
                           select * from TABLE(org_tab2))
         and (pi_org_rel_id is null or pi_org_rel_id = cc.org_id);

      open res for
        select cc.cc_id,
               concat(cc.cc_remote_id, concat(' ', cc.cc_name)) cc_name,
               cc.cc_region_id,
               cc.kladr_code,
               cc.mrk_org_id org_id,
               cc.cc_remote_id
          from t_calc_center cc
         where cc.cc_id in (select column_value from table(cc_tab))
           and ((pi_region_id is null and pi_tariff_id is null) or
               (cc.cc_region_id in
               (select column_value from table(region_tab))))
         order by cc.cc_region_id, cc.cc_remote_id;
    end if;
    return res;
  exception
    when ex_org_id_invalid then
      po_err_num := 1001;
      po_err_msg := 'Не указан ИД организации.';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Calc_Center_List2;
  ----------------------------------------------------------------------------
  --
  function Get_Calc_Center_List3(pi_org_id       in array_num_2,
                                 pi_block        in number,
                                 pi_org_relation in num_tab,
                                 pi_region_id    in t_dic_region.REG_ID%type, -- регион в котором берем CC
                                 pi_tariff_id    in T_TARIFF2.ID%type, -- тариф, по типу и региону которого ограничим РЦ
                                 pi_kladr_code   in KLADR.CODE%type, -- код населенного пункта в КЛАДР для которого берем CC
                                 pi_worker_id    in T_USERS.USR_ID%type, -- ограничение по пользователю(>организации>регионы)
                                 po_err_num      out pls_integer,
                                 po_err_msg      out t_Err_Msg)
    return sys_refcursor is
    res      sys_refcursor;
    org_tab  num_tab := num_tab();
    org_tab2 num_tab := num_tab();
  begin

    org_tab := get_orgs_tab_for_multiset(pi_orgs         => pi_org_id,
                                         Pi_worker_id    => pi_worker_id,
                                         pi_block        => pi_block,
                                         pi_org_relation => pi_org_relation,
                                         pi_is_rtmob     => 0);

    if (pi_org_id is null) then
      open res for
        select distinct CC.CC_ID,
                        CONCAT(CC.CC_REMOTE_ID, CONCAT(' ', CC.CC_NAME)) CC_NAME,
                        CC.CC_REGION_ID,
                        CC.KLADR_CODE,
                        CC.MRK_ORG_ID ORG_ID,
                        CC.CC_REMOTE_ID
          from T_CALC_CENTER     CC,
               t_Tariff2         T,
               T_ABSTRACT_TAR    AT,
               T_DIC_MVNO_REGION DMR
         where /*(pi_org_id is null or pi_org_id = CC.MRK_ORG_ID)
                                                                                                                                                                                                                                   and*/
         (pi_kladr_code is null or pi_kladr_code = CC.KLADR_CODE)
         and (pi_region_id is null or pi_region_id = CC.CC_REGION_ID)
         AND DMR.REG_ID = CC.CC_REGION_ID
         and (pi_tariff_id is null or
         (pi_tariff_id = T.ID and AT.AT_ID = T.AT_ID and
         AT.AT_REGION_ID = DMR.ID and
         cc_type_is(CC.CC_TYPES, T.TYPE_VDVD_ID) = 1))
         order by CC.CC_REGION_ID, CC.CC_REMOTE_ID;
    else
      open res for
        select distinct CC.CC_ID,
                        CONCAT(CC.CC_REMOTE_ID, CONCAT(' ', CC.CC_NAME)) CC_NAME,
                        CC.CC_REGION_ID,
                        CC.KLADR_CODE,
                        CC.MRK_ORG_ID ORG_ID,
                        CC.CC_REMOTE_ID
          from T_CALC_CENTER     CC,
               T_ORG_CALC_CENTER OCC,
               T_ABSTRACT_TAR    AT,
               T_ORGANIZATIONS   O,
               t_dic_mvno_region dmr
         where OCC.CC_ID = CC.CC_ID
           and OCC.ORG_ID = O.ORG_ID
              --and (pi_org_id is null or pi_org_id = CC.MRK_ORG_ID)
           and (OCC.ORG_ID in
               (select *
                   from TABLE(org_tab)
                 union all
                 select * from TABLE(org_tab2)))
              --and (pi_org_rel_id is null or pi_org_rel_id = OCC.ORG_ID)
           and (pi_kladr_code is null or pi_kladr_code = CC.KLADR_CODE)
           and (pi_region_id is null or
               (pi_region_id = CC.CC_REGION_ID and
               (O.REGION_ID is null or O.REGION_ID = -1 or
               CC.CC_REGION_ID = O.REGION_ID)))
           and dmr.reg_id = cc.cc_region_id
           and (pi_tariff_id is null or
               (pi_tariff_id = AT.AT_ID and AT.AT_REGION_ID = DMR.ID and
               --DMR.REG_ID = CC.CC_REGION_ID AND
               (O.REGION_ID is null or O.REGION_ID = -1 or
               CC.CC_REGION_ID = O.REGION_ID)))
         order by CC.CC_REGION_ID, CC.CC_REMOTE_ID;
    end if;
    return res;
  exception
    when ex_org_id_invalid then
      po_err_num := 1001;
      po_err_msg := 'Не указан ИД организации.';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  ------------------------------------------------------------------------
  function Is_Have_USI_Job(pi_worker in T_USERS.USR_ID%type)
    return pls_integer is
    ret pls_integer;
  begin
    if (pi_worker is null) then
      return 0;
    end if;
    select count(*)
      into ret
      from mv_org_tree ORH, T_USER_ORG UO
     where UO.USR_ID = pi_worker
       and UO.ORG_ID = ORH.ORG_ID
       and ORH.ROOT_RELTYPE = 1002
       and orh.org_reltype != 1009;
    if (ret > 0) then
      ret := 1;
    end if;
    return ret;
  end Is_Have_USI_Job;
  ------------------------------------------------------------------------
  function Is_Have_SP_Job(pi_worker in T_USERS.USR_ID%type)
    return pls_integer is
    ret pls_integer;
  begin
    if (pi_worker is null) then
      return 0;
    end if;
    select count(*)
      into ret
      from mv_org_tree ORH, T_USER_ORG UO
     where UO.USR_ID = pi_worker
       and UO.ORG_ID = ORH.ORG_ID
       and ORH.Root_Reltype = 1003;
    if (ret > 0) then
      ret := 1;
    end if;
    return ret;
  end Is_Have_SP_Job;
  ------------------------------------------------------------------------
  function Is_Have_Parent_Job(pi_worker in T_USERS.USR_ID%type,
                              pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return pls_integer is
    ret pls_integer;
  begin
    if (pi_worker is null or pi_org_id is null) then
      return 0;
    end if;
    Select Count(*)
      into ret
      from (select tor.ORG_pID
              from mv_org_tree tor
            Connect by prior tor.org_pid = tor.org_id
             start with tor.ORG_ID = pi_org_id) t
      join T_USER_ORG UO
        on UO.ORG_ID = t.ORG_pID
       and UO.USR_ID = pi_worker
     where RowNum <= 1;
    return ret;
  end Is_Have_Parent_Job;
  ------------------------------------------------------------------------
  function Get_Jobs_Number(pi_worker in T_USERS.USR_ID%type)
    return pls_integer is
    ret pls_integer;
  begin
    if (pi_worker is null) then
      return 0;
    end if;
    select count(*)
      into ret
      from (select distinct UO.ORG_ID
              from T_USER_ORG UO
             where UO.USR_ID = pi_worker);
    return ret;
  end Get_Jobs_Number;
  ------------------------------------------------------------------------
  function Get_Job_Org_Id_If_One(pi_worker in T_USERS.USR_ID%type)
    return T_ORGANIZATIONS.ORG_ID%type is
    ret T_ORGANIZATIONS.ORG_ID%type;
  begin
    if (pi_worker is null) then
      return null;
    end if;

    if (Get_Jobs_Number(pi_worker) = 1) then
      select ORG_ID
        into ret
        from (select distinct UO.ORG_ID
                from T_USER_ORG UO
               where UO.USR_ID = pi_worker);

      return ret;
    end if;
    return null;
  end Get_Job_Org_Id_If_One;

  ------------------------------------------------------------------------
  function Get_Job_Org_Type_If_One(pi_worker in T_USERS.USR_ID%type)
    return T_RELATION_TYPE.REL_TP_ID%type is
    ret T_RELATION_TYPE.REL_TP_ID%type;
  begin
    if (pi_worker is null) then
      return null;
    end if;

    if (Get_Jobs_Number(pi_worker) = 1) then
      BEGIN
        select distinct ORH.ROOT_RELTYPE
          into ret
          from mv_org_tree ORH
          join t_org_is_rtmob rt
            on rt.org_id = ORH.ORG_ID
          left join t_org_is_rtmob rt_p
            on rt_p.org_id = ORH.ORG_pID
         where ORH.ORG_ID = (select distinct UO.ORG_ID
                               from T_USER_ORG UO
                              where UO.USR_ID = pi_worker)
           and (ORH.ORG_RELTYPE not in (1006, 1009))
           and nvl(rt.is_org_rtm, 0) =
               nvl(rt_p.is_org_rtm, nvl(rt.is_org_rtm, 0));
        return ret;
      EXCEPTION
        WHEN TOO_MANY_ROWS THEN
          return null;
      END;
    end if;

    return null;
  end Get_Job_Org_Type_If_One;
  ------------------------------------------------------------------------
  function Is_Have_Rel_Childrens(pi_worker   in T_USERS.USR_ID%type,
                                 pi_rel_type in T_ORG_RELATIONS.ORG_RELTYPE%type)
    return pls_integer is
    ret pls_integer := 0;
  begin
    if (pi_worker is null) then
      return 0;
    end if;
    select count(*)
      into ret
      from T_ORG_RELATIONS ORL, T_USER_ORG UO
     where UO.USR_ID = pi_worker
       and UO.ORG_ID = ORL.ORG_PID
       and ORL.ORG_RELTYPE = pi_rel_type;
    return ret;
  end Is_Have_Rel_Childrens;
  ------------------------------------------------------------------------
  function Get_Root_Org_Or_Self(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return number is
    tmp number;
  begin
    Select MAX(decode(t.root_org_id,
                      1,
                      decode(is_org_rtmob(t.org_id), 0, 0, 1, 2, 1),
                      t.root_org_id))
      into tmp
      from mv_org_tree t
     where t.org_id = pi_org_id
       and t.root_reltype in (1004, 1002, 1007, 999)
       and nvl(is_org_rtmob(t.org_id), 1) = nvl(is_org_rtmob(t.org_pid), 1);
    return tmp;
  exception
    when others then
      return null;
  end Get_Root_Org_Or_Self;
  ------------------------------------------------------------------------
  function Get_Parent_Org(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return T_ORGANIZATIONS.ORG_ID%type is
    res T_ORGANIZATIONS.ORG_ID%type;
  begin
    select ORG_PID
      into res
      from T_ORG_RELATIONS
     where ORG_ID = pi_org_id
       and ORG_RELTYPE = 1001;
    return res;
  end Get_Parent_Org;
  ------------------------------------------------------------------------
  function Get_Parent_Root_Org(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return T_ORGANIZATIONS.ORG_ID%type is
    res T_ORGANIZATIONS.ORG_ID%type;
  begin
    select Max(ORG_PID)
      into res
      from T_ORG_RELATIONS t
     where ORG_ID = pi_org_id
       and t.org_reltype not in ('1005', '1009');
    return res;
  end Get_Parent_Root_Org;
  ----------------------------------------------------------------------------------------------------
  function Get_Org_Name(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select O.ORG_NAME,
             is_org_rtmob(o.org_id) is_org_rtmob,
             is_org_filial(o.org_id) is_org_filial
        from T_ORGANIZATIONS O
       where O.ORG_ID = pi_org_id;
    return res;
  end Get_Org_Name;
  ----------------------------------------------------------------------------------------------------
  function Get_Org_Name1(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return T_ORGANIZATIONS.ORG_NAME%type is
    res T_ORGANIZATIONS.ORG_NAME%type := '';
  begin
    begin
      select O.ORG_NAME
        into res
        from T_ORGANIZATIONS O
       where O.ORG_ID = pi_org_id;
    exception
      when others then
        return '';
    end;
    return res;
  end Get_Org_Name1;
  ----------------------------------------------------------------------------------------------------
  -- возвращает полное наименование юр. лица
  function Get_Org_Full_Name(pi_org_id in number) return varchar2 is
    res T_ORGANIZATIONS.ORG_NAME%type := '';
  begin
    select O.ORG_FULL_NAME
      into res
      from T_ORGANIZATIONS O
     where O.ORG_ID = pi_org_id;
    return res;
  exception
    when others then
      return '';
  end Get_Org_Full_Name;

  ------------------------------------------------------------------------
  --добавлена 1008 связь(задача 68638)
  function Get_Dogovor_By_Org(pi_org_id      in T_DOGOVOR.DOG_ID%type,
                              pi_worker_id   in T_USERS.USR_ID%type,
                              po_report_form out sys_refcursor,
                              po_err_num     out pls_integer,
                              po_err_msg     out t_Err_Msg)
    return sys_refcursor is
    res            sys_refcursor;
    l_user_org_tab num_tab;
  begin
    l_user_org_tab := GET_USER_ORGS_TAB(pi_worker_id);
    open res for
      select D.DOG_ID,
             D.DOG_NUMBER,
             D.DOG_DATE,
             D.ORG_REL_ID,
             D.IS_VIS_AUTO,
             ORL.ORG_PID,
             ORL.ORG_ID,
             ORL.ORG_RELTYPE,
             D.IS_ENABLED,
             D.PREMIA_SCHEMA,
             D.WITH_NDS,
             D.V_LICE,
             D.NA_OSNOVANII,
             D.CONN_TYPE,
             d.codes,
             o.is_pay_espp,
             d.dog_class_id,
             d.is_accept,
             dd.url,
             d.org_name_sprav,
             d.dog_number_sprav,
             d.org_legal_form,
             d.payment_type,
             d.m2m_type,
             d.temporary_indent,
             d.max_sim_count,
             d.is_employee_number_required
        from T_DOGOVOR         D,
             T_ORG_RELATIONS   ORL,
             T_ORGANIZATIONS   O,
             t_dogovor_details dd
       where O.ORG_ID = pi_org_id
         and O.ROOT_ORG_ID = ORL.ORG_ID
         and ORL.ID = D.ORG_REL_ID(+)
         and d.dog_id = dd.dog_id(+)
         and ORL.ORG_RELTYPE in (1002, 1003, 1004, 1007, 1008, 999)
         and (ORL.ORG_PID in (select *
                                from TABLE(l_user_org_tab)
                              union all
                              select -1 from dual) or
             ORL.ORG_ID in (select * from TABLE(l_user_org_tab)));

    open po_report_form for
      select dd.dog_type, dd.date_dog_type, dd.dog_id
        from T_DOGOVOR             D,
             T_ORG_RELATIONS       ORL,
             T_ORGANIZATIONS       O,
             t_dogovor_report_form dd
       where O.ORG_ID = pi_org_id
         and O.ROOT_ORG_ID = ORL.ORG_ID
         and ORL.ID = D.ORG_REL_ID(+)
         and d.dog_id = dd.dog_id
         and ORL.ORG_RELTYPE in
             ('1002', '1003', '1004', '1007', '1008', '999')
         and (ORL.ORG_PID in (select *
                                from TABLE(l_user_org_tab)
                              union all
                              select -1 from dual) or
             ORL.ORG_ID in (select * from TABLE(l_user_org_tab)));
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Dogovor_By_Org;
  ------------------------------------------------------------------------

  ------------------------------------------------------------------------
  function Get_Dogovor(pi_dogovor_id  in T_DOGOVOR.DOG_ID%type,
                       pi_worker_id   in T_USERS.USR_ID%type,
                       po_prm_cur     out sys_refcursor, -- курсор разрешений
                       po_report_form out sys_refcursor,
                       po_region_tab  out sys_refcursor,
                       po_err_num     out pls_integer,
                       po_err_msg     out t_Err_Msg) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select D.DOG_ID,
             D.DOG_NUMBER,
             D.DOG_DATE,
             D.ORG_REL_ID,
             D.IS_VIS_AUTO,
             ORL.ORG_PID,
             ORL.ORG_ID,
             ORL.ORG_RELTYPE,
             D.IS_ENABLED,
             D.PREMIA_SCHEMA,
             D.WITH_NDS,
             D.V_LICE,
             D.NA_OSNOVANII,
             D.CONN_TYPE,
             tas.id_schema,
             tas.overdraft,
             D.Codes,
             org.is_pay_espp,
             d.dog_class_id,
             d.is_accept,
             dd.url,
             dd.url2,
             -- 33537 olia_serg
             d.org_name_sprav,
             d.dog_number_sprav,
             d.org_legal_form,
             d.percent_stb,
             d.priznak_stb,
             d.payment_type,
             d.m2m_type,
             d.temporary_indent,
             d.max_sim_count,
             d.is_employee_number_required
        from T_ORG_RELATIONS ORL
        join t_dogovor d
          on d.org_rel_id = orl.id
        left join t_dogovor_details dd
          on dd.dog_id = d.dog_id
        Join t_Organizations org
          on org.Org_Id = orl.org_id
        left join t_acc_owner tao
          on d.dog_id = tao.owner_ctx_id
        left join t_accounts ta
          on ta.acc_owner_id = tao.owner_id
         and ta.acc_type = acc_operations.c_acc_type_lic
        left join t_acc_schema tas
          on tas.id_acc = ta.acc_id
       where D.DOG_ID = pi_dogovor_id
         and D.ORG_REL_ID = ORL.ID
         and ORL.ORG_ID in
             (select * from TABLE(GET_USER_ORGS_TAB(pi_worker_id, 0)));

    open po_prm_cur for
      select dp.dp_prm_id      as perm,
             p.prm_is_out_cc   as is_out_cc,
             p.prm_is_out_terr as is_out_terr
        from t_dogovor_prm dp
        join t_perm p
          on dp.dp_prm_id = p.prm_id
       where dp.dp_dog_id = pi_dogovor_id
         and dp.dp_is_enabled = 1;

    open po_report_form for
      select t.dog_type, t.date_dog_type
        from t_dogovor_report_form t
       where t.dog_id = pi_dogovor_id;

    open po_region_tab for
      select t.kl_region, t.kl_name
        from t_dic_region t
        join t_dogovor d
          on d.dog_id = pi_dogovor_id
        join t_org_relations re
          on re.id = d.org_rel_id --
        join t_dic_mrf m
          on m.org_id = re.org_pid
         and t.mrf_id = m.id
       where t.reg_id in (select o.region_id
                            from mv_org_tree tor
                            join t_organizations o
                              on o.org_id = tor.org_id
                             and o.is_enabled = 1
                           where tor.org_pid = re.org_id
                             and tor.dog_id = pi_dogovor_id);
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Dogovor;
  ------------------------------------------------------------------------

  ------------------------------------------------------------------------
  -- Изменение договора
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
                           -- ** Nemtseva (множ. договора) beg **
                           pi_list_prm in num_tab, -- список разрешений для обновления договора (мб null)
                           -- ** Nemtseva (множ. договора) end **
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
                           pi_org_name_sprav      in t_dogovor.org_name_sprav%type,
                           pi_dog_number_sprav    in t_dogovor.dog_number_sprav%type,
                           pi_org_legal_form      in t_dogovor.org_legal_form%type,
                           pi_type_report_form    in num_date_tab,
                           pi_percent_stb         in t_dogovor.percent_stb%type,
                           pi_PRIZNAK_STB         in t_dogovor.PRIZNAK_STB%type,
                           pi_payment_type        in t_dogovor.payment_type%type,
                           pi_m2m_type            in t_dogovor.m2m_type%type,
                           pi_region_tab          in string_tab,
                           pi_temporary_indent    in t_dogovor.temporary_indent%type,
                           pi_max_sim_count       in t_dogovor.max_sim_count%type,
                           pi_required_emp_num       t_dogovor.is_employee_number_required%TYPE
                           ) is
    l_org_id number := null;
    --    ex_acc_denied exception;
    ex_not_enough_rights exception;
    l_err_num   pls_integer;
    l_err_msg   t_Err_Msg;
    l_org_pid   number;
    l_org_name  varchar2(255); --для fetch
    l_o_id      number;
    l_o_pid     number;
    l_res       sys_refcursor;
    l_dog_id    t_dogovor.dog_id%type;
    l_overdraft number;
    --    pi_codes varchar2(100);
    l_count_list_prm number;
    l_count          number;
    ex_dog_class_perm exception;
    l_is_org_rtm number;
  begin
    logging_pkg.info(':pi_dogovor_id  :=' || pi_dogovor_id || ';
               :pi_dog_number  :=''' || pi_dog_number || ''';
               :pi_region_tab  :=''' ||
                     get_str_by_string_tab(pi_region_tab) || ''';
               :pi_date        :=' ||
                     get_str_by_date(pi_date) || ';
               :pi_autovis     :=' || pi_autovis || ';
               :pi_worker_id   :=' || pi_worker_id || ';
               :pi_with_nds    :=' || pi_with_nds || ';
               :pi_pr_schema   :=' || pi_pr_schema || ';
               :pi_v_lice      :=''' || pi_v_lice || ''';
               :pi_na_osnovanii:=''' ||
                     pi_na_osnovanii || ''';
               :pi_conn_type   :=' || pi_conn_type || ';
               :pi_list_prm    :=' ||
                     get_str_by_num_tab(pi_list_prm) || ';
               :pi_acc_schema  :=' || pi_acc_schema || ';
               :pi_overdraft   :=' || pi_overdraft || ';
               :pi_org_list    :=' ||
                     get_str_by_num_tab(pi_org_list) || ';
               :pi_codes       :=''' || pi_codes || ''';
               :pi_is_pay_espp :=' || pi_is_pay_espp || ';
               :pi_is_accept   :=' || pi_is_accept || ';
               :pi_style_file1 :=''' || pi_style_file1 || ''';
               :pi_style_file2 :=''' || pi_style_file2 || ''';
               :pi_block       :=' || pi_block || ';' || '
               pi_max_sim_count' || pi_max_sim_count || '
               pi_required_emp_num=' || pi_required_emp_num,
                     'Change_Dogovor');

    if pi_dogovor_id is not null and nvl(pi_m2m_type, 0) > 0 then
      check_dog_m2m_type(pi_dog_id   => pi_dogovor_id,
                         pi_m2m_type => pi_m2m_type,
                         po_err_num  => po_err_num,
                         po_err_msg  => po_err_msg);
      if po_err_num <> 0 then
        return;
      end if;
    end if;

    if pi_overdraft = -1 then
      l_overdraft := 0;
    else
      l_overdraft := pi_overdraft;
    end if;

    -- получаем id организаций по номеру договора
    l_res := get_orgs_by_dogid(pi_dogovor_id, pi_worker_id);
    loop
      fetch l_res
        into l_org_id, l_org_pid, l_org_name, l_org_name;
      exit when l_res%NOTFOUND;
    end loop;
    close l_res;
    -- проверяем права на Редактирование договора с организацией (5209)
    if (not Security_pkg.Check_Rights_str('EISSD.ORGS.DOGOVOR.EDIT',
                                          l_org_pid,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg,
                                          false)) or /* and*/
       (Check_Mask_Orgs_Dog(l_org_pid, pi_list_prm, po_err_num, po_err_msg) = 0) then
      raise ex_not_enough_rights;
    end if;
    if (not Security_pkg.Check_Rights_str('EISSD.ORGS.DOGOVOR.EDIT' /*1001*/,
                                          l_org_pid, --l_org_id,
                                          pi_worker_id,
                                          l_err_num,
                                          l_err_msg,
                                          false)) then
      po_err_msg := l_err_msg;
      po_err_num := l_err_num;
      return;
    end if;

    -- 28.05.2010 №17192 begin
    -- Сохраняем старую версию в таблицу t_dogovor_hst
    insert into t_dogovor_hst
      (dog_id,
       dog_date,
       dog_number,
       org_rel_id,
       is_vis_auto,
       is_enabled,
       premia_schema,
       with_nds,
       v_lice,
       na_osnovanii,
       conn_type,
       codes,
       dog_class_id,
       is_accept,
       hst_date,
       hst_user,
       -- 33537 olia_serg
       org_name_sprav,
       dog_number_sprav,
       org_legal_form,
       percent_stb,
       PRIZNAK_STB,
       payment_type,
       m2m_type,
       temporary_indent,
       max_sim_count,
       is_employee_number_required)
      select d.dog_id,
             d.dog_date,
             d.dog_number,
             d.org_rel_id,
             d.is_vis_auto,
             d.is_enabled,
             d.premia_schema,
             d.with_nds,
             d.v_lice,
             d.na_osnovanii,
             d.conn_type,
             d.codes,
             d.dog_class_id,
             d.is_accept,
             sysdate,
             pi_worker_id,
             -- 33537 olia_serg
             d.org_name_sprav,
             d.dog_number_sprav,
             d.org_legal_form,
             d.percent_stb,
             d.PRIZNAK_STB,
             d.payment_type,
             d.m2m_type,
             d.temporary_indent,
             d.max_sim_count,
             d.is_employee_number_required
        from t_dogovor d
       where d.dog_id = pi_dogovor_id;
    -- 28.05.2010 №17192 end

    update T_DOGOVOR D
       set D.DOG_NUMBER    = pi_dog_number,
           D.DOG_DATE      = pi_date,
           D.IS_VIS_AUTO   = pi_autovis,
           D.PREMIA_SCHEMA = pi_pr_schema,
           D.WITH_NDS      = pi_with_nds,
           D.V_LICE        = pi_v_lice,
           D.NA_OSNOVANII  = pi_na_osnovanii,
           D.CONN_TYPE     = pi_conn_type,
           D.CODES         = pi_codes,
           D.IS_ACCEPT     = pi_is_accept,
           -- 33537 olia_serg
           d.org_name_sprav   = pi_org_name_sprav,
           d.dog_number_sprav = pi_dog_number_sprav,
           d.org_legal_form   = pi_org_legal_form,
           d.percent_stb      = pi_percent_stb,
           d.priznak_stb      = pi_PRIZNAK_STB,
           --           d.is_pay_espp   = pi_is_pay_espp,
           d.payment_type     = pi_payment_type,
           d.m2m_type         = nvl(pi_m2m_type, 0),
           d.temporary_indent = pi_temporary_indent,
           d.max_sim_count    = pi_max_sim_count,
           d.is_employee_number_required = pi_required_emp_num
     where D.DOG_ID = pi_dogovor_id;

    delete from t_dogovor_report_form t where t.dog_id = pi_dogovor_id;
    insert into t_dogovor_report_form
      (dog_id, dog_type, date_dog_type)
      select pi_dogovor_id, num1, dates from table(pi_type_report_form);

    merge into t_dogovor_details dd
    using (select pi_dogovor_id  as dog_id,
                  pi_style_file1 as css,
                  pi_style_file2 as css2
             from dual) s
    on (s.dog_id = dd.dog_id)
    when matched then
      update set dd.url = s.css where dd.dog_id = s.dog_id
    when not matched then
      insert (dd.dog_id, dd.url, dd.url2) values (s.dog_id, s.css, s.css2);

    -- проверка прав
    --задача № 37495
    select count(*) into l_count_list_prm from table(pi_list_prm);

    if (l_count_list_prm <> 0) then
      select count(*)
        into l_count
        from t_dic_dog_class_perm dd
       where dd.prm_id in (select column_value from table(pi_list_prm));

      if (l_count = 0) then
        raise ex_dog_class_perm;
      end if;
    else
      -- договор для которого не предусмотрены договорые пермишены, например, Сайт-Партнер
      select count(*)
        into l_count
        from t_pattern_rights t
       where t.dog_class_id in
             (select distinct d.dog_class_id
                from t_dogovor d
               where d.dog_id = pi_dogovor_id);

      if (l_count = 0) then
        raise ex_dog_class_perm;
      end if;
    end if;

    -- ** Nemtseva (множ. договора) beg**
    -- обновляем список разрешений
    --добавляем разрешения, которых ранее не было
    insert into t_dogovor_prm tdp
      (dp_dog_id, dp_prm_id, dp_is_enabled)
      (select pi_dogovor_id, plp.column_value, 1
         from table(pi_list_prm) PLP
        where plp.column_value not in
              (select tdp.dp_prm_id
                 from t_dogovor_prm TDP
                where tdp.dp_dog_id = pi_dogovor_id));

    -- для разрешений, которых нет в новом списке is_enabled = 0
    update t_dogovor_prm TDP
       set tdp.dp_is_enabled = 0
     where tdp.dp_dog_id = pi_dogovor_id
       and tdp.dp_is_enabled = 1
       and tdp.dp_prm_id not in
           (select plp.column_value from table(pi_list_prm) PLP);

    --определим кому принадлежат организации
    select max(t.is_org_rtm)
      into l_is_org_rtm
      from t_org_is_rtmob t
     where t.org_id in (l_org_id, l_org_pid);

    -- во всех подчинённых договорах для разрешений, которых нет
    -- в новом списке is_enabled = 0 /*21.04.09*/
    -- добавлено условие на is_org_rtm (72316)
    for oradog in (Select tor.org_pid, tor.org_id, td.dog_id
                     from t_org_relations tor
                     join t_org_is_rtmob t
                       on t.org_id = tor.org_id
                     left join t_dogovor td
                       on tor.id = td.org_rel_id
                    where td.dog_id is not null
                      and nvl(t.is_org_rtm, l_is_org_rtm) = l_is_org_rtm
                      and level > 1
                   Connect By prior tor.org_id = tor.org_pid
                    Start with tor.org_pid = l_org_pid
                           and td.dog_id = pi_dogovor_id) loop
      l_o_id   := oradog.org_id;
      l_o_pid  := oradog.org_pid;
      l_dog_id := oradog.dog_id;
      update t_dogovor_prm tdp
         set tdp.dp_is_enabled = 0
       where tdp.dp_is_enabled = 1
         and tdp.dp_dog_id = oradog.dog_id
         and tdp.dp_prm_id not in
             (select plp.column_value from table(pi_list_prm) PLP);

    end loop; /*21.04.09*/
    -- активируем разрешения, которые заблокированы и есть в новом списке
    update t_dogovor_prm TDP
       set tdp.dp_is_enabled = 1
     where tdp.dp_dog_id = pi_dogovor_id
       and tdp.dp_is_enabled = 0
       and tdp.dp_prm_id in
           (select plp.column_value from table(pi_list_prm) PLP);

    -- ** Nemtseva (множ. договора) end**
    /*23.06.09 (задача 8519)^ обновляем схему ведения счетов*/
    acc_operations.edit_acc_schema(pi_dogovor_id,
                                   pi_acc_schema,
                                   l_overdraft,
                                   po_err_num,
                                   po_err_msg);

    -- 19/10/2009 (Задача № 11710) Добавляем/удаляем доверенности
    if pi_org_list is not null then
      add_warrant(pi_dogovor_id,
                  l_org_id,
                  pi_org_list,
                  po_err_num,
                  po_err_msg);
    end if;

    if pi_block is not null then
      Block_Dogovor(pi_dogovor_id,
                    pi_block,
                    pi_worker_id,
                    po_err_num,
                    po_err_msg);
    else
      if pi_region_tab is not null and pi_region_tab.count <> 0 and
         pi_region_tab.first is not null then
        Add_subOrgs(l_org_id,
                    pi_dogovor_id,
                    pi_region_tab,
                    pi_worker_id,
                    po_err_num,
                    po_err_msg);
      end if;
      -- Пересчитаем таблицу T_ORG_RELTYPE_DOWN
      recreateorgreltypedown_for_org(l_org_id, null);
    end if;
    --удалим ненужные каналы продаж
    change_org_channel_dog(l_org_id, pi_worker_id, po_err_num, po_err_msg);
  exception
    when ex_acc_denied then
      po_err_num := 1001;
      po_err_msg := 'Ошибка. Указаный договор не найден в области видимости пользователя.';
    when ex_not_enough_rights then
      po_err_num := 1;
      po_err_msg := 'Недостаточно прав для редактирования договора!';
    when ex_dog_class_perm then
      po_err_num := 2;
      po_err_msg := 'Не переданы права договора';
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
  end Change_Dogovor;
  ------------------------------------------------------------------------
  procedure Block_Dogovor(pi_dogovor_id in T_DOGOVOR.DOG_ID%type,
                          pi_block      in pls_integer,
                          pi_worker_id  in T_USERS.USR_ID%type,
                          po_err_num    out pls_integer,
                          po_err_msg    out t_Err_Msg) is
    l_org_id  t_organizations.org_id%type;
    l_org_pid t_organizations.org_id%type;
    ex_acc_denied exception;
    us_tab  num_tab;
    us_tab0 num_tab;
    --l_count_rel number;
    ex_many_dogovor exception;
    l_check_dog    number;
    l_dog_class_id number;
  begin
    us_tab  := GET_USER_ORGS_TAB(pi_worker_id);
    us_tab0 := GET_USER_ORGS_TAB(pi_worker_id, 0);

    begin
      select ORL.ORG_ID, ORL.ORG_PID, d.dog_class_id
        into l_org_id, l_org_pid, l_dog_class_id
        from T_DOGOVOR D, T_ORG_RELATIONS ORL
       where D.DOG_ID = pi_dogovor_id
         and D.ORG_REL_ID = ORL.ID
         and ORL.ORG_PID in (select * from TABLE(us_tab))
         and ORL.ORG_ID in (select * from TABLE(us_tab0));
    exception
      when no_data_found then
        raise ex_acc_denied;
    end;

    if pi_block = 0 then

      select count(*)
        into l_check_dog
        from t_org_relations r
        join t_dogovor d
          on r.id = d.org_rel_id
       where r.org_id = l_org_id
         and r.org_pid = l_org_pid
         and nvl(d.is_enabled, 0) = 1
         and d.dog_class_id = l_dog_class_id;
      if l_check_dog != 0 and l_dog_class_id not in (1, 8, 9) then
        po_err_num := 1;
        po_err_msg := 'Невозможно выполнить операцию. Организация имеет незаблокированный договор с данным классом договора.';
        return;
      end if;

    end if;

    -- checking access for operation for specified user
    if (not Security_pkg.Check_Rights_str('EISSD.ORGS.DOGOVOR.BLOCK/UNBLOCK',
                                          l_org_pid,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) then
      return;
    end if;

    -- 01.06.2010 №17192 begin
    -- Сохраняем старую версию в таблицу t_dogovor_hst
    insert into t_dogovor_hst
      (dog_id,
       dog_date,
       dog_number,
       org_rel_id,
       is_vis_auto,
       is_enabled,
       premia_schema,
       with_nds,
       v_lice,
       na_osnovanii,
       conn_type,
       codes,
       dog_class_id,
       hst_date,
       hst_user)
      select d.dog_id,
             d.dog_date,
             d.dog_number,
             d.org_rel_id,
             d.is_vis_auto,
             d.is_enabled,
             d.premia_schema,
             d.with_nds,
             d.v_lice,
             d.na_osnovanii,
             d.conn_type,
             d.codes,
             d.dog_class_id,
             sysdate,
             pi_worker_id
        from t_dogovor d
       where d.dog_id = pi_dogovor_id;
    -- 01.06.2010 №17192 end

    update T_DOGOVOR D
       set D.IS_ENABLED =
           (1 - pi_block)
     where D.DOG_ID = pi_dogovor_id;
    if pi_block = 1 then
      update t_organizations t
         set t.is_enabled =
             (1 - pi_block)
       where t.org_id in (select tt.org_id
                            from t_org_relations tt
                           where tt.org_pid = l_org_id
                             and tt.org_reltype = '1001')
         and t.org_id in (select tt.org_id
                            from t_org_relations tt
                           where tt.org_pid in
                                 (select r.org_id
                                    from t_dic_region r
                                    join t_dic_mrf m
                                      on m.id = r.mrf_id
                                   where m.org_id = l_org_pid)
                             and tt.org_reltype = '1006');
    end if;

    -- Пересчитаем таблицу T_ORG_RELTYPE_DOWN
    recreateorgreltypedown_for_org(l_org_id, null);
  exception
    when ex_acc_denied then
      po_err_num := 1001;
      po_err_msg := 'Ошибка. Указаный договор не найден в области видимости пользователя.';
    when ex_many_dogovor then
      po_err_num := 1002;
      po_err_msg := 'Невозможно выполнить операцию. Организация имеет незаблокированный договор с другим подразделением';
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
  end Block_Dogovor;
  ------------------------------------------------------------------------
  procedure Sync_Child_Org_To_Asr(pi_parent_id in T_ORGANIZATIONS.ORG_ID%type,
                                  pi_child_id  in T_ORGANIZATIONS.ORG_ID%type) is
    sql_exp              varchar2(2000);
    l_asr_id             number;
    l_remote_ids         num_tab := num_tab();
    l_cc_ids             num_tab := num_tab();
    l_remote_ids_already num_tab := num_tab();
    l_cc_par_ids         num_tab := num_tab();
  begin
    -- достаем внешние идентификаторы для дочерней организации из таблицы синхронизации
    sql_exp := 'select deal.cc_id bulk collect into :l_remote_ids_already
                  from dealer_eq@asrcomv deal
                  where deal.eissd_dealer_id = :pi_child_id
                  order by deal.cc_id asc';
    execute immediate sql_exp
      using out l_remote_ids_already, pi_child_id;
    commit;
    -- достаем расчетные центры родительской организации из таблицы синхронизации
    sql_exp := 'select deal.cc_id bulk collect into :l_cc_par_ids
                  from dealer_eq@asrcomv deal
                  where deal.eissd_dealer_id = :pi_parent_id and
                        not deal.remote_dealer_id in (select * from TABLE(:l_remote_ids_already))
                  order by deal.cc_id asc';
    execute immediate sql_exp
      using out l_cc_par_ids, pi_parent_id, l_remote_ids_already;
    commit;

    for item in (select *
                   from t_asr a
                  where a.asr_id = 2
                  order by a.asr_id asc) loop
      -- берем АСР
      l_asr_id := item.asr_id;

      -- берем список расчетных центров дочерней организации по которым мы хотим
      -- провести синхронизацию
      select cc.cc_remote_id bulk collect
        into l_cc_ids
        from t_org_calc_center occ, t_calc_center cc, t_asr_region ar
       where occ.org_id = pi_child_id
         and occ.cc_id = cc.cc_id
         and cc.cc_region_id = ar.region_id
         and ar.asr_id = l_asr_id
         and cc.cc_remote_id in (select * from TABLE(l_cc_par_ids))
       order by cc.cc_remote_id asc;

      -- берем список внешних идентификаторов дилера по списку расчетных центров
      sql_exp := 'select deal.remote_dealer_id bulk collect into :l_remote_ids
                    from dealer_eq@asrcomv deal
                    where deal.eissd_dealer_id = :pi_parent_id and
                          deal.cc_id in (select * from TABLE(:l_cc_ids))
                    order by deal.cc_id asc';
      execute immediate sql_exp
        using out l_remote_ids, pi_parent_id, l_cc_ids;
      commit;
      -- собственно синхронизируем информацию по дочерней организации
    --ASR_PROTOCOL_OUT.Sync_Dealer_ID(l_asr_id,pi_child_id,l_cc_ids);
    end loop;
  exception
    when others then
      logging_pkg.error(SQLCODE || ' # ' || sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        c_package || 'Sync_Child_Org_To_Asr');
  end Sync_Child_Org_To_Asr;
  ------------------------------------------------------------------------
  procedure Sync_CC_To_Asr(pi_org_id in T_ORGANIZATIONS.ORG_ID%type,
                           pi_cc_was in num_tab,
                           pi_cc_now in num_tab) is
    l_asr_id number;
    cc_tab   num_tab := num_tab();
  begin
    l_asr_id := 2; -- заглушка
    select cc.cc_remote_id bulk collect
      into cc_tab
      from t_calc_center cc, t_dic_region r
     where cc.cc_id in (select * from TABLE(pi_cc_now))
       and not cc.cc_id in (select * from TABLE(pi_cc_was))
       and cc.cc_region_id = r.reg_id;

    ASR_PROTOCOL_OUT.Sync_Dealer_ID(l_asr_id, pi_org_id, cc_tab);

  exception
    when others then
      null;
  end Sync_CC_To_Asr;
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
  begin
    return security_pkg.Get_User_Orgs_Tab_With_Param1(pi_worker_id,
                                                      pi_org_id,
                                                      pi_self_include,
                                                      pi_childrens_include,
                                                      pi_curated_include,
                                                      pi_curated_sub_include,
                                                      pi_curators_include,
                                                      pi_tm_1009_include);
  end Get_User_Orgs_Tab_With_Param1;
  ------------------------------------------------------------------------
  function Get_Orgs1(pi_worker_id           in T_USERS.USR_ID%type, -- пользователь
                     pi_org_id              in T_ORGANIZATIONS.ORG_ID%type := null, -- от конкретной организации
                     pi_parents_include     in pls_integer := 0, -- включать ли родителей найденных организаций
                     pi_self_include        in pls_integer := 1, -- включать pi_org_id
                     pi_childrens_include   in pls_integer := 1, -- включать детей
                     pi_curated_include     in pls_integer := 1, -- включать курируемых
                     pi_curated_sub_include in pls_integer := 1, -- включать курируемых субдиллерами
                     pi_curators_include    in pls_integer := 0, -- включать кураторов
                     pi_incl_only_act_dog   in pls_integer := 1) -- включать только активные договоры
   return num_tab is
    -- Функция возвращающая массив организаций видимых пользователю
    res        num_tab := null;
    l_table_00 num_tab := num_tab();
    l_table_0  num_tab := num_tab();
    l_table_1  num_tab := num_tab();
    l_table_01 num_tab := num_tab();
    l_table_02 num_tab := num_tab();
  begin

    -- Организации, видимые пользователю
    l_table_0 := Get_User_Orgs_Tab(pi_worker_id, pi_incl_only_act_dog);
    -- Организации с параметрами, начиная с pi_org_id или для pi_org_id is null
    if (pi_org_id is null) then
      l_table_1 := Get_User_Orgs_Tab_With_Param1(pi_worker_id,
                                                 null,
                                                 pi_self_include,
                                                 pi_childrens_include,
                                                 pi_curated_include,
                                                 pi_curated_sub_include);
    else
      l_table_1 := Get_User_Orgs_Tab_With_Param1(null,
                                                 pi_org_id,
                                                 pi_self_include,
                                                 pi_childrens_include,
                                                 pi_curated_include,
                                                 pi_curated_sub_include);
    end if;

    -- Пересечение множеств организации, которые видны пользователю и связаны с указанной организацией
    select distinct ORG_ID bulk collect
      into l_table_01
      from (select /*+ PRECOMPUTE_SUBQUERY */
             column_value ORG_ID
              from TABLE(l_table_0)
            intersect
            select /*+ PRECOMPUTE_SUBQUERY */
             column_value ORG_ID
              from TABLE(l_table_1));

    -- Организации, которые связаны с l_table_01 отношениями типа подчиненности (родители для l_table_01)
    if (pi_parents_include = 1) then
      select distinct ORH.ORG_PID bulk collect
        into l_table_00
        from mv_org_tree ORH
       where (ORH.ORG_ID in (select /*+ PRECOMPUTE_SUBQUERY */
                              *
                               from TABLE(l_table_01)))
         and ORH.ORG_RELTYPE = 1001;
    end if;
    -- Организации, которые связаны с l_table_01 и l_table_00 отношениями типа подчиненности (кураторы для l_table_01 и l_table_00)
    if (pi_curators_include = 1) then
      select distinct ORH.Root_Org_Pid bulk collect
        into l_table_02
        from mv_org_tree ORH
       where (ORH.ORG_ID in (select /*+ PRECOMPUTE_SUBQUERY */
                              *
                               from TABLE(l_table_01)
                             union all
                             select /*+ PRECOMPUTE_SUBQUERY */
                              *
                               from TABLE(l_table_00)))
         and (ORH.ROOT_RELTYPE in (1002, 1004, 1007, 999));
    end if;
    -- Результат
    select distinct ORG_ID bulk collect
      into res
      from (select /*+ PRECOMPUTE_SUBQUERY */
             column_value ORG_ID
              from TABLE(l_table_01)
            union
            select /*+ PRECOMPUTE_SUBQUERY */
             column_value ORG_ID
              from TABLE(l_table_00)
            union
            select /*+ PRECOMPUTE_SUBQUERY */
             column_value ORG_ID
              from TABLE(l_table_02));
    return res;
  end Get_Orgs1;
  ---------------------------------------------------------------------------
  -- возвращает № договора и дату для данного дилера и курирующей организации
  function Get_Dog_By_Org(pi_org_id           in number,
                          pi_org_pid          in number := null,
                          pi_dogovor_class_id in num_tab,
                          pi_worker_id        in number,
                          po_err_num          out pls_integer,
                          po_err_msg          out varchar2)
    return sys_refcursor is
    cur               sys_refcursor;
    l_Org_Tab         num_tab := num_tab(pi_org_pid);
    l_Org_Id          number;
    l_Org_pId         number;
    l_count_dog_class number;
    ex_raise exception;
    l_tab1 num_tab;
    l_tab2 num_tab;
  begin
    logging_pkg.debug('pi_org_id=' || pi_org_id || ' pi_org_pid=' ||
                      pi_org_pid || ' pi_dogovor_class_id=' ||
                      get_str_by_num_tab(pi_dogovor_class_id),
                      'Get_Dog_By_Org');
    select count(*) into l_count_dog_class from table(pi_dogovor_class_id);
    if l_count_dog_class = 0 then
      raise ex_raise;
    end if;
    if pi_org_pid is not null then
      -- Проверяю порядок организаций. Если наоборот -- переварачиваю.
      Begin
        Select Count(*)
          Into l_Org_Id
          from (Select tor.*
                  from (Select vot.org_id, vot.org_pid
                          from t_org_relations vot
                        Connect By Prior vot.Org_pId = vot.Org_id
                         Start with vot.Org_Id = pi_org_id) tor
                Connect By Prior tor.Org_Id = Tor.Org_pid
                 Start with Tor.Org_pId = pi_org_pid);
        If l_Org_Id > 0 then
          -- Порядок верен
          l_Org_Id  := pi_org_id;
          l_Org_pId := pi_org_pid;
        Else
          -- организации переданы наоборот
          l_Org_Id  := pi_org_pid;
          l_Org_pId := pi_org_id;
        End If;
      Exception
        when Others then
          l_Org_Id  := pi_org_id;
          l_Org_pId := pi_org_pid;
      End;
      ---------------------------------------------------------
      -- Если не передан куратор -- получаю возможных кураторов
      ---------------------------------------------------------
      begin
        --insert into tmp(tmp) values (pi_org_pid);
        Select t.org_pid bulk collect
          into l_Org_Tab
          from (Select tor.org_pid, tor.id
                  from t_org_relations tor
                Connect by prior tor.org_pid = tor.org_id
                 start with tor.org_id = l_Org_Id
                        and NVL(l_Org_pId, 0) = 0
                        and tor.org_pid >= 0) t
          join t_dogovor dog
            on dog.org_rel_id = t.id;
      exception
        when others then
          l_Org_Tab := num_tab(l_Org_pId);
      end;
      --  возвращает договора между 2-мя организациями,
      --  между которыми только одна связь курирования
      if l_Org_pId is not null and l_Org_Id is not null then
        select tor.org_id bulk collect
          into l_tab1
          from mv_org_tree tor
        connect by prior tor.org_pid = tor.org_id
         start with tor.org_id in (l_Org_pId);

      select tor.org_pid bulk collect
        into l_tab2
        from t_org_relations tor
      connect by prior tor.org_pid = tor.org_id
             and tor.org_reltype = '1001'
       start with tor.org_id in (l_Org_Id)
              and tor.org_reltype = '1001';

        open cur for
          select distinct tab1.*
            from (select dog.dog_id,
                         dog.dog_date,
                         dog.dog_number,
                         dog.dog_class_id class_dog,
                         dog.is_enabled
                    from t_org_relations tor
                    join t_organizations o
                      on o.org_id = tor.org_id
                    left join T_ORG_REL_DOG r
                      on r.org_rel_id = tor.id
                    left join t_dogovor dog
                      on dog.org_rel_id = tor.id
                      or r.dog_id = dog.dog_id
                  connect by prior tor.org_pid = tor.org_id
                   start with tor.org_id in (l_Org_Id)) tab1
            join (select dog.dog_id,
                         dog.dog_date,
                         dog.dog_number,
                         dog.dog_class_id class_dog,
                         dog.is_enabled
                    from t_org_relations tor
                    join t_organizations o
                      on o.org_id = tor.org_id
                    left join T_ORG_REL_DOG r
                      on r.org_rel_id = tor.id
                    left join t_dogovor dog
                      on dog.org_rel_id = tor.id
                      or r.dog_id = dog.dog_id
                  connect by prior tor.org_pid = tor.org_id
                   start with tor.org_pid in (select * from table(l_tab1))
                             /* (select tor.org_id
                                 from mv_org_tree tor
                               connect by prior tor.org_pid = tor.org_id
                                start with tor.org_id in (l_Org_pId))*/
                   /* Start with tor.Org_pId = l_Org_pId*/) tab2
              on tab1.dog_id = tab2.dog_id
           where tab1.class_dog in
                 (select * from TABLE(pi_dogovor_class_id))
             and l_Org_pId not in (select * from table(l_tab2))
                 /*(select tor.org_pid
                    from t_org_relations tor
                  connect by prior tor.org_pid = tor.org_id
                         and tor.org_reltype = '1001'
                   start with tor.org_id in (l_Org_Id)
                          and tor.org_reltype = '1001')*/;
      else
        open cur for
          Select distinct dog.dog_id,
                          dog.dog_date,
                          dog.dog_number,
                          dog.dog_class_id class_dog,
                          dog.is_enabled
            from (Select *
                    from (Select tor.org_pid,
                                 tor.org_id,
                                 tor.id,
                                 sys_Connect_By_Path(tor.org_reltype, '|') scPath
                            from t_org_relations tor
                          Connect By tor.Org_Id = Prior tor.Org_Pid
                           Start with tor.Org_Id = l_Org_Id) tor
                   Where (Length(scPath) - 4) <=
                         Length(replace(scPath, '1004'))
                      or (Length(scPath) - 4) <=
                         Length(replace(scPath, '1006'))
                      or instr(scPath, '999') > 0
                  Connect By Prior tor.Org_Id = tor.Org_Pid
                   Start with tor.Org_pId = l_Org_pId
                          and l_Org_pId > 0
                           or tor.Org_pId in
                              (Select * from table(l_Org_Tab))) tor
            left join t_org_rel_Dog ord
              on ord.org_rel_id = tor.id
            join t_dogovor dog
              on dog.org_rel_id = tor.id
              or dog.dog_id = ord.dog_id
          -- join t_dic_dogovor_class cl on cl.id=dog.dog_class_id
           where dog.dog_class_id in
                 (select * from TABLE(pi_dogovor_class_id));
      end if;
    else
      open cur for
        select distinct d.dog_id,
                        d.dog_date,
                        d.dog_number,
                        d.dog_class_id class_dog,
                        d.is_enabled
          from mv_org_tree tor
          join t_dogovor d
            on d.org_rel_id in (tor.id, tor.root_rel_id)
         where tor.org_id = pi_org_id
           and d.is_enabled = 1
           and d.dog_class_id in (select * from TABLE(pi_dogovor_class_id));
    end if;
    return cur;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Dog_By_Org;

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
    return sys_refcursor is
    cur               sys_refcursor;
    l_Org_Id          number;
    l_Org_pId         number;
    l_count_dog_class number;
    ex_raise exception;
  begin
    logging_pkg.debug('pi_org_id=' || pi_org_id || ' pi_org_pid=' ||
                      pi_org_pid || ' pi_dogovor_class_id=' ||
                      get_str_by_num_tab(pi_dogovor_class_id),
                      'Get_Dog_By_Org_for_tmc_op');
    select count(*) into l_count_dog_class from table(pi_dogovor_class_id);
    if l_count_dog_class = 0 then
      raise ex_raise;
    end if;
    -- Проверяю порядок организаций. Если наоборот -- переварачиваю.
    Begin
      Select Count(*)
        Into l_Org_Id
        from (Select tor.*
                from (Select vot.org_id, vot.org_pid
                        from t_org_relations vot
                      Connect By Prior vot.Org_pId = vot.Org_id
                       Start with vot.Org_Id = pi_org_id) tor
              Connect By Prior tor.Org_Id = Tor.Org_pid
               Start with Tor.Org_pId = pi_org_pid);
      If l_Org_Id > 0 then
        -- Порядок верен
        l_Org_Id  := pi_org_id;
        l_Org_pId := pi_org_pid;
      Else
        -- организации переданы наоборот
        l_Org_Id  := pi_org_pid;
        l_Org_pId := pi_org_id;
      End If;
    Exception
      when Others then
        l_Org_Id  := pi_org_id;
        l_Org_pId := pi_org_pid;
    End;

    --  возвращает договора между 2-мя организациями,
    --  между которыми только одна связь курирования
    open cur for
      select distinct tab1.*
        from (select dog.dog_id,
                     dog.dog_date,
                     dog.dog_number,
                     dog.dog_class_id class_dog,
                     dog.is_enabled
                from t_org_relations tor
                join t_organizations o
                  on o.org_id = tor.org_id
                left join T_ORG_REL_DOG r
                  on r.org_rel_id = tor.id
                left join t_dogovor dog
                  on dog.org_rel_id = tor.id
                  or r.dog_id = dog.dog_id
              connect by prior tor.org_pid = tor.org_id
               start with tor.org_id in (l_Org_Id)) tab1
        join (select dog.dog_id,
                     dog.dog_date,
                     dog.dog_number,
                     dog.dog_class_id class_dog,
                     dog.is_enabled
                from t_org_relations tor
                join t_organizations o
                  on o.org_id = tor.org_id
                left join T_ORG_REL_DOG r
                  on r.org_rel_id = tor.id
                left join t_dogovor dog
                  on dog.org_rel_id = tor.id
                  or r.dog_id = dog.dog_id
              connect by prior tor.org_pid = tor.org_id
               Start with tor.Org_pId = l_Org_pId) tab2
          on tab1.dog_id = tab2.dog_id
       where tab1.class_dog in (select * from TABLE(pi_dogovor_class_id))
         and l_Org_pId not in
             (select tor.org_pid
                from t_org_relations tor
              connect by prior tor.org_pid = tor.org_id
                     and tor.org_reltype = '1001'
               start with tor.org_id in (l_Org_Id)
                      and tor.org_reltype = '1001');

    return cur;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Dog_By_Org_for_tmc_op;

  ----------------------------------------------------------------------------
  -- 59657 Добавлен параметр pi_ignore_blocked_org
  ----------------------------------------------------------------------------
  function Get_List_User_Podcluch(pi_org_id    t_organizations.org_id%type,
                                  pi_right_id  in t_rights.right_string_id%type,
                                  pi_worker_id in T_USERS.USR_ID%type,
                                  po_err_num   out pls_integer,
                                  po_err_msg   out t_Err_Msg)
    return sys_refcursor is
    res        sys_refcursor;
    l_right_id number;
  begin
    begin
      select r.right_id
        into l_right_id
        from t_rights r
       where r.right_string_id = pi_right_id;
    exception
      when no_data_found then
        po_err_num := sqlcode;
        po_err_msg := 'Право не найдено: ' || pi_right_id;
        return null;
    end;

    open res for
      select distinct tuo.usr_id        USER_ID,
                      person_lastname   P_SURNAME,
                      person_firstname  P_NAME,
                      person_middlename P_SECONDNAME,
                      tuo.org_id        ORG_ID,
                      org_name          ORG_NAME,
                      tu.is_enabled
        from t_user_org TUO, t_users TU, t_person TP, t_organizations ORG
       where tuo.usr_id = tu.usr_id
         and tu.usr_person_id = tp.person_id
         and tuo.org_id = org.org_id
         and tuo.org_id = pi_org_id
         and security_pkg.Check_Rights_Number(l_right_id,
                                              tuo.org_id,
                                              tuo.usr_id) = 1
         and org.is_enabled = 1
         and tu.is_enabled = 1
         and org.org_id in
             (Select * from TABLE(get_user_orgs_tab(pi_worker_id, 1)))

       order by person_lastname, person_firstname, person_middlename;
    return res;
  exception
    when others then
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      po_err_num := sqlcode;
      return null;
  end Get_List_User_Podcluch;

  ---------------------------------------------------------------------------
  --- Проверяет, можно ли создать договор между указанными организациями
  ---------------------------------------------------------------------------
  function Is_Dog_Permitted(pi_org_pid   in T_ORGANIZATIONS.ORG_ID%type,
                            pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                            pi_worker_id in T_USERS.USR_ID%type,
                            pi_list_prm  in num_tab, -- список разрешений для договора (мб null)
                            po_err_num   out pls_integer,
                            po_err_msg   out varchar2) return number is
    /* 1 => можно, 0 => нельзя /**/
    l_in_rel      t_org_relations.org_reltype%type;
    l_is_1004_rel t_org_relations.org_reltype%type; -- есть ли на пути связь 1004
    l_is_path     number;
    l_org         number;
    l_check       number;
  begin
    --    savepoint save_point;
    select count(*)
      into l_check
      from t_org_relations t
      join t_org_is_rtmob r1
        on r1.org_id = t.org_id
      join t_org_is_rtmob r2
        on r2.org_id = t.org_pid
     where t.org_pid = pi_org_pid
       and t.org_id = pi_org_id
       and r1.is_org_rtm = 0
       and r2.is_org_rtm = 1;
    -- Если курируемая организация - подразделение принципала, выдаём ошибку
    if is_org_usi(pi_org_id) = 1 and l_check = 0 then
      raise ex_usi; -- Нельзя заключать договра с подразделениями принципала
    end if;
    -- Если курируемая организация не является подразделением принципала и
    -- то проверяем курирующую на наличие входящей связи 1004
    if is_org_usi(pi_org_pid) = 0 then
      select max(tor.org_reltype)
        into l_in_rel
        from t_org_relations tor
       where tor.org_id = pi_org_pid
         and tor.org_pid <> -1
         and tor.org_reltype in (1004, 999);

      if l_in_rel is null then
        raise ex_wrong_rel_dog; -- входящая связь у организации (org_pid) д.б. 1004
      end if;
    end if;

    -- проверяем права на создание договора
    if (not Security_pkg.Check_Rights_str('EISSD.ORGS.DOGOVOR.DEALER.REGISTER',
                                          pi_org_pid,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg)) or
       (Check_Mask_Orgs_Dog(pi_org_pid, pi_list_prm, po_err_num, po_err_msg) = 0) then
      --return null;
      raise ex_not_enough_dog_rights;
    end if;

    -- выдаёт список родителей и типы связей
    Select max(t.org_reltype)
      into l_is_1004_rel
      from t_org_relations t
    Connect By t.org_id = prior t.org_pid
     Start with t.org_id = pi_org_id;
    -- если нет связи 1004 org_id с родителями, то всё ok
    if l_is_1004_rel not in (1004, 999) then
      return 1;
    end if;

    -- проверяем наличие связи между организациями
    select count(*)
      into l_is_path
      from (select distinct *
              from (Select tor.org_id, tor.org_pid, tor.org_reltype
                      from t_org_relations tor
                    Connect By tor.org_id = Prior tor.org_pid
                     Start with tor.org_id = pi_org_Id
                            and NVL(org_id, 0) > 0) T
            Connect By Prior t.org_id = t.org_pid
             Start with t.org_pid = pi_org_pId
                    and NVL(t.org_pid, 0) > 0);

    if l_is_path <> 0 then
      select distinct max(t.org_reltype)
        into l_is_1004_rel
        from (Select tor.org_id, tor.org_pid, tor.org_reltype
                from t_org_relations tor
              Connect By tor.org_id = Prior tor.org_pid
               Start with tor.org_id = pi_org_Id
                      and NVL(org_id, 0) > 0) T
       where t.org_reltype in (1004, 999)
      Connect By Prior t.org_id = t.org_pid
       Start with t.org_pid = pi_org_pId
              and NVL(t.org_pid, 0) > 0;

      if l_is_1004_rel is null then
        raise ex_rel_not_found_dog; -- между организациями нет ни одной связи 1004
      end if;

      if l_is_path > 1 then
        return 1; -- можно создать договор, но надо добавить связь между организациями
      end if;
    else
      Select Count(*)
        into l_org
        from (select distinct rel.org_id, CONNECT_BY_ISCYCLE cbi
                from t_org_relations rel
              connect by nocycle prior rel.org_id = rel.org_pid
               start with rel.org_id = 0)
       Where cbi > 0;

      if l_org > 0 /*is not null*/ /*= pi_org_id*/
       then
        raise ex_cycle;
      end if;

      select count(*)
        into l_is_path
        from (select distinct *
                from (Select tor.org_id, tor.org_pid, tor.org_reltype
                        from t_org_relations tor
                      Connect By tor.org_id = Prior tor.org_pid
                       Start with tor.org_id = pi_org_pId
                              and NVL(org_id, 0) > 0) T
              Connect By Prior t.org_id = t.org_pid
               Start with t.org_pid = pi_org_Id
                      and NVL(t.org_pid, 0) > 0);

      if l_is_path = 0 then
        return 1; -- можно создать договор, но надо добавить связь между организациями
      else
        -- если l_is_path <>0, неверный порядок организаций
        raise ex_wrong_order;
      end if;

    end if;
    return 1;
  exception
    when ex_wrong_rel_dog then
      po_err_num := 1;
      po_err_msg := 'У курирующей организации отсутствует входящая связь курирования!';
      --      rollback to save_point;
      return 0;
    when ex_usi then
      po_err_num := 2;
      po_err_msg := 'Нельзя заключать договоры с подразделениями принципала!';
      --      rollback to save_point;
      return 0;
    when ex_rel_not_found_dog then
      po_err_num := 3;
      po_err_msg := 'Между заданными организациями нет ни одой связи курирования!';
      --      rollback to save_point;
      return 0;
    when ex_cycle then
      po_err_num := 4;
      po_err_msg := 'Создание договора между заданными организациями приведёт к возникновению цикла!';
      --      rollback to save_point;
      return 0;
    when ex_not_enough_dog_rights then
      po_err_num := 5;
      po_err_msg := 'У организации недостаточно прав для создания договора!';
      --      rollback to save_point;
      return 0;
    when ex_wrong_order then
      po_err_num := sqlcode;
      po_err_msg := 'Организации заданы в неверном порядке!';
      return 0;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      --      rollback to save_point;
      return 0;
  end Is_Dog_Permitted;
  ------------------------------------------------------------------------
  -- проверяет (по маскам), может ли договор быть заключён у организации
  function Check_Mask_Orgs_Dog(pi_org_id   in number,
                               pi_list_prm in num_tab, -- список разрешений для договора (мб null)
                               po_err_num  out pls_integer,
                               po_err_msg  out t_Err_Msg) return number is

    l_dogs_rights num_tab;
  begin
    -- ** lau (множ. договора) beg **
    -- проверяем достаточно ли прав у организации для
    -- регистрации договора с заданным набором услуг

    select distinct r.pr_right_id bulk collect
      into l_dogs_rights
      from t_perm_rights r
      join table(pi_list_prm) t
        on t.column_value = r.pr_prm_id;

    if (security_pkg.check_has_all_right(l_dogs_rights,
                                         security_pkg.get_orgs_right(pi_org_id)) = 0) then
      raise ex_not_enough_dog_rights;
    end if;

    return 1;
  exception
    when ex_not_enough_dog_rights then
      po_err_num := 1;
      po_err_msg := 'У организации недостаточно прав для создания (изменения) договора с заданным списком прав.';
      return 0;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      rollback to save_point;
      return 0;
  end;
  -----------------------------------------------
  --возвращает 2 организации по id договора
  function Get_Orgs_By_DogId(pi_dog_id    in t_dogovor.dog_id%type,
                             pi_worker_id in number) return sys_refcursor is

    res    sys_refcursor;
    us_tab num_tab;
  begin
    us_tab := GET_USER_ORGS_TAB(pi_worker_id);
    open res for
      select tor.org_id,
             tor.org_pid,
             org.org_name,
             org_p.org_name org_name_Parent
        from t_dogovor td
        Join mv_org_Tree tor
          on tor.id = td.org_rel_id
        Join t_Organizations org
          on org.Org_Id = tor.org_id
        Join t_Organizations org_p
          on org_p.Org_Id = tor.org_pid
       where td.dog_id = pi_dog_id
         and tor.org_id in (select * from TABLE(us_tab));
    return res;
  end;
  ------------------------------------------------------------------------
  --возвращает 2 организации по id договора
  ------------------------------------------------------------------------
  function Get_Orgs_By_DogId(pi_dog_id in t_dogovor.dog_id%type)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select tor.org_id,
             tor.org_pid,
             org.org_name,
             org_p.org_name org_name_Parent
        from t_dogovor       td,
             t_org_relations tor,
             t_Organizations org,
             t_Organizations org_p
       where tor.id = td.org_rel_id
         and org.org_id = tor.org_id
         and org_p.org_id = tor.org_pid
         and td.dog_id = pi_dog_id;
    return res;
  end;
  ---------------------------------------------------------------------------
  --- Проверяет, можно ли изменить  договор между указанными организациями
  ---------------------------------------------------------------------------
  function Is_Dog_Permitted_Change(pi_org_pid   in T_ORGANIZATIONS.ORG_ID%type,
                                   pi_org_id    in T_ORGANIZATIONS.ORG_ID%type,
                                   pi_worker_id in T_USERS.USR_ID%type,
                                   pi_list_prm  in num_tab,
                                   po_err_num   out pls_integer,
                                   po_err_msg   out t_Err_Msg) -- список разрешений для договора (мб null)
   return number is
    /* 1 => можно, 0 => нельзя /**/
  begin
    -- Проверяем, существует ли договор между организациями
    if Get_Dog_By_Org(pi_org_id,
                      pi_org_pid,
                      null,
                      pi_worker_id,
                      po_err_num,
                      po_err_msg) is null then
      raise ex_acc_denied;
    end if;

    -- проверяем права на Редактирование договора с организацией (5209)
    if (not Security_pkg.Check_Rights_str('EISSD.ORGS.DOGOVOR.EDIT',
                                          pi_org_pid,
                                          pi_worker_id,
                                          po_err_num,
                                          po_err_msg,
                                          true)) or
       (Check_Mask_Orgs_Dog(pi_org_pid, pi_list_prm, po_err_num, po_err_msg) = 0) then
      return 0;
    else
      return 1;
    end if;
  exception
    when ex_acc_denied then
      po_err_num := 1;
      po_err_msg := 'Ошибка. Указаный договор не найден в области видимости пользователя.';
      return 0;
  end Is_Dog_Permitted_Change;
  ------------------------------------------------------------------------
  -- Выдает список направлений использования ТМЦ по указанному договору
  function Get_Tmc_Move_Serv_By_Dog(pi_dog_id    in t_dogovor.dog_id%type,
                                    pi_worker_id in t_users.usr_id%type,
                                    po_err_num   out pls_integer,
                                    po_err_msg   out t_Err_Msg)
    return sys_refcursor is
    l_cur     sys_refcursor;
    l_res     sys_refcursor;
    l_prm_tab num_tab;
  begin
    logging_pkg.debug('pi_dog_id=' || pi_dog_id || ' pi_worker_id=' ||
                      pi_worker_id,
                      'Get_Tmc_Move_Serv_By_Dog');

    open l_res for
      select distinct p.prm_id tmc_serv_id, Substr(p.prm_name, 27) TMC_SERV_NAME
        from t_dogovor_prm dp
        join t_perm_to_perm tt
          on tt.perm_dog = dp.dp_prm_id
        join t_perm p
          on p.prm_id = tt.direction
       where dp.dp_dog_id = pi_dog_id
         and dp.dp_is_enabled = 1
       order by 1;
    return l_res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Tmc_Move_Serv_By_Dog;
  ------------------------------------------------------------------------
  -- Определяет, является ли организация оператором (принципал)
  function Is_Org_Operator(pi_org_id    in t_organizations.org_id%type,
                           pi_worker_id in t_users.usr_id%type,
                           po_err_num   out pls_integer,
                           po_err_msg   out t_Err_Msg) return number is
    l_res number := 0;
  begin
    if (is_org_usi(pi_org_id) > 0) then
      l_res := 1;
    end if;
    return l_res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return l_res;
  end Is_Org_Operator;
  -------------------------------------------------------------------------
  -- Возвращает oorg_id и org_pid по id договора
  procedure Get_Orgs_By_Dog(pi_dog_id  in OUT t_dogovor.dog_id%type,
                            po_org_id  in out number,
                            po_org_pid in out number) is
  begin
    If pi_dog_id is not Null then
      select tor.org_id, tor.org_pid
        into po_org_id, po_org_pid
        from t_org_relations tor, t_dogovor td
       where td.org_rel_id = tor.id
         and td.dog_id = pi_dog_id;
    eLSE
      Select Max(dog.dog_id)
        Into pi_dog_id
        from t_org_relations tor
        join t_dogovor dog
          on dog.org_rel_id = tor.id
       Where tor.org_id = po_org_id
         and tor.org_pid = po_org_pid;
    End If;
  exception
    when others then
      null;
  end Get_Orgs_By_Dog;
  ----------------------------------------------------------------------
  -- возвращает ИД договора для конкретной организации
  ----------------------------------------------------------------------
  procedure Get_DogId_By_Org(pi_org_id    in T_DOGOVOR.DOG_ID%type,
                             pi_worker_id in T_USERS.USR_ID%type,
                             po_dog_id    out t_dogovor.dog_id%type) is
    l_user_org_tab num_tab;
  begin
    l_user_org_tab := GET_USER_ORGS_TAB(pi_worker_id);
    select min(D.DOG_ID)
      into po_dog_id
      from T_DOGOVOR D, T_ORG_RELATIONS ORL, T_ORGANIZATIONS O
     where O.ORG_ID = pi_org_id
       and O.ROOT_ORG_ID = ORL.ORG_ID
       and ORL.ID = D.ORG_REL_ID(+)
       and ORL.ORG_RELTYPE in (1002, 1004, 999)
       and ORL.ORG_PID in (select *
                             from TABLE(l_user_org_tab)
                           union all
                           select -1 from dual)
       and ORL.ORG_ID in (select * from TABLE(l_user_org_tab));
  end Get_DogId_By_Org;
  ----------------------------------------------------------------------
  -- проверяет, есть ли у организации право на заключение договоров
  -- 0 => нет, 1 => да
  function Is_Make_Dog_Perm(pi_org_id in number) return number is
    l_err_num pls_integer;
    l_err_msg t_Err_Msg;
    res       number;
  begin
    res := security.Is_Perm_In_Org(pi_org_id, 1150, l_err_num, l_err_msg);
    return res;
  end Is_Make_Dog_Perm;
  ----------------------------------------------------------------------
  -- выдаёт список прав организации по ид организации
  function Get_Rights_By_org(pi_org_id in number) return sys_refcursor is

    res sys_refcursor;
  begin

    open res for
      select tp.prm_name perm
        from t_perm tp
       where tp.prm_type = 8500
         and tp.prm_id in
             (select r.pr_prm_id
                from t_perm_rights r
                join table(security_pkg.get_orgs_right(pi_org_id)) t
                  on t.column_value = r.pr_right_id);
    return res;
  end Get_Rights_By_org;
  ----------------------------------------------------------------------
  -- Выдаёт список коммутаторов, доступных для заданной организации
  function Get_List_Comm(pi_org_id  in number,
                         po_err_num out pls_integer,
                         po_err_msg out t_Err_Msg) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select distinct ccc.id_comm, tc.name_comm
        from t_organizations org
        join t_org_calc_center occ
          on occ.org_id = org.org_id
        join t_calc_center cc
          on cc.cc_id = occ.cc_id
        join t_commut_cc ccc
          on ccc.id_cc = cc.cc_id
        join t_commutators tc
          on tc.id_comm = ccc.id_comm
       where org.org_id = pi_org_id
       order by ccc.id_comm asc;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_List_Comm;
  --------------------------------------------------------------------------
  --  Возвращает дерево организаций доступных пользователю.
  --  Возвращает только первый уровень.
  -- Параметры -----------------------------
  -- pi_RootOrgs - узлы деревьев, у которых нужно вернуть детей
  --добавлена 1008 связь (задача 68638 )
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

   return sys_refcursor is
    c_pr_name constant varchar2(65) := c_package ||
                                       'Get_Orgs_Tree_By_Level';
    res            sys_refcursor;
    l_org_type     num_tab := pi_org_type;
    l_dop_org_type num_tab := pi_dop_org_type;
    l_RootOrgs     num_tab := pi_RootOrgs;
    user_orgs      num_tab;
    l_ssc_orgs     num_tab := num_tab();
    --l_deliv_orgs   num_tab := num_tab();
    l_show_ssc     number := 1;
    l_show_deliv   number := 1;
    l_show_others  number := 1;
    l_show_1002    number := 1;
    l_show_1003    number := 1;
    l_show_1004    number := 1;
    l_show_1007    number := 1;
    l_show_1008    number := 1;
    l_org_dop_type num_tab := pi_dop_org_type;
    /****************************************/
    function getStrParam return varchar2 is
    begin
      return substr('pi_RootOrgs=' || get_str_by_num_tab(pi_RootOrgs) || ';' ||
                    'pi_org_type=' || get_str_by_num_tab(pi_org_type) || ';' ||
                    'pi_dop_org_type=' ||
                    get_str_by_num_tab(pi_dop_org_type) || ';' ||
                    'pi_block_org=' || pi_block_org || ';' ||
                    'pi_block_dog=' || pi_block_dog || ';' ||
                    'pi_right_id=' || pi_right_id || ';' ||
                    'pi_include_Parent=' || pi_include_Parent || ';' ||
                    'pi_branch=' || pi_branch || ';' || 'pi_show_rtk=' ||
                    pi_show_rtk || ';' || 'pi_worker_id=' || pi_worker_id,
                    1,
                    2000);
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    if (pi_org_type is null or pi_org_type.count = 0) then
      l_org_type := num_tab(1001, 1002, 1003, 1004, 1006, 1007, 1008, 999);
    end if;
    if (pi_dop_org_type is null or pi_dop_org_type.count = 0) then
      l_dop_org_type := l_org_type;
    end if;

    if pi_dop_org_type is null or pi_dop_org_type.count = 0 then
      l_org_dop_type := l_org_type;
    end if;

    if l_dop_org_type is not null and l_dop_org_type.count > 0 then
      select sum(case
                   when column_value = 999 then
                    1
                   else
                    0
                 end),
             sum(case
                   when column_value = 998 then
                    1
                   else
                    0
                 end),
             sum(case
                   when column_value not in (999, 998) then
                    1
                   else
                    0
                 end),
             sum(case
                   when column_value in (1004) then
                    1
                   else
                    0
                 end),
             sum(case
                   when column_value in (1002) then
                    1
                   else
                    0
                 end),
             sum(case
                   when column_value in (1003) then
                    1
                   else
                    0
                 end),
             sum(case
                   when column_value in (1007) then
                    1
                   else
                    0
                 end),
             sum(case
                   when column_value in (1008) then
                    1
                   else
                    0
                 end)
        into l_show_ssc,
             l_show_deliv,
             l_show_others,
             l_show_1004,
             l_show_1002,
             l_show_1003,
             l_show_1007,
             l_show_1008
        from table(l_org_dop_type);
    end if;
    -- Вычитка всех организаций, которые должны быть видны пользователю.
    -- 1) Вычитываем все организации по иерархии сверху вниз и снизу вверх от организаций, на которые у пользователя есть права.
    with orgs_tree as
     (SELECT tor.org_id
        FROM t_org_relations tor
        join T_ORG_IS_RTMOB rt
          ON rt.ORG_ID = tor.ORG_ID
        left join T_ORG_IS_RTMOB rt_p
          ON rt_p.ORG_ID = tor.ORG_PID
       where tor.org_reltype <> '1009'
         and nvl(rt.is_org_rtm, 0) =
             nvl(rt_p.is_org_rtm, nvl(rt.is_org_rtm, 0))
      connect by prior tor.org_id = tor.org_pid
       start with tor.org_id in
                  (select uo.org_id
                     from t_user_org uo
                    where uo.usr_id = pi_worker_id)
              and tor.org_reltype <> '1009'
      UNION ALL
      SELECT tor.org_id
        FROM t_org_relations tor
       where tor.org_reltype <> '1009'
      connect by tor.org_id = prior tor.org_pid
       start with tor.org_id in
                  (select uo.org_id
                     from t_user_org uo
                    where uo.usr_id = pi_worker_id)
              and tor.org_reltype <> '1009')
    -- 4) Соберём все вместе.
    select * bulk collect into user_orgs from (select * from orgs_tree);

    -- 83754. Если надо отображать ЦПО ли доставку - найдём все организации, которые содержат ЦПО ниже по иерархии
    if l_show_ssc = 1 or l_show_deliv = 1 THEN
      SELECT distinct org_id bulk collect
        INTO l_ssc_orgs
        from (SELECT org.org_id
                FROM T_ORGANIZATIONS org
                JOIN T_ORG_RELATIONS rel
                  ON rel.ORG_ID = org.ORG_ID
                LEFT JOIN T_ORG_RELTYPE_DOWN d
                  ON d.ORG_ID = org.ORG_ID
               WHERE ((l_show_ssc = 1 and
                     (d.ssc = 1 OR org.IS_SS_CENTER = 1)) or
                     (l_show_deliv = 1 and d.delivery = 1))
                 AND rel.ORG_PID IN (SELECT * FROM TABLE(l_RootOrgs))
              UNION ALL
              SELECT ro.*
                FROM TABLE(l_RootOrgs) ro
                join T_ORG_RELTYPE_DOWN rd
                  on ro.column_value = rd.org_id
               WHERE (pi_include_Parent = 1 or
                     rd.org_id in (select * from table(user_orgs)))
                 and (l_show_others >= 1 or
                     (l_show_deliv = 1 and rd.delivery = 1) or
                     (l_show_ssc = 1 and rd.SSC = 1)));
    END IF;

    open res for
      select distinct ORG_ID,
                      ORG_PID,
                      ORG_NAME,
                      MAX(org_reltype) org_reltype,
                      MAX(H_DOG) H_DOG,
                      H_SUB,
                      H_ANY_CHILD,
                      CC_NAMES,
                      REGION_ID,
                      TIP,
                      LVL,
                      KL_REGION,
                      IS_ENABLED,
                      IS_ORG_RTMOB,
                      IS_ORG_FILIAL,
                      MIN(RN) as rn_min,
                      root_reltype
        from (select t.org_id,
                     t.org_pid,
                     o.org_name,
                     t.org_reltype org_reltype,
                     nvl2(dog.dog_id, 1, 0) h_dog,
                     (SELECT COUNT(1)
                        FROM T_ORG_RELTYPE_DOWN n
                       WHERE n.ORG_ID = t.org_id
                         and 1001 in (n.reltype, n.root_reltype)
                         AND n.IS_SELF = 0
                         AND rownum < 2) h_sub,
                     (SELECT COUNT(1)
                        FROM T_ORG_RELTYPE_DOWN n
                       WHERE n.ORG_ID = o.org_id
                         AND ((l_show_1002 = 1 and
                             1002 in (n.reltype, n.root_reltype)) or
                             (l_show_1003 = 1 and
                             1003 in (n.reltype, n.root_reltype)) or
                             (l_show_ssc = 1 and
                             999 in (n.reltype, n.root_reltype)) or
                             (l_show_1004 = 1 and
                             1004 in (n.reltype, n.root_reltype)) or
                             (l_show_1007 = 1 and
                             1007 in (n.reltype, n.root_reltype)) or
                             (l_show_1008 = 1 and
                             (1009 in (n.reltype, n.root_reltype) or
                             1008 in (n.reltype, n.root_reltype))) or
                             (l_show_ssc = 1 AND n.ssc = 1) or
                             (l_show_deliv = 1 AND n.delivery = 1))
                         AND (pi_block_org = 1 OR n.IS_ENABLED = 1)
                         AND (pi_block_dog = 1 OR n.DOG_ENABLED = 1)
                         and (l_show_others >= 1 or
                             (n.delivery = 1 AND l_show_deliv = 1) or
                             (n.SSC = 1 AND l_show_ssc = 1))
                         AND (pi_branch in (2, 3) OR
                             (pi_branch = 0 and nvl(n.is_org_rtm, 0) = 0) OR
                             (pi_branch = 1 and nvl(n.is_org_rtm, 1) = 1))
                         AND n.IS_SELF = 0
                         AND rownum < 2) as h_any_child,
                     case
                     -- Заглушка для МРФ ЮГ
                       when cc_count.org_id = 2004855 then
                        null
                       when cc_count.org_id is not null then
                        orgs.concat_cc_names(t.org_id)
                       else
                        null
                     end cc_names,
                     o.region_id,
                     decode(t.org_pid, -1, 0, 1) TIP, -- тип связи - курирование
                     0 lvl,
                     reg.kl_region,
                     o.is_enabled,
                     rt.is_org_rtm is_org_rtmob,
                     is_org_filial(t.org_id) is_org_filial,
                     1 rn, -- По данному столбцу сортируются выходные данные, чтобы первой шла Организация-родитель.
                     decode(nvl(rrs.root_reltype, t.root_reltype),
                            999,
                            999,
                            null) root_reltype
                from mv_org_tree t
                join t_organizations o
                  on o.org_id = t.org_id
                left join t_organizations o_p
                  on o_p.org_id = t.org_pid
                left join t_dogovor dog
                  on dog.org_rel_id = t.id
                left join (select occ.org_id, count(occ.cc_id)
                            from t_org_calc_center OCC
                           group by occ.org_id
                          having count(occ.cc_id) = 1) cc_count
                  on cc_count.org_id = t.org_id
                left join t_dic_region reg
                  on reg.reg_id = o.region_id
                join t_org_is_rtmob rt
                  on rt.org_id = t.org_id
                left join t_org_is_rtmob oir_pid
                  on oir_pid.org_id = t.org_pid
              --для рыжего цвета РРС
                left join mv_org_tree rrs
                  on rrs.org_id = t.org_id
                 and t.root_reltype = 1006
                 and rrs.root_reltype = 999
               where t.org_pid in (select * from table(l_RootOrgs))
                 and (t.org_reltype != 1009)
                 and ( /*l_check = 0 or*/
                      (l_show_others >= 1 and
                      t.org_id in
                      (select td.org_id
                          from t_org_reltype_down td
                         where td.delivery = 0
                           and td.ssc = 0
                           and (((l_show_1002 = 1 and
                               1002 in (td.reltype, td.root_reltype)) or
                               (l_show_1003 = 1 and
                               1003 in (td.reltype, td.root_reltype)) or
                               (l_show_ssc = 1 and
                               999 in (td.reltype, td.root_reltype)) or
                               (l_show_1004 = 1 and
                               1004 in (td.reltype, td.root_reltype)) or
                               (l_show_1007 = 1 and
                               1007 in (td.reltype, td.root_reltype)) or
                               (l_show_1008 = 1 and
                               (1009 in (td.reltype, td.root_reltype) or
                               1008 in (td.reltype, td.root_reltype)))) and
                               ((pi_block_org = 1 OR td.IS_ENABLED = 1) AND
                               (pi_block_dog = 1 OR td.DOG_ENABLED = 1) AND
                               (pi_branch in (2, 3) OR
                               (pi_branch = 0 and nvl(td.is_org_rtm, 0) = 0) OR
                               (pi_branch = 1 and nvl(td.is_org_rtm, 1) = 1))))) and
                      o.is_ss_center = 0) or
                      (l_show_ssc = 1 AND
                      t.org_id in (select * from table(l_ssc_orgs))) or
                      (l_show_deliv = 1 AND
                      t.org_id in (select * from table(l_ssc_orgs))))
                 and t.org_id in (select * from table(user_orgs))
                 and ((pi_show_rtk = 1 or
                     (pi_branch in (2, 3) and
                     (nvl(rt.is_org_rtm, 1) = 1 or
                     (nvl(rt.is_org_rtm, 0) = nvl(oir_pid.is_org_rtm, 0)) or
                     reg_id is not null))) or
                     (pi_branch = 0 and nvl(rt.is_org_rtm, 0) = 0 and
                     nvl(oir_pid.is_org_rtm, 0) = 0) or
                     (pi_branch = 1 and nvl(rt.is_org_rtm, 1) = 1))
                 and ((pi_block_org = 0 and NVL(o.is_enabled, 0) <> 0) or
                     pi_block_org = 1)
                 and ((pi_block_org = 0 and NVL(o_p.is_enabled, 0) <> 0) or
                     t.org_pid = -1 or pi_block_org = 1)
                 and ((pi_block_Dog = 0 and NVL(dog.is_enabled, 1) <> 0) or
                     pi_block_Dog = 1)
                 and (pi_branch <> 3 or is_org_rrs(t.org_id) = 0)
              union
              select t.org_id,
                     max(t.org_pid) org_pid,
                     o.org_name,
                     min(t.org_reltype) org_reltype,
                     min(nvl2(dog.dog_id, 1, 0)) h_dog,
                     (SELECT COUNT(1)
                        FROM T_ORG_RELTYPE_DOWN n
                       WHERE n.ORG_ID = t.org_id
                         and 1001 in (n.reltype, n.root_reltype)
                         AND n.IS_SELF = 0
                         AND rownum < 2) h_sub,
                     (SELECT COUNT(1)
                        FROM T_ORG_RELTYPE_DOWN n
                       WHERE n.ORG_ID = t.org_id
                         and ((l_show_1002 = 1 and
                             1002 in (n.reltype, n.root_reltype)) or
                             (l_show_1003 = 1 and
                             1003 in (n.reltype, n.root_reltype)) or
                             (l_show_1004 = 1 and
                             1004 in (n.reltype, n.root_reltype)) or
                             (l_show_1007 = 1 and
                             1007 in (n.reltype, n.root_reltype)) or
                             (l_show_ssc = 1 and
                             999 in (n.reltype, n.root_reltype)) or
                             (l_show_1008 = 1 and
                             (1009 in (n.reltype, n.root_reltype) or
                             1008 in (n.reltype, n.root_reltype))) or
                             (l_show_ssc = 1 AND n.ssc = 1) or
                             (l_show_deliv = 1 AND n.delivery = 1))
                         AND (pi_block_org = 1 OR n.IS_ENABLED = 1)
                         AND (pi_block_dog = 1 OR n.DOG_ENABLED = 1)
                         and (l_show_others >= 1 or
                             (n.delivery = 1 AND l_show_deliv = 1) or
                             (n.SSC = 1 AND l_show_ssc = 1))
                         AND (pi_branch in (2, 3) OR
                             (pi_branch = 0 and nvl(n.is_org_rtm, 0) = 0) OR
                             (pi_branch = 1 and nvl(n.is_org_rtm, 1) = 1))
                         AND n.IS_SELF = 0
                         AND rownum < 2) as h_any_child,
                     max(case
                         -- Заглушка для МРФ ЮГ
                           when cc_count.org_id = 2004855 then
                            null
                           when cc_count.org_id is not null then
                            orgs.concat_cc_names(t.org_id)
                           else
                            null
                         end) cc_names,
                     o.region_id,
                     max(decode(t.org_pid, -1, 0, 1)) TIP, -- тип связи - курирование
                     0 lvl,
                     reg.kl_region,
                     o.is_enabled,
                     max(rt.is_org_rtm) is_org_rtmob,
                     is_org_filial(t.org_id) is_org_filial,
                     0 rn,
                     decode(nvl(rrs.root_reltype, t.root_reltype),
                            999,
                            999,
                            null) root_reltype
                from mv_org_tree t
                join t_organizations o
                  on o.org_id = t.org_id
                left join t_dogovor dog
                  on dog.org_rel_id = t.id
                left join (select occ.org_id, count(occ.cc_id)
                             from t_org_calc_center OCC
                            group by occ.org_id
                           having count(occ.cc_id) = 1) cc_count
                  on cc_count.org_id = t.org_id
                left join t_dic_region reg
                  on reg.reg_id = o.region_id
                join t_org_is_rtmob rt
                  on rt.org_id = t.org_id
                left join t_org_is_rtmob oir_pid
                  on oir_pid.org_id = t.org_pid
              --для рыжего цвета РРС
                left join mv_org_tree rrs
                  on rrs.org_id = t.org_id
                 and t.root_reltype = 1006
                 and rrs.root_reltype = 999
               where t.org_id in (select * from table(l_RootOrgs))
                 and (t.org_reltype != 1009)
                 and ( /*l_check = 0 or*/
                      (l_show_others >= 1 and
                      t.org_id in
                      (select td.org_id
                          from t_org_reltype_down td
                         where td.delivery = 0
                           and td.ssc = 0
                           and (((l_show_1002 = 1 and
                               1002 in (td.reltype, td.root_reltype)) or
                               (l_show_1003 = 1 and
                               1003 in (td.reltype, td.root_reltype)) or
                               (l_show_ssc = 1 and
                               999 in (td.reltype, td.root_reltype)) or
                               (l_show_1004 = 1 and
                               1004 in (td.reltype, td.root_reltype)) or
                               (l_show_1007 = 1 and
                               1007 in (td.reltype, td.root_reltype)) or
                               (l_show_1008 = 1 and
                               (1009 in (td.reltype, td.root_reltype) or
                               1008 in (td.reltype, td.root_reltype)))) and
                               ((pi_block_org = 1 OR td.IS_ENABLED = 1) AND
                               (pi_block_dog = 1 OR td.DOG_ENABLED = 1) AND
                               (pi_branch in (2, 3) OR
                               (pi_branch = 0 and nvl(td.is_org_rtm, 0) = 0) OR
                               (pi_branch = 1 and nvl(td.is_org_rtm, 1) = 1))))) and
                      o.is_ss_center = 0) or
                      (l_show_ssc = 1 AND
                      t.org_id in (select * from table(l_ssc_orgs))) or
                      (l_show_deliv = 1 AND
                      t.org_id in (select * from table(l_ssc_orgs))))
                 and (pi_include_Parent = 1 or
                     t.org_id in (select * from table(user_orgs)))
                 and ((pi_show_rtk = 1 or
                     (pi_branch in (2, 3) and
                     (nvl(rt.is_org_rtm, 1) = 1 or
                     (nvl(rt.is_org_rtm, 0) = nvl(oir_pid.is_org_rtm, 0)) or
                     reg_id is not null))) or
                     (pi_branch = 0 and nvl(rt.is_org_rtm, 0) = 0 and
                     nvl(oir_pid.is_org_rtm, 0) = 0) or
                     (pi_branch = 1 and nvl(rt.is_org_rtm, 1) = 1))
                 and ((pi_block_org = 0 and NVL(o.is_enabled, 0) <> 0) or
                     pi_block_org = 1)
                 and ((pi_block_Dog = 0 and NVL(dog.is_enabled, 1) <> 0) or
                     pi_block_Dog = 1)
                 and (pi_branch <> 3 or is_org_rrs(t.org_id) = 0)
               group by t.org_id,
                        o.org_name,
                        o.region_id,
                        o.is_enabled,
                        reg.kl_region,
                        decode(nvl(rrs.root_reltype, t.root_reltype),
                               999,
                               999,
                               null))
       GROUP BY ORG_ID,
                ORG_PID,
                ORG_NAME,
                H_SUB,
                H_ANY_CHILD,
                CC_NAMES,
                REGION_ID,
                TIP,
                LVL,
                KL_REGION,
                IS_ENABLED,
                IS_ORG_RTMOB,
                IS_ORG_FILIAL,
                root_reltype
       order by rn_min, org_reltype, org_name;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(getStrParam || po_err_num || po_err_msg, c_pr_name);
      return null;
  end;
  -----------------------------------------------------------------------------
  -- Возвращает ID ФЭСа по ID региона.
  -----------------------------------------------------------------------------
  Function Get_FES_Bi_Id_Region(Pi_Region_Id in Number,
                                pi_is_rtm    number,
                                po_err_num   out pls_integer,
                                po_err_msg   out t_Err_Msg) return Number is
    res Number;
  Begin
    if pi_is_rtm = 0 then
      --- Для  перми возвращаем другую орг-цию
      If Pi_Region_Id = 3 then
        res := 2001825;
        return res;
      End If;
      If Pi_Region_Id = -1 then
        res := 0;
        return res;
      end if;
      select t.org_id
        into res
        from t_dic_region t
       where t.reg_id = Pi_Region_Id;
    else
      select t.rtmob_org_id
        into res
        from t_dic_region_info t
       where t.reg_id = Pi_Region_Id;
    end if;

    Return res;
  Exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return Null;
  End Get_FES_Bi_Id_Region;
  -----------------------------------------------------------------------------
  -- ** ЗАДЧА №11710 **
  -- Добавление/редактирование доверенности по договору
  procedure Add_Warrant(pi_dog_id   in number,
                        pi_org_id   in number,
                        pi_org_list in num_tab, -- список организаций, которым выдаётся доверенность
                        po_err_num  out pls_integer,
                        po_err_msg  out t_Err_Msg) is

    l_new_rel_id number;
  begin
    -- ** 1 ** удаляем доверенности, которых нет
    delete from t_org_rel_dog t
     where t.dog_id = pi_dog_id
       and t.org_rel_id not in
           (select tor.id
              from t_org_relations tor
             where tor.org_id = pi_org_id
               and tor.org_reltype = c_rel_tp_warrant
               and tor.org_pid in
                   (select column_value from table(pi_org_list)));

    delete from t_org_relations tor
     where tor.org_id = pi_org_id
       and tor.org_reltype = c_rel_tp_warrant
       and tor.org_pid not in (select column_value from table(pi_org_list));

    -- ** 2 ** добавляем доверенности, которых нет
    for warrec in (select column_value org_id
                     from table(pi_org_list)
                    where column_value not in
                          (select tor.org_pid
                             from t_org_relations tor
                           --
                             join t_org_rel_dog ord
                               on ord.org_rel_id = tor.id
                           --
                            where tor.org_id = pi_org_id
                              and tor.org_reltype = c_rel_tp_warrant
                                 --
                              and ord.dog_id = pi_dog_id)) loop
      begin
        select nvl(tor.id, 0)
          into l_new_rel_id
          from t_org_relations tor
         where tor.org_id = pi_org_id
           and tor.org_pid = warrec.org_id
           and tor.org_reltype = c_rel_tp_warrant;

      exception
        when no_data_found then
          l_new_rel_id := Add_Org_Relation(pi_org_id,
                                           warrec.org_id,
                                           c_rel_tp_warrant);
      end;

      insert into t_org_rel_dog t
        (org_id, org_pid, dog_id, org_rel_id)
      values
        (pi_org_id, warrec.org_id, pi_dog_id, l_new_rel_id);
      --end if;
    end loop;
    --Fix_Org_Tree;
    --return 1;
  Exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      --return 0;
  end Add_Warrant;
  -----------------------------------------------------------------------------
  -- ** ЗАДЧА №11710 **
  -- возвращает список организаций, у которых есть доверенность по указанному договору
  function Get_Warrant_By_DogId(pi_dog_id  in number,
                                po_err_num out pls_integer,
                                po_err_msg out t_Err_Msg)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select tor.org_pid
        from t_org_rel_dog t
        join t_org_relations tor
          on tor.id = t.org_rel_id
       where t.dog_id = pi_dog_id
         and tor.org_reltype = c_rel_tp_warrant;
    return res;
  Exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_Warrant_By_DogId;
  -------------------------------------------------
  -- по БИК банка возвращает список его отделений
  function Get_Bank_Department(pi_bik in t_bank_department.bik_bank%type)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select t.id, t.name
        from t_bank_department t
       where t.bik_bank = pi_bik;
    return res;
  exception
    when others then
      return null;
  end Get_Bank_Department;
  -------------------------------------------------
  -- добавляет отделение, если его не существовало (pi_id_dep = null),
  -- обновляет название, если оно было изменено, либо ничего не делает=)
  procedure Add_Bank_Department(pi_bik      in t_bank_department.bik_bank%type,
                                pi_id_dep   in t_bank_department.id%type,
                                pi_name_dep in t_bank_department.name%type) is
    l_count number;
  begin

    if pi_id_dep is null then
      -- проверяем, существует ли такое отделение
      select count(*)
        into l_count
        from t_bank_department t
       where t.bik_bank = pi_bik
         and lower(trim(t.name)) = lower(trim(pi_name_dep));
      -- добавляем отделение
      if l_count = 0 then
        insert into t_bank_department
          (name, bik_bank)
        values
          (pi_name_dep, pi_bik);
      end if;
    else
      -- проверяем, существует ли такое отделение
      select count(*)
        into l_count
        from t_bank_department t
       where t.bik_bank = pi_bik
         and lower(trim(t.name)) = lower(trim(pi_name_dep));

      if l_count = 0 then
        -- обновляем название отделения
        update t_bank_department t
           set t.name = pi_name_dep
         where t.id = pi_id_dep;
      end if;
    end if;
  end Add_Bank_Department;
  ------------------------------------

  function get_org_dog_by_usl(pi_usl_tab   in num_tab,
                              pi_org_id    in number,
                              pi_worker_id in number) return sys_refcursor as
    res sys_refcursor;
  begin
    open res for
      select d.dog_id, d.dog_number, d.codes
        from t_dogovor d, t_org_relations tor, t_dogovor_prm dp
       where tor.id = d.org_rel_id
         and d.dog_id = dp.dp_dog_id
         and tor.org_id = pi_org_id
         and dp.dp_prm_id in (select * from table(pi_usl_tab));
    return res;
  end;

  --------------------------------------------------------------------------
  -- Сбрасывание организации по умолчанию (26706)
  -- (в случае блокировки организации)
  --------------------------------------------------------------------------
  procedure Del_user_org(pi_org_id in t_organizations.org_id%type) is
    l_region     t_dic_region.reg_id%type;
    l_org_region t_organizations.org_id%type;
    l_err_num    number;
    l_err_msg    varchar2(2000);
  begin
    -- Удаляем из t_user_org
    delete from t_user_org uo where uo.org_id = pi_org_id;

    select o.region_id
      into l_region
      from t_organizations o
     where o.org_id = pi_org_id;

    if is_org_rtmob(pi_org_id) = 0 then
      l_org_region := Get_FES_Bi_Id_Region(l_region,
                                           0,
                                           l_err_num,
                                           l_err_msg);
    else
      l_org_region := Get_FES_Bi_Id_Region(l_region,
                                           1,
                                           l_err_num,
                                           l_err_msg);
    end if;

    -- Проставляем для всех пользователей вместо старой организации ФЭС
    update t_sessions s
       set s.cache_date = sysdate
     where s.id_worker in
           (select u.usr_id from t_users u where u.org_id = pi_org_id);

    update t_users u
       set u.org_id = l_org_region
     where u.org_id = pi_org_id;
  end Del_user_org;
  -----------------------------------------------------------------------
  -- Получение флага доверительные запросы
  -----------------------------------------------------------------------
  function get_dog_is_accept(pi_dog_id  in number,
                             po_err_num out pls_integer,
                             po_err_msg out t_Err_Msg) return number is
    l_is_accept number;
  begin
    select d.is_accept
      into l_is_accept
      from t_dogovor d
     where d.dog_id = pi_dog_id;
    return l_is_accept;
  exception
    when no_data_found then
      po_err_num := 1;
      po_err_msg := 'Договор ' || pi_dog_id || ' не найден';
      return null;
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM;
      return null;
  end;
  --------------------------------------------------------------
  -- Проверка наличия договора с сайтом-партнером
  --------------------------------------------------------------
  function get_partner_site_dog(pi_worker_id in number,
                                pi_dog_id    in number,
                                pi_check_sum in varchar2,
                                po_err_num   out number,
                                po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    -- проверка прав
    open res for
      select d.dog_id,
             d.is_enabled,
             dd.url,
             case
               when 6010 member of security_pkg.get_dog_rights(d.dog_id) then
                1
               else
                0
             end as site
        from t_dogovor d, t_dogovor_details dd
       where dd.dog_id = d.dog_id
         and (pi_dog_id is null or d.dog_id = pi_dog_id)
         and (pi_check_sum is null or dd.check_sum = pi_check_sum);
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  --------------------------------------------------------------
  -- Получение списка договоров (и огранизаций) по классу договора
  --------------------------------------------------------------
  function get_dogovors_by_class(pi_dog_class in t_dic_dogovor_class.id%type,
                                 po_err_num   out number,
                                 po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select d.dog_id, d.dog_number, org.org_id, org.org_name
        from t_dogovor d
        join t_org_relations rel
          on d.org_rel_id = rel.id
        join t_organizations org
          on org.org_id = rel.org_id
       where d.dog_class_id = pi_dog_class
         and d.is_enabled = 1;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;

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
      return orgs.Get_User_Orgs_Tab_With_Param1(pi_worker_id           => pi_worker_id,
                                                pi_org_id              => pi_org_id,
                                                pi_self_include        => pi_self_include,
                                                pi_childrens_include   => pi_childrens_include,
                                                pi_curated_include     => pi_curated_include,
                                                pi_curated_sub_include => pi_curated_sub_include,
                                                pi_curators_include    => pi_curators_include,
                                                pi_tm_1009_include     => pi_tm_1009_include);
    else
      if pi_tm_1009_include = 1 then
        l_rel_tab := num_tab(1001, 1002, 1004, 1006, 1007, 1008, 1009, 999);
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
        res := orgs.Get_User_Orgs_Tab_With_Param1(pi_worker_id           => pi_worker_id,
                                                  pi_org_id              => pi_org_id,
                                                  pi_self_include        => pi_self_include,
                                                  pi_childrens_include   => pi_childrens_include,
                                                  pi_curated_include     => pi_curated_include,
                                                  pi_curated_sub_include => pi_curated_sub_include,
                                                  pi_curators_include    => pi_curators_include,
                                                  pi_tm_1009_include     => pi_tm_1009_include);
      else
        for OrgRec in (select distinct tor.org_id
                         from t_org_relations tor
                         left join t_user_org tuo
                           on tuo.org_id = tor.org_id
                          and tuo.usr_id = pi_worker_id
                         left join t_roles_perm rp
                           on rp.rp_role_id = tuo.role_id
                         left join t_perm_rights pr
                           on pr.pr_prm_id = rp.rp_perm_id
                          and pr.pr_right_id = pi_right_id
                        where pr.pr_right_id is not null
                       connect by prior tor.org_id = tor.org_pid
                        start with tor.org_id = nvl(pi_org_id, 1)) loop
          tmp := orgs.Get_User_Orgs_Tab_With_Param1(pi_worker_id           => pi_worker_id,
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
    return res;
  end Get_User_Orgs_Tab_By_Right;
  -----------------------------------------------------------------------------------------
  --Получение типа точек продаж
  -----------------------------------------------------------------------------------------
  function Get_Dic_Org_Type(po_err_num out number, po_err_msg out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select t.id, t.name, t.short_name from t_dic_organization_type t;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  -----------------------------------------------------------------------------------------
  -- 51465 Получение МРФ по организации
  -----------------------------------------------------------------------------------------
  function get_mrf_by_org_id(pi_org_id  in t_organizations.org_id%type,
                             po_err_num out number,
                             po_err_msg out varchar2) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select distinct o.org_id,
                      o.org_name,
                      o.org_full_name,
                      o.adr2_id,
                      a.addr_index,
                      a.addr_country,
                      a.addr_city,
                      a.addr_street,
                      a.addr_building,
                      a.addr_office,
                      o.org_settl_account,
                      o.org_con_account,
                      o.org_kpp,
                      o.org_bik,
                      o.org_okpo,
                      o.org_okonx,
                      o.org_inn,
                      dm.bnk_name,
                      dm.bnk_town,
                      dm.id mrf_id,
                      p.person_lastname,
                      p.person_firstname,
                      p.person_middlename
        from mv_org_tree     t,
             t_organizations o,
             t_address       a,
             t_dic_mrf       dm,
             t_person        p
       where t.org_id in (select m.org_id from t_dic_mrf m)
         and t.org_id = o.org_id
         and o.adr2_id = a.addr_id
         and dm.org_id(+) = t.org_id
         and o.resp_id = p.person_id
      Connect by prior t.org_pid = t.org_id
       Start with t.org_id = pi_org_id;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end get_mrf_by_org_id;
  -----------------------------------------------------------------------------------------
  -- 51465 получение по региону реквизитов
  -----------------------------------------------------------------------------------------
  function Get_region_details(pi_region_id in number,
                              po_err_num   out number,
                              po_err_msg   out varchar2) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select r.reg_id
        from t_dic_region r
        join t_organizations org
          on org.org_id = r.org_id
        join t_dic_mrf dm
          on dm.id = r.mrf_id
       where r.reg_id = pi_region_id;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;

  ---------------------------------------------------------------------------------------
  -- Загрузка печати и подписи для региона с веба
  ---------------------------------------------------------------------------------------
  procedure Add_Document_By_Region(pi_region_id    in t_region_documents.reg_id%type,
                                   pi_document     in t_region_documents.document%type,
                                   PI_RTM_DOCUMENT in number,
                                   pi_worker_id    in t_users.usr_id%type,
                                   po_err_num      out number,
                                   po_err_msg      out varchar2) is
    NO_RIGHT_EXC EXCEPTION;
    l_org_tab num_tab := num_tab();

    l_region_id number;
    l_count     number;
  begin
    if (not Security_pkg.Check_User_Right_str('EISSD.STAMP/SIGN.DOWNLOAD',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      RAISE NO_RIGHT_EXC;
    end if;

    -- Проверяем, есть ли у пользователя организации УСИ
    select count(*)
      into l_count
      from t_user_org t
     where t.usr_id = pi_worker_id
       and is_org_usi(t.org_id) = 1
     order by 1;

    if l_count = 0 then
      RAISE NO_RIGHT_EXC;
    end if;

    -- Достаем организации пользователя (УСИ)
    select t.org_id bulk collect
      into l_org_tab
      from t_user_org t
     where t.usr_id = pi_worker_id
       and is_org_usi(t.org_id) = 1
     order by 1;

    for i in (select column_value from table(l_org_tab) t) loop
      if i.column_value = 0 or is_worker_developer(pi_worker_id) = 1 then
        -- РТК или разработчик
        --костылим
        update t_region_documents t
           set t.document     = decode(PI_RTM_DOCUMENT,
                                       0,
                                       pi_document,
                                       t.document),
               t.RTM_DOCUMENT = decode(PI_RTM_DOCUMENT,
                                       1,
                                       pi_document,
                                       t.RTM_DOCUMENT)
         where t.reg_id = pi_region_id;
        return;
      else
        -- МРФ или филиал
        select o.region_id
          into l_region_id
          from t_organizations o
         where o.org_id = i.column_value;
        if l_region_id = -1 then
          -- МРФ
          -- Достаем все регионы данного МРФ и сравниваем с нужным
          select count(*)
            into l_count
            from t_organizations o
           where o.org_id in
                 (select t.org_id
                    from mv_org_tree t
                   where t.org_pid = i.column_value)
             and o.region_id <> -1
             and o.region_id = pi_region_id;
          if l_count = 0 then
            RAISE NO_RIGHT_EXC;
          else
            update t_region_documents t
               set t.document     = decode(PI_RTM_DOCUMENT,
                                           0,
                                           pi_document,
                                           t.document),
                   t.RTM_DOCUMENT = decode(PI_RTM_DOCUMENT,
                                           1,
                                           pi_document,
                                           t.RTM_DOCUMENT)
             where t.reg_id = pi_region_id;
            return;
          end if;
        else
          -- Филиал
          if l_region_id <> pi_region_id then
            RAISE NO_RIGHT_EXC;
          else
            update t_region_documents t
               set t.document     = decode(PI_RTM_DOCUMENT,
                                           0,
                                           pi_document,
                                           t.document),
                   t.RTM_DOCUMENT = decode(PI_RTM_DOCUMENT,
                                           1,
                                           pi_document,
                                           t.RTM_DOCUMENT)
             where t.reg_id = pi_region_id;
            return;
          end if;
        end if;
      end if;
    end loop;

  exception
    WHEN NO_RIGHT_EXC THEN
      po_err_num := 1;
      po_err_msg := 'У пользователя отсутствуют права на выполнение операции.';
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
  end Add_Document_By_Region;

  -----------------------------------------------------------------------------------------
  -- Получение по региону МРФа
  -----------------------------------------------------------------------------------------
  function Get_mrf_by_region(pi_region_id in number,
                             po_err_num   out number,
                             po_err_msg   out varchar2) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select dm.id, dm.name_mrf
        from t_dic_region dr, t_dic_mrf dm
       where dm.id = dr.mrf_id
         and dr.reg_id = pi_region_id;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_mrf_by_region;
  ------------------------------------------------------------------------------------
  -- 50802 Получение ИД права по его строковому ИД
  ------------------------------------------------------------------------------------
  function get_right_id_by_str_id(pi_str_right_id in T_RIGHTS.RIGHT_STRING_ID%type,
                                  po_err_num      out pls_integer,
                                  po_err_msg      out t_Err_Msg)
    return number is
    res number;
  begin
    select t.right_id
      into res
      from t_rights t
     where t.right_string_id = pi_str_right_id;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return - 1;
  end get_right_id_by_str_id;
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

   return num_tab is
  begin
    return security_pkg.Get_User_Orgs_Tab_By_Right_str(pi_worker_id,
                                                       pi_str_right_id,
                                                       pi_org_id,
                                                       pi_self_include,
                                                       pi_childrens_include,
                                                       pi_curated_include,
                                                       pi_curated_sub_include,
                                                       pi_curators_include);
  end Get_User_Orgs_Tab_By_Right_str;
  ------------------------------------------------------------------------------------
  -- 55378 Определяем, является ли организация филиалом
  ------------------------------------------------------------------------------------
  function is_org_filial(pi_org_id in t_organizations.org_id%type)
    return number is
    res number;
  begin
    select count(*)
      into res
      from t_organizations o
     where (o.org_id in (select dr.org_id from t_dic_region dr) or
           o.org_id = 2001825 or
           o.org_id in
           (select dri.RTMOB_ORG_ID from t_dic_region_info dri))
       and o.org_id = pi_org_id;
    return res;
  end is_org_filial;
  ------------------------------------------------------------------------------------
  -- 57954 Определяем, является ли организация стартапом
  ------------------------------------------------------------------------------------
  function is_org_startup(pi_org_id in t_organizations.org_id%type)
    return number is
    --res number;
  begin
    /*select count(*)
     into res
     from t_organizations o
    where (o.region_id in (43, 57, 46) or
          o.org_id in (2004866, 2004876, 2004855))
      and o.org_id = pi_org_id;*/
    return 0;
  end is_org_startup;
  ------------------------------------------------------------------------------------
  -- 57857 Сохранение прав для абонки на организации
  ------------------------------------------------------------------------------------
  procedure save_ab_service_for_org(pi_org_id     in t_organizations.org_id%type,
                                    pi_ab_service in num_tab := num_tab(),
                                    pi_worker_id  in number,
                                    po_err_num    out pls_integer,
                                    po_err_msg    out t_Err_Msg) is
    ex_no_right exception;
    l_is_org_rtm number;
  begin

    -- Удаляем все права по абонке, которые были до этого на организации
    delete from t_org_ab_rights oar where oar.org_id = pi_org_id;

    --определим кому принадлежат организации
    select t.is_org_rtm
      into l_is_org_rtm
      from t_org_is_rtmob t
     where t.org_id in (pi_org_id);

    -- Удаляем у подчиненных организаций все права, которых нет в пришедшем списке
    delete from t_org_ab_rights oar
     where oar.org_id in
           (select tor.org_id
              from mv_org_tree tor
              join t_org_is_rtmob t
                on t.org_id = tor.org_id
             where nvl(t.is_org_rtm, l_is_org_rtm) = l_is_org_rtm
            connect by prior tor.org_id = tor.org_pid
             start with tor.org_id = pi_org_id)
       and oar.id_req not in
           (select column_value from table(pi_ab_service));

    -- Сохраняем новые данные
    insert into t_org_ab_rights
      select pi_org_id, column_value from table(pi_ab_service);

    -- Сохраняем в историю новые данные
    if pi_ab_service.count > 0 then
      insert into t_org_ab_rights_hst
        select SEQ_ORG_AB_RIGHTS_HST.nextval, pi_worker_id, sysdate, oar.*
          from t_org_ab_rights oar
         where oar.org_id = pi_org_id;
    else
      insert into t_org_ab_rights_hst
      values
        (SEQ_ORG_AB_RIGHTS_HST.nextval,
         pi_worker_id,
         sysdate,
         pi_org_id,
         null);
    end if;

  exception
    when ex_no_right then
      po_err_num := 1;
      po_err_msg := 'Пользователь не является сотрудником РТК!';
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
  end save_ab_service_for_org;
  ------------------------------------------------------------------------------------
  -- 57857 Получение прав для абонки на организации
  ------------------------------------------------------------------------------------
  function get_ab_service_for_org(pi_org_id    in t_organizations.org_id%type,
                                  pi_worker_id in number,
                                  po_err_num   out pls_integer,
                                  po_err_msg   out t_Err_Msg)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select distinct oar.id_req
        from t_org_ab_rights oar
       where oar.org_id = pi_org_id
       order by oar.id_req;
    return res;
  exception
    when others then
      po_err_num := SQLCODE;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end get_ab_service_for_org;
  ---------------------------------------------------------------
  -- 57857 Сохранение списка доверенных IP-адресов
  ---------------------------------------------------------------
  procedure save_trusted_ip(pi_ip_addresses in IP_ADDRESSE_TAB,
                            pi_org_id       in t_organizations.org_id%type,
                            pi_worker_id    in t_users.usr_id%type,
                            po_err_code     out number,
                            po_err_msg      out varchar2) is
  begin
    if (not Security_pkg.Check_Rights_str('EISSD.TRUSTED_IP.EDIT',
                                          pi_org_id,
                                          pi_worker_id,
                                          po_err_code,
                                          po_err_msg,
                                          true)) then
      return;
    end If;
    delete from t_ip_trusted t where t.org_id = pi_org_id;

    insert into t_ip_trusted
      (org_id,
       ip_address_from,
       ip_address_to,
       ip_from,
       ip_to,
       IP_ADDRESS_FROM_NUM,
       IP_ADDRESS_TO_NUM)
      select pi_org_id,
             ip_address_from,
             ip_address_to,
             ip_from,
             ip_to,
             TO_NUMBER(LPAD(SUBSTR(IP_ADDRESS_FROM,
                                   1,
                                   INSTR(IP_ADDRESS_FROM, '.', 1, 1) - 1),
                            3,
                            '0') ||
                       LPAD(SUBSTR(IP_ADDRESS_FROM,
                                   INSTR(IP_ADDRESS_FROM, '.', 1, 1) + 1,
                                   INSTR(IP_ADDRESS_FROM, '.', 1, 2) -
                                   INSTR(IP_ADDRESS_FROM, '.', 1, 1) - 1),
                            3,
                            '0') ||
                       LPAD(SUBSTR(IP_ADDRESS_FROM,
                                   INSTR(IP_ADDRESS_FROM, '.', 1, 2) + 1,
                                   INSTR(IP_ADDRESS_FROM, '.', 1, 3) -
                                   INSTR(IP_ADDRESS_FROM, '.', 1, 2) - 1),
                            3,
                            '0') ||
                       LPAD(SUBSTR(IP_ADDRESS_FROM,
                                   INSTR(IP_ADDRESS_FROM, '.', 1, 3) + 1),
                            3,
                            '0')),
             TO_NUMBER(LPAD(SUBSTR(IP_ADDRESS_TO,
                                   1,
                                   INSTR(IP_ADDRESS_TO, '.', 1, 1) - 1),
                            3,
                            '0') ||
                       LPAD(SUBSTR(IP_ADDRESS_TO,
                                   INSTR(IP_ADDRESS_TO, '.', 1, 1) + 1,
                                   INSTR(IP_ADDRESS_TO, '.', 1, 2) -
                                   INSTR(IP_ADDRESS_TO, '.', 1, 1) - 1),
                            3,
                            '0') ||
                       LPAD(SUBSTR(IP_ADDRESS_TO,
                                   INSTR(IP_ADDRESS_TO, '.', 1, 2) + 1,
                                   INSTR(IP_ADDRESS_TO, '.', 1, 3) -
                                   INSTR(IP_ADDRESS_TO, '.', 1, 2) - 1),
                            3,
                            '0') ||
                       LPAD(SUBSTR(IP_ADDRESS_TO,
                                   INSTR(IP_ADDRESS_TO, '.', 1, 3) + 1),
                            3,
                            '0'))

        from table(pi_ip_addresses);
  exception
    when others then
      po_err_code := -1;
      po_err_msg  := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
  end save_trusted_ip;
  ---------------------------------------------------------------
  -- 57857 Получение списка доверенных IP-адресов
  ---------------------------------------------------------------
  function get_trusted_ip(pi_org_id    in t_organizations.org_id%type,
                          pi_worker_id in t_users.usr_id%type,
                          po_err_code  out number,
                          po_err_msg   out varchar2) return sys_refcursor is
    res sys_refcursor;
  begin

    open res for
      select distinct t.org_id,
                      t.ip_address_from,
                      t.ip_address_to,
                      t.ip_from,
                      t.ip_to,
                      t.id
        from t_ip_trusted t
       where t.org_id = pi_org_id
       order by t.ip_address_from;
    return res;
  exception
    when others then
      po_err_code := -1;
      po_err_msg  := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end get_trusted_ip;

  ------------------------------------------------------------------------------------
  -- 55378 Определяем, является ли организация филиалом
  ------------------------------------------------------------------------------------
  function is_org_mrf_or_filial(pi_org_id   in t_organizations.org_id%type,
                                po_err_code out number,
                                po_err_msg  out varchar2) return number is
    res     number := 0;
    l_count number := 0;
  begin

    select count(*)
      into l_count
      from t_dic_mrf t
     where t.org_id = pi_org_id;

    if l_count > 0 then
      res := 1;
    end if;

    l_count := 0;

    select count(*)
      into l_count
      from t_organizations o
     where (o.org_id in (select dr.org_id from t_dic_region dr) or
           o.org_id = 2001825)
       and o.org_id = pi_org_id;

    if l_count > 0 then
      res := 2;
    end if;

    return res;
  exception
    when others then
      po_err_code := sqlcode;
      po_err_msg  := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end is_org_mrf_or_filial;

  ------------------------------------------------------------------------------------
  -- 61554 Возвращаем id и название головной организации
  ------------------------------------------------------------------------------------
  function Get_Head_Org_Name(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
  --return varchar2 is
   return sys_refcursor is
    res        sys_refcursor;
    l_org_name T_ORGANIZATIONS.ORG_NAME%type;
    l_org_id   number;
  begin

    select (case
             when is_org_usi(t.org_id) = 1 then
              t.org_id
             else
              (case
                when t.org_id = t.root_org_id and
                     is_org_usi(t.root_org_pid) <> 1 then
                 t.root_org_pid
                else
                 t.root_org_id
              end)
           end)
      into l_org_id
      from mv_org_tree t
     where t.org_id = pi_org_id
       and org_reltype not in (1005, 1006)
       and rownum = 1;

    select o.org_name
      into l_org_name
      from t_organizations o
     where o.org_id = pi_org_id;

    open res for
      select org_id, org_name, l_org_name org_cur_name
        from t_organizations
       where org_id = l_org_id;

    return res;

  exception
    when others then
      open res for
        select org_id, org_name, l_org_name org_cur_name
          from t_organizations
         where 1 = 2;
      return res;
  end Get_Head_Org_Name;

  ------------------------------------------------------------------------------------
  -- 61554 Возвращаем название головной организации
  ------------------------------------------------------------------------------------
  function Get_Head_Org_Name2(pi_org_id in T_ORGANIZATIONS.ORG_ID%type)
    return varchar2 is
    l_org_id   number;
    l_org_name T_ORGANIZATIONS.ORG_NAME%type;
  begin

    select (case
             when is_org_usi(t.org_id) = 1 then
              t.org_id
             else
              (case
                when t.org_id = t.root_org_id and
                     is_org_usi(t.root_org_pid) <> 1 then
                 t.root_org_pid
                else
                 t.root_org_id
              end)
           end)
      into l_org_id
      from mv_org_tree t
     where t.org_id = pi_org_id
       and org_reltype not in (1005, 1006)
       and rownum = 1;

    select org_name
      into l_org_name
      from t_organizations
     where org_id = l_org_id;

    return l_org_name;

  exception
    when others then
      return null;
  end Get_Head_Org_Name2;
  ------------------------------------------------------------------------------------
  -- получение пути орг-ции до вершины
  ------------------------------------------------------------------------------------
  function get_path_org_to_top(pi_org_id    in t_organizations.org_id%type,
                               pi_worker_id in number) return sys_refcursor is
    res              sys_refcursor;
    l_user_structure number;
  begin

    logging_pkg.debug('pi_org_id=' || pi_org_id || '; pi_worker_id=' ||
                      pi_worker_id,
                      'ORGS.get_path_org_to_top');

    select case
             when max(nvl(rt.is_org_rtm, 1)) = 1 and
                  min(nvl(rt.is_org_rtm, 0)) = 0 then
              2
             else
              max(rt.is_org_rtm)
           end
      into l_user_structure
      from t_user_org t
      join t_org_is_rtmob rt
        on rt.org_id = t.org_id
     where t.usr_id = pi_worker_id;
    open res for
      select distinct (replace(tree_org, '@@', ',')) || ',-1,' nn,
                      (replace(tree_org1, '@@', ',')) || ',-1,' nn1

        from (select tor.org_id,
                     CONNECT_BY_ROOT(org.org_id) root_org_id,
                     /*reverse*/
                     (sys_connect_by_path( /*reverse*/(to_char(org.org_id)),
                                          '@@')) tree_org,
                     reverse(sys_connect_by_path(reverse(to_char(org.org_id)),
                                                 '@@')) tree_org1
                from t_org_relations tor
                join t_organizations org
                  on tor.org_id = org.org_id
                join t_org_is_rtmob rt
                  on rt.org_id = tor.org_id
              connect by prior tor.org_pid = tor.org_id
                     and (l_user_structure = 2 or
                         nvl(rt.is_org_rtm, l_user_structure) =
                         l_user_structure)
                     and tor.org_reltype not in ('1006', '1009')
               start with tor.org_id = nvl(pi_org_id, 1)
                      and tor.org_reltype not in ('1006', '1009')) aa
        join t_org_is_rtmob rt
          on rt.org_id = aa.root_org_id
         and (l_user_structure = 2 or
             nvl(rt.is_org_rtm, l_user_structure) = l_user_structure)
       where aa.org_id = 1
       order by nn1;
    return res;
  exception
    when others then
      return null;
  end;

  ------------------------------------------------------------------------------------
  -- получение пути до вершины для пользователя.
  ------------------------------------------------------------------------------------
  function get_path_org_to_top2(pi_worker_id in number) return sys_refcursor is
    res              sys_refcursor;
    l_user_structure number;
  begin

    logging_pkg.debug('pi_worker_id=' || pi_worker_id,
                      c_package || '.get_path_org_to_top2');

    select case
             when max(nvl(rt.is_org_rtm, 1)) = 1 and
                  min(nvl(rt.is_org_rtm, 0)) = 0 then
              2
             else
              max(rt.is_org_rtm)
           end
      into l_user_structure
      from t_user_org t
      join t_org_is_rtmob rt
        on rt.org_id = t.org_id
     where t.usr_id = pi_worker_id;
    open res for

    --
      with ids as
       (select x.org_id,
               SECURITY.is_user_have_right_for_org(pi_org_id    => x.org_id,
                                                   pi_worker_id => pi_worker_id) as have_rights,
               rn
          from (select tor.org_id,
                       CONNECT_BY_ROOT(org.org_id) root_org_id,
                       rownum rn
                  from t_org_relations tor
                  join t_organizations org
                    on tor.org_id = org.org_id
                  join t_org_is_rtmob rt
                    on rt.org_id = tor.org_id
                connect by prior tor.org_pid = tor.org_id
                       and (l_user_structure = 2 or
                           nvl(rt.is_org_rtm, l_user_structure) =
                           l_user_structure)
                 start with tor.org_id IN
                            (select org_id
                               from t_user_org
                              where usr_id = pi_worker_id)) x
          join t_org_is_rtmob rt
            on rt.org_id = x.root_org_id
           and (l_user_structure = 2 or
               nvl(rt.is_org_rtm, l_user_structure) = l_user_structure))
      select i.org_id, i.have_rights
        from ids i
       where not exists
       (select org_pid
                from mv_org_tree m
                join t_org_is_rtmob rt
                  on rt.org_id = m.org_id
                left join t_org_is_rtmob rt_p
                  on rt_p.org_id = m.org_pid
               where (m.org_pid, 1) in (select org_id, have_rights from ids)
              connect by prior m.ORG_PID = m.org_id
                     and m.ORG_RELTYPE != '1009'
                     and (rt.is_org_rtm is null or
                         rt.is_org_rtm =
                         nvl(rt_p.is_org_rtm, rt.is_org_rtm))
               start with m.org_id = i.org_id)
       order by i.rn desc;
    --
    return res;
  exception
    when others then
      return null;
  end;
  ------------------------------------------------------------------------------------
  -- получение наименования организации с учетом организации
  ------------------------------------------------------------------------------------
  function get_org_name_tree(pi_org_id in t_organizations.org_id%type)
    return varchar2 is
    res varchar2(4000);
  begin
    select substr(nn, 1, length(nn) - 1)
      into res
      from (select max(replace(tree_org, '@@', '/')) nn

              from (select tor.org_id,
                           CONNECT_BY_ROOT(org.org_id) root_org_id,
                           reverse(sys_connect_by_path(reverse(org.org_name),
                                                       '@@')) tree_org
                      from t_org_relations tor
                      join t_organizations org
                        on tor.org_id = org.org_id
                    connect by prior tor.org_pid = tor.org_id
                     start with tor.org_id = nvl(pi_org_id, 1)) aa
             where aa.org_id = 1);

    return res;
  exception
    when others then
      return null;
  end;

  -----------------------------------------------------------------------------------------
  -- Получение по региону МРФа
  -----------------------------------------------------------------------------------------
  function Get_mrf_by_region_kladr(pi_region_id in varchar2,
                                   po_err_num   out number,
                                   po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select dm.id, dm.name_mrf, dr.reg_id
        from t_dic_region dr, t_dic_mrf dm
       where dm.id = dr.mrf_id
         and dr.kl_region = pi_region_id;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end Get_mrf_by_region_kladr;

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
    return sys_refcursor is
    res sys_refcursor;
  begin

    if (not Security_pkg.Check_User_Right_str('EISSD.ORGS.PARAM.TM',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return null;
    end if;

    open res for
      select distinct o.org_id,
                      o.org_name,
                      date_tab.mm_date month_date,
                      m.id mrf_id,
                      nvl(tm.count_contact_transf, 0) count_contact_transf,
                      nvl(tm.count_contact_processed, 0) count_contact_processed,
                      nvl(tm.count_contact_finish, 0) count_contact_finish,
                      m.name_mrf
        from t_organizations o
        join mv_org_tree tree
          on o.org_id = tree.org_id
        join t_dogovor td
          on tree.id = td.org_rel_id
        join t_organizations po
          on po.org_id = tree.org_pid
        join t_dic_mrf m
          on 1 = 1        
        join (select distinct trunc(pi_date_start - 1 + level, 'mm') mm_date
                from dual
              connect by level <= (pi_date_end - pi_date_start + 1)) date_tab
          on 1 = 1
        left join t_org_param_tm tm
          on tm.org_id = o.org_id
         and tm.mrf_id = m.id
         and tm.month_date = date_tab.mm_date
       where tree.org_pid = pi_org_pid
         and (pi_org_id is null or tree.org_id = pi_org_id)
         and tree.org_reltype = 1008
         and o.is_enabled = 1
         and td.is_enabled = 1
       order by o.org_name, date_tab.mm_date, m.id;

    return res;

  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'orgs.get_dop_param_TM');
      return null;
  end;
  -------------------------------------------------------------------------
  --редактирование доп парметров организации
  --------------------------------------------------------------------------
  procedure save_org_dop_param_TM(pi_param     in org_param_tm_tab,
                                  pi_worker_id in number,
                                  po_err_num   out number,
                                  po_err_msg   out varchar2) is
    l_count number;
  begin
    savepoint sp_save_org_dop_param_TM;

    if (not Security_pkg.Check_User_Right_str('EISSD.ORGS.PARAM.TM',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      return;
    end if;

    if pi_param is null or pi_param.count = 0 then
      po_err_num := 1;
      po_err_msg := 'Не переданны данные для сохранения';
      return;
    end if;

    select max(count(*))
      into l_count
      from table(pi_param) p
     group by p.org_id, trunc(p.month_date, 'MM'), p.mrf_id;

    if l_count > 1 then
      po_err_num := 1;
      po_err_msg := 'Переданны дублирующиеся записи';
      return;
    end if;

    delete from t_org_param_tm tm
     where (tm.org_id, tm.month_date, tm.mrf_id) in
           (select p.org_id, trunc(p.month_date, 'MM'), p.mrf_id
              from table(pi_param) p
             where p.org_id is not null);

    insert into t_org_param_tm tm
      (org_id,
       month_date,
       count_contact_transf,
       count_contact_processed,
       count_contact_finish,
       mrf_id)
      select p.org_id,
             trunc(p.month_date, 'MM'),
             p.count_contact_transf,
             p.count_contact_processed,
             p.count_contact_finish,
             p.mrf_id
        from table(pi_param) p
       where p.org_id is not null;

    insert into t_org_param_tm_hst h
      select seq_ORG_PARAM_TM_HST.Nextval, pi_worker_id, sysdate, tm.*
        from t_org_param_tm tm
        join table(pi_param) p
          on p.org_id = tm.org_id
         and tm.month_date = trunc(p.month_date, 'MM')
         and tm.mrf_id = p.mrf_id
       where p.org_id is not null;

  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'orgs.save_org_dop_param_TM');
      rollback to sp_save_org_dop_param_TM;
      return;
  end;
  -------------------------------------------------------------------------
  --
  -------------------------------------------------------------------------
  function Get_Filials_by_worker(pi_worker_id in number,
                                 po_err_num   out pls_integer,
                                 po_err_msg   out t_Err_Msg)
    return sys_refcursor is
    l_cur            sys_refcursor;
    l_reg_id         t_dic_region.reg_id%type;
    l_kl_name        t_dic_region.kl_name%type;
    l_kl_region      t_dic_region.kl_region%type;
    l_EXT_ID         number;
    l_org_id         number;
    l_org_name       t_organizations.org_name%type;
    l_org_full_name  t_organizations.org_full_name%type;
    l_rtmob_org_id   number;
    l_reg_tab        num_tab := num_tab();
    res              sys_refcursor;
    us_orgs          num_tab := num_tab();
    l_mrf_id         number;
    l_get_pay_m2m    number;
    l_source_type    number;
    l_terminal_level number;
    l_gmt            number;
  begin
    select tor.org_id bulk collect
      into us_orgs
      from t_org_relations tor
    connect by prior tor.org_id = tor.org_pid
     start with tor.org_id in
                (select uo.org_id
                   from t_user_org uo
                  where uo.usr_id = pi_worker_id);
    --us_orgs := Get_User_Orgs_Tab_With_Param(pi_worker_id);
    l_cur := orgs.Get_Regions(pi_only_ural    => 0,
                              pi_only_filial  => 1,
                              pi_is_org_rtmob => 0,
                              pi_worker_id    => pi_worker_id,
                              po_err_num      => po_err_num,
                              po_err_msg      => po_err_msg);
    loop
      fetch l_cur
        into l_reg_id,
             l_kl_name,
             l_kl_region,
             l_EXT_ID,
             l_org_id,
             l_org_name,
             l_org_full_name,
             l_rtmob_org_id,
             l_mrf_id,
             l_get_pay_m2m,
             l_source_type,
             l_terminal_level,
             l_gmt;
      exit when l_cur%notfound;
      l_reg_tab.extend;
      l_reg_tab(l_reg_tab.count) := l_reg_id;
    end loop;
    close l_cur;
    open res for
      select ok.org_id   filial_id_rtk,
             ok.org_name filial_name_rtk,
             om.org_id   filial_id_rtm,
             om.org_name filial_name_rtm
        from t_dic_region r
        join t_dic_region_info ii
          on ii.reg_id = r.reg_id
        join t_organizations ok
          on (r.org_id = 2004862 and ok.org_id = 2004855)
          or (r.org_id <> 2004862 and ok.org_id = r.org_id)
        join t_organizations om
          on om.org_id = ii.rtmob_org_id
       where r.reg_id in (select * from table(l_reg_tab))
         and (ok.org_id in (select * from table(us_orgs)) or
             om.org_id in (select * from table(us_orgs)))
       order by om.org_name;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'orgs.Get_Filials_by_worker');
      return null;
  end;
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
    return num_tab is
    res                num_tab := num_tab();
    l_rel_tab          num_tab;
    l_prm_tab          num_tab;
    l_is_rtm_structure number;
    l_current_org      number;
    l_user_rtk         boolean;
    l_user_rtm         boolean;
  begin
    begin
      select nvl(is_org_rtmob(o.org_id), 1), o.org_id
        into l_is_rtm_structure, l_current_org
        from t_organizations o
       where o.org_id = pi_org_id
         and (pi_is_enabled_org = 0 or o.is_enabled = 1);
    exception
      when no_data_found then
        po_err_num := sqlcode;
        po_err_msg := 'Организация не найдена';
        logging_pkg.error('организация не найдена. pi_org_id = ' ||
                          pi_org_id,
                          'orgs.get_orgs_by_structure_and_prm');
        return num_tab();
    end;

    if l_current_org is not null then

      security_pkg.get_user_structures(pi_worker_id => pi_worker_id,
                                       po_is_rtk    => l_user_rtk,
                                       po_is_rtm    => l_user_rtm,
                                       po_err_num   => po_err_num,
                                       po_err_msg   => po_err_msg);

      -- Типы связей
      if pi_rel_tab is null or pi_rel_tab.count = 0 then
        select t.rel_tp_id bulk collect
          into l_rel_tab
          from t_relation_type t;
      else
        l_rel_tab := pi_rel_tab;
      end if;

      -- Договорные пермишены
      if pi_prm_tab is null or pi_prm_tab.count = 0 then
        select p.prm_id bulk collect
          into l_prm_tab
          from t_perm p
         where p.prm_type = c_prm_type_dogovor;
      else
        l_prm_tab := pi_prm_tab;
        -- Если выбранная организация не имеет договоров по данному пермишену в структуре пользователя - сразу выходим
        if l_user_rtk and not l_user_rtm then
          if is_org_have_prm(pi_org_id         => pi_org_id,
                             pi_prm_tab        => l_prm_tab,
                             pi_is_org_enabled => pi_is_enabled_org,
                             pi_is_dog_enabled => pi_is_enabled_dog,
                             pi_structure      => 0,
                             po_err_num        => po_err_num,
                             po_err_msg        => po_err_msg) < 1 then
            return num_tab();
          end if;
        elsif l_user_rtm and not l_user_rtk then
          if is_org_have_prm(pi_org_id         => pi_org_id,
                             pi_prm_tab        => l_prm_tab,
                             pi_is_org_enabled => pi_is_enabled_org,
                             pi_is_dog_enabled => pi_is_enabled_dog,
                             pi_structure      => 1,
                             po_err_num        => po_err_num,
                             po_err_msg        => po_err_msg) < 1 then
            return num_tab();
          end if;
        else
          null;
        end if;

      end if;

      if nvl(pi_is_cur_rtk, 0) = 0 and nvl(pi_with_curated, 0) = 0 and
         nvl(pi_with_self, 0) = 1 then
        return num_tab(l_current_org);
        --      elsif (nvl(pi_is_cur_rtk, 0) = 1 and l_is_rtm_structure = 1) or
        --         nvl(pi_with_curated, 0) = 1 then
      else
        select distinct tor.org_id bulk collect
          into res
          from t_org_relations tor
          join t_organizations o
            on o.org_id = tor.org_id
           and (o.is_enabled = 1 or pi_is_enabled_org = 1)
          left join t_dogovor d
            on d.org_rel_id = tor.id
         where (l_is_rtm_structure = 0 or pi_is_cur_rtk = 1 or
               nvl(is_org_rtmob(tor.org_id), 1) <> 0) --  либо структура ртк, либо есть призранака "абоненты курируемых ртк" либо организация ртк
           and (l_is_rtm_structure = 0 or pi_with_curated = 1 or
               nvl(is_org_rtmob(tor.org_id), 1) = 0) -- либо структура ртк, либо нет принака "абоненты курируемых", либо организация ртм
           and (pi_with_self = 1 or tor.org_id <> l_current_org)
        connect by tor.org_pid = prior tor.org_id
               and tor.org_reltype in
                   (select column_value from table(l_rel_tab))
               and (nvl(d.dog_id, 0) = 0 or exists
                    (select null
                       from t_dogovor_prm prm
                      where prm.dp_dog_id = d.dog_id
                        and prm.dp_prm_id in (select * from table(l_prm_tab))
                        and prm.dp_is_enabled = 1))
         start with tor.org_id = l_current_org;
      end if;
    end if;

    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'orgs.get_orgs_by_structure_and_prm');
      return num_tab();
  end;

  -----------------------------------------------------------------
  -- Список организаций и договоров
  -----------------------------------------------------------------
  function get_org_dog_list(pi_worker_id      in number,
                            pi_org_id         in number,
                            pi_is_enabled_org in number,
                            pi_is_enabled_dog in number,
                            pi_is_cur_rtk     in number,
                            pi_with_curated   in number,
                            pi_with_self      in number,
                            pi_rel_tab        in num_tab,
                            po_err_num        out number,
                            po_err_msg        out varchar2)
    return array_num_2 is
    res                array_num_2 := array_num_2();
    l_rel_tab          num_tab;
    l_is_rtm_structure number;
    l_current_org      number;
    l_user_rtk         boolean;
    l_user_rtm         boolean;
  begin
    begin
      select nvl(is_org_rtmob(o.org_id), 1), o.org_id
        into l_is_rtm_structure, l_current_org
        from t_organizations o
       where o.org_id = pi_org_id
         and (pi_is_enabled_org = 0 or o.is_enabled = 1);
    exception
      when no_data_found then
        po_err_num := sqlcode;
        po_err_msg := 'Организация не найдена';
        logging_pkg.error('организация не найдена. pi_org_id = ' ||
                          pi_org_id,
                          'orgs.get_org_dog_list');
        return array_num_2();
    end;

    if l_current_org is not null then

      security_pkg.get_user_structures(pi_worker_id => pi_worker_id,
                                       po_is_rtk    => l_user_rtk,
                                       po_is_rtm    => l_user_rtm,
                                       po_err_num   => po_err_num,
                                       po_err_msg   => po_err_msg);

      -- Типы связей
      if pi_rel_tab is null or pi_rel_tab.count = 0 then
        select t.rel_tp_id bulk collect
          into l_rel_tab
          from t_relation_type t;
      else
        l_rel_tab := pi_rel_tab;
      end if;

      if nvl(pi_is_cur_rtk, 0) = 0 and nvl(pi_with_curated, 0) = 0 and
         nvl(pi_with_self, 0) = 1 then
        return null; -- num_tab(l_current_org);
      else
        select distinct rec_num_2(tor.org_id, d.dog_id) bulk collect
          into res
          from t_org_relations tor
          join t_organizations o
            on o.org_id = tor.org_id
           and (o.is_enabled = 1 or pi_is_enabled_org = 1)
          left join t_dogovor d
            on d.org_rel_id = tor.id
         where (l_is_rtm_structure = 0 or pi_is_cur_rtk = 1 or
               nvl(is_org_rtmob(tor.org_id), 1) <> 0) --  либо структура ртк, либо есть призранака "абоненты курируемых ртк" либо организация ртк
           and (l_is_rtm_structure = 0 or pi_with_curated = 1 or
               nvl(is_org_rtmob(tor.org_id), 1) = 0) -- либо структура ртк, либо нет принака "абоненты курируемых", либо организация ртм
           and (pi_with_self = 1 or tor.org_id <> l_current_org)
        connect by tor.org_pid = prior tor.org_id
               and tor.org_reltype in
                   (select column_value from table(l_rel_tab))
         start with tor.org_id = l_current_org;
      end if;
    end if;

    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'orgs.get_orgs_by_structure_and_prm');
      return array_num_2();
  end get_org_dog_list;

  --------------------------------------------------------
  -- Проверка - есть ли у организации в выбранной структуре пермишен
  --------------------------------------------------------
  function is_org_have_prm(pi_org_id         in number,
                           pi_prm_tab        in num_tab,
                           pi_is_org_enabled in number,
                           pi_is_dog_enabled in number,
                           pi_structure      in number,
                           po_err_num        out number,
                           po_err_msg        out varchar2) -- not null!
   return integer is
    res integer;
  begin
    if is_org_usi(pi_org_id) > 0 then
      return 1;
    end if;

    select count(tor.id)
      into res
      from t_org_relations tor
      join t_organizations o
        on o.org_id = tor.org_id
       and (pi_is_org_enabled = 0 or o.is_enabled = 1)
      left join t_dogovor d
        on d.org_rel_id = tor.id
       and (pi_is_dog_enabled = 0 or d.is_enabled = 1)
      left join t_dogovor_prm prm
        on prm.dp_dog_id = d.dog_id
       and prm.dp_prm_id in (select column_value from table(pi_prm_tab))
     where d.dog_id is not null
       and prm.dp_dog_id is not null
       and nvl(is_org_rtmob(tor.org_pid), 0) = pi_structure
     start with tor.org_id = pi_org_id
    connect by tor.org_id = prior tor.org_pid
           and prior nvl(d.dog_id, 0) = 0; -- prior d.dog_id is null не работает
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'orgs.is_org_have_prm');
      return 0;
  end;

  -----------------------------------------------------------------
  -- Договоры между двумя организациями
  -----------------------------------------------------------------
  function get_dogovors_between_orgs(pi_org_id           in number,
                                     pi_org_pid          in number := null,
                                     pi_dogovor_class_id in num_tab,
                                     pi_worker_id        in number,
                                     po_err_num          out pls_integer,
                                     po_err_msg          out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select dog.dog_id,
             dog.dog_date,
             dog.dog_number,
             dog.dog_class_id class_dog
        from (select tor.*
                from t_org_relations tor
               where tor.org_id = pi_org_id
               start with tor.org_pid = pi_org_pid
              connect by prior tor.org_id = tor.org_pid) t
        join t_dogovor dog
          on dog.org_rel_id = t.id
         and dog.is_enabled = 1;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'orgs.get_dogovors_between_orgs');
      if res%isopen then
        close res;
      end if;
      return null;
  end;

  ----------------------------------------------------------------
  -- список организаций пользователя, ограниченный по пермишену
  ----------------------------------------------------------------
  function get_user_orgs_by_prm(pi_worker_id in number,
                                pi_rel_tab   in num_tab,
                                pi_prm_tab   in num_tab,
                                po_err_num   out number,
                                po_err_msg   out varchar2) return num_tab is
    res num_tab := num_tab();
    tmp num_tab := num_tab();
  begin
    for orgs in (select distinct tuo.org_id
                   from t_user_org tuo
                  where tuo.usr_id = pi_worker_id) loop
      tmp := get_orgs_by_structure_and_prm(pi_worker_id      => pi_worker_id,
                                           pi_org_id         => orgs.org_id,
                                           pi_is_enabled_org => 0,
                                           pi_is_enabled_dog => 0,
                                           pi_is_cur_rtk     => 1,
                                           pi_with_curated   => 1,
                                           pi_with_self      => 1,
                                           pi_rel_tab        => pi_rel_tab,
                                           pi_prm_tab        => pi_prm_tab,
                                           po_err_num        => po_err_num,
                                           po_err_msg        => po_err_msg);

      for i in 1 .. tmp.count loop
        res.extend;
        res(res.count) := tmp(i);
      end loop;
    end loop;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'orgs.get_user_orgs_by_prm');
      return num_tab();
  end;
  -- Плюс учет заблокированности для отчетов типа остатков и движения
  function get_user_orgs_by_prm(pi_worker_id in number,
                                pi_rel_tab   in num_tab,
                                pi_prm_tab   in num_tab,
                                pi_block     in number,
                                po_err_num   out number,
                                po_err_msg   out varchar2) return num_tab is
    res num_tab := num_tab();
    tmp num_tab := num_tab();
  begin
    for orgs in (select distinct tuo.org_id
                   from t_user_org tuo
                  where tuo.usr_id = pi_worker_id) loop
      tmp := get_orgs_by_structure_and_prm(pi_worker_id      => pi_worker_id,
                                           pi_org_id         => orgs.org_id,
                                           pi_is_enabled_org => pi_block,
                                           pi_is_enabled_dog => pi_block,
                                           pi_is_cur_rtk     => 1,
                                           pi_with_curated   => 1,
                                           pi_with_self      => 1,
                                           pi_rel_tab        => pi_rel_tab,
                                           pi_prm_tab        => pi_prm_tab,
                                           po_err_num        => po_err_num,
                                           po_err_msg        => po_err_msg);

      for i in 1 .. tmp.count loop
        res.extend;
        res(res.count) := tmp(i);
      end loop;
    end loop;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg,
                        'orgs.get_user_orgs_by_prm');
      return num_tab();
  end;

  --получение наименований организации по списку
  function get_org_name_tab(pi_org_tab in num_tab) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select o.org_id,
             O.ORG_NAME,
             is_org_rtmob(o.org_id) is_org_rtmob,
             is_org_filial(o.org_id) is_org_filial,
             0 is_startup,
             r.kl_region
        from T_ORGANIZATIONS O
        left join t_dic_region r
          on r.reg_id = o.region_id
       where O.ORG_ID in (select * from table(pi_org_tab))
       order by o.org_id;
    return res;
  exception
    when others then

      logging_pkg.error(sqlcode || '.' || sqlerrm || ' ' ||
                        dbms_utility.format_error_backtrace,
                        'orgs.get_org_name_tab');
      return null;
  end;
  ------------------------------------------------------------------------
  -- Получение рутовых орг-ций, доступных пользователю
  ------------------------------------------------------------------------
  function Get_Root_Orgs_by_User(pi_worker_id in number,
                                 po_err_num   out number,
                                 po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      with ids as
       (select distinct tr.org_pid
          from t_user_org uo
          join mv_org_tree tr
            on tr.org_id = uo.org_id
          join t_org_is_rtmob rt
            on rt.org_id = tr.org_id
          left join t_org_is_rtmob rt_p
            on rt_p.org_id = tr.org_pid
         where uo.usr_id = pi_worker_id
           and (rt.is_org_rtm is null or
               rt.is_org_rtm = nvl(rt_p.is_org_rtm, rt.is_org_rtm)))
      select *
        from ids i
       where not exists (select org_pid
                from mv_org_tree m
                join t_org_is_rtmob rt
                  on rt.org_id = m.org_id
                left join t_org_is_rtmob rt_p
                  on rt_p.org_id = m.org_pid
               where m.org_pid in (select * from ids)
              connect by prior m.ORG_PID = m.org_id
                     and m.ORG_RELTYPE != '1009'
                     and (rt.is_org_rtm is null or
                         rt.is_org_rtm =
                         nvl(rt_p.is_org_rtm, rt.is_org_rtm))
               start with m.org_id = i.org_pid);
    return res;
  exception
    when others then
      return null;
  end;
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
    return sys_refcursor is
    res sys_refcursor;
  begin
    logging_pkg.debug(get_str_by_num_tab(pi_org_type),
                      'Get_Child_Org_by_Type');
    open res for
      select distinct tor.org_id, o.org_name
        from mv_org_tree tor
        join t_organizations o
          on o.org_id = tor.org_id
        join t_org_is_rtmob rt
          on rt.org_id = o.org_id
        left join t_dogovor d
          on d.org_rel_id = tor.id
       where tor.org_reltype in (select * from table(pi_org_type))
         and (o.is_enabled = 1 or pi_block_org = 1)
         and (nvl(d.is_enabled, 1) = 1 or pi_block_dog = 1)
         and (pi_branch = 2 or
             (nvl(rt.is_org_rtm, 0) = 0 and pi_branch = 0) or
             (nvl(rt.is_org_rtm, 1) = 1 and pi_branch = 1))
      connect by prior tor.org_id = tor.org_pid
       start with tor.org_id in (pi_org_pid);
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg, 'Get_Child_Org_by_Type');
      return null;
  end;
  ------------------------------------------------------------------------
  --получение списка договорных прав по орг-ции
  ------------------------------------------------------------------------
  function Get_dog_perm_by_ORG(pi_org_id    in number,
                               pi_worker_id in number,
                               po_err_num   out number,
                               po_err_msg   out varchar2)
    return sys_refcursor is
    c_pr_name constant varchar2(65) := c_package || '.Get_dog_perm_by_ORG';
    res sys_refcursor;
  begin
    open res for
      select distinct dp.dp_prm_id
        from t_org_relations tor
        left join t_dogovor d
          on d.org_rel_id = tor.id
        left join t_dogovor_prm dp
          on dp.dp_dog_id = d.dog_id
         and dp.dp_is_enabled = 1
       where d.dog_id is not null
      connect by prior tor.org_pid = tor.org_id
       start with tor.org_id in (pi_org_id);
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_msg, c_pr_name);
      return null;
  end;
  ------------------------------------------------------------------------
  -- Получение ИД региона ЕИССД по ИД региона, возращаемому ПС
  ------------------------------------------------------------------------
  function getRegionByRegionPS(pi_reg_ps    in t_dic_regions_ps.ps_reg_id%type,
                               pi_worker_id in number,
                               po_err_num   out number,
                               po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select t.reg_id region_id, t.id_mvno region_id_mvno
        from t_dic_regions_ps t
       where t.ps_reg_id = pi_reg_ps;
    return res;
  exception
    when no_data_found then
      po_err_num := 1;
      po_err_msg := 'Соответствие регионов не найдено в системе.';
      return null;
  end getRegionByRegionPS;
  ------------------------------------------------------------------------
  --получение справочника услуг ЦПО
  ------------------------------------------------------------------------
  function get_dic_org_ss_service(pi_worker_id in number,
                                  po_err_num   out number,
                                  po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select t.id, t.ss_service
        from t_dic_org_ss_service t
       where is_actual = 1;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
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
    return sys_refcursor is
    c_pr_name constant varchar2(65) := c_package ||
                                       'get_list_org_ss_center';
    res            sys_refcursor;
    l_order_asc    number; -- по возрастанияю
    l_order_desc   number; -- по убыванию
    l_max_num_page number;
    l_num_page     number;
    l_column       number;
    l_col_request  request_Order_Tab;
    l_check_reg    number := 0;
    org_tab        num_tab := num_tab();
    /****************************************/
    function getStrParam return varchar2 is
    begin
      return substr('pi_date_actual=' || pi_date_actual || ';' ||
                    'pi_reg_tab=' || get_str_by_string_tab(pi_reg_tab) || ';' ||
                    'pi_addr_obj_id=' || pi_addr_obj_id || ';' ||
                    'pi_house_obj_id=' || pi_house_obj_id || ';' ||
                    'pi_addr_building=' || pi_addr_building || ';' ||
                    'pi_addr_office=' || pi_addr_office || ';' ||
                    'pi_num_page=' || pi_num_page || ';' ||
                    'pi_count_req=' || pi_count_req || ';' || 'pi_column=' ||
                    pi_column || ';' || 'pi_sorting=' || pi_sorting || ';' ||
                    'pi_worker_id=' || pi_worker_id,
                    1,
                    2000);
    end getStrParam;
    /****************************************/
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    if pi_reg_tab is not null and pi_reg_tab.count > 0 then
      l_check_reg := 1;
    end if;

    -- Если параметры не переданы
    l_column := nvl(pi_column, 2);

    If nvl(pi_sorting, 0) = 0 then
      l_order_asc := l_column;
    else
      l_order_desc := l_column;
    end If;

    org_tab := Get_User_Orgs_Tab(pi_worker_id, 0);

    -- сформируем сортированный список
    select request_Order_Type(org_id, rownum) bulk collect
      into l_col_request
      from (select t.org_id
              from t_org_ss_center t
              join t_organizations o
                on o.org_id = t.org_id
            --and o.is_enabled = 1 --110193
              left join t_dic_region r
                on r.reg_id = o.region_id
              left join t_dic_mrf m
                on r.mrf_id = m.id
              left join t_address a
                on a.addr_id = o.adr2_id
              left join t_address_object ao
                on ao.id = a.addr_obj_id
            --контактное лицо
              left join t_person p
                on p.person_id = o.touch_id
             where t.is_enabled = 1
               and o.is_ss_center = 1
                  /* and (pi_date_actual is null or t.close_date >= pi_date_actual or
                  t.close_date is null)*/
               and (l_check_reg = 0 or
                   r.kl_region in (select * from table(pi_reg_tab)))
               and (pi_addr_obj_id is null or
                   a.addr_obj_id in
                   (select ao.id
                       from t_address_object ao
                     connect by prior ao.id = ao.parent_id
                            and ao.is_deleted = 0
                      start with ao.id = pi_addr_obj_id))
               and (pi_house_obj_id is null or
                   a.addr_house_obj_id = pi_house_obj_id)
               and (pi_addr_building is null or
                   a.addr_building = pi_addr_building)
               and (pi_addr_office is null or a.addr_office = pi_addr_office)
               and (pi_segment_service is null or
                   t.segment_service = pi_segment_service)
               and t.org_id in (select * from TABLE(org_tab))
             order by decode(l_order_asc,
                             null,
                             null,
                             1,
                             t.org_id,
                             2,
                             m.name_mrf,
                             3,
                             r.kl_name,
                             4,
                             r.kl_region,
                             5,
                             o.org_name,
                             6,
                             o.org_full_name,
                             7,
                             a.addr_city,
                             8,
                             a.addr_street,
                             9,
                             t.latitude,
                             10,
                             t.longitude,
                             11,
                             p.person_lastname || p.person_firstname ||
                             p.person_middlename,
                             12,
                             t.segment_service,
                             13,
                             t.square_meter,
                             14,
                             ao.a_full_name,
                             null) asc,
                      decode(l_order_desc,
                             null,
                             null,
                             1,
                             t.org_id,
                             2,
                             m.name_mrf,
                             3,
                             r.kl_name,
                             4,
                             r.kl_region,
                             5,
                             o.org_name,
                             6,
                             o.org_full_name,
                             7,
                             a.addr_city,
                             8,
                             a.addr_street,
                             9,
                             t.latitude,
                             10,
                             t.longitude,
                             11,
                             p.person_lastname || p.person_firstname ||
                             p.person_middlename,
                             12,
                             t.segment_service,
                             13,
                             t.square_meter,
                             14,
                             ao.a_full_name,
                             null) desc);
    po_all_count   := l_col_request.count;
    l_max_num_page := round(po_all_count / nvl(pi_count_req, 1));

    if (pi_num_page > l_max_num_page and l_max_num_page <> 0) then
      l_num_page := l_max_num_page + 1;
    else
      l_num_page := nvl(pi_num_page, 1);
    end if;

    open res for
      select t.org_id,
             o.org_name,
             o.org_full_name,
             r.kl_region,
             r.reg_id,
             r.kl_name || ' ' || r.kl_socr reg_name,
             m.name_mrf,
             t.segment_service,
             t.latitude,
             t.longitude,
             t.ownership,
             t.square_meter,
             t.workers_number,
             a.addr_id,
             a.addr_city,
             a.addr_street,
             a.addr_building,
             a.addr_office,
             p.person_lastname contact_lastname,
             p.person_firstname contact_firstname,
             p.person_middlename contact_middlename,
             p.person_sex contact_sex,
             p.person_email contact_email,
             p.person_phone contact_phone,
             o.is_pay_espp,
             (select cast(collect(SSC_PHONES_type(sp.phone_number,
                                                  sp.segment_service)) as
                          SSC_PHONES_tab)
                from T_ORG_SSC_PHONE sp
               where sp.org_id = t.org_id) org_phone,
             a.addr_city,
             a.addr_street,
             a.addr_building,
             a.addr_index,
             o.erp_r12_num,
             t.addr_fact_district,
             t.cluster_id,
             t.open_date,
             t.close_date,
             (select cast(collect(c.phone) as string_tab)
                from t_org_ssc_contact_phone c
               where c.org_id = o.org_id) contact_phones,
             case
               when t.close_date <= pi_date_actual then
                'Закрыт'
               else
                case
                  when tt.date_start is not null then
                   'Временно закрыт'
                  else
                   'Открыт'
                end
             end as status,
             t.url,
             t.cnt_cashbox,
             t.full_name_ssc,
             t.PRIORITY,
             o.IS_STAMP,
             t.metro,
             t.email,
             t.is_electro_queue,
             t.is_gold_pool,
             t.cnt_term_agent,
             t.cnt_term_rtc,
             resp.person_lastname resp_lastname,
             resp.person_firstname resp_firstname,
             resp.person_middlename resp_middlename,
             resp.person_phone resp_phone,
             resp.person_email resp_email
        from (Select request_id org_id, rn
                from table(l_col_request)
               where rn between
                     (l_num_page - 1) * nvl(pi_count_req, po_all_count) + 1 and
                     (l_num_page) * nvl(pi_count_req, po_all_count)) col_rc
        join t_org_ss_center t
          on t.org_id = col_rc.org_id
        join t_organizations o
          on o.org_id = t.org_id
      --and o.is_enabled = 1
        left join t_dic_region r
          on r.reg_id = o.region_id
        left join t_dic_mrf m
          on r.mrf_id = m.id
        left join t_address a
          on a.addr_id = o.adr2_id
        left join t_address_object ao
          on ao.id = a.addr_obj_id
      --контактное лицо
        left join t_person p
          on p.person_id = o.touch_id
        left join t_org_ssc_temp_close tt
          on pi_date_actual between tt.date_start and tt.date_end
         and tt.org_id = o.org_id
        left join t_person resp
          on resp.person_id = o.resp_id
       order by col_rc.rn;

    open po_timetable for
      select t.id,
             t.org_id,
             t.day_number,
             t.work_start,
             t.work_end,
             t.break_start,
             t.break_end,
             CASE
               WHEN t.WORK_START = c_work_start AND t.WORK_END = c_work_end THEN
                1
               ELSE
                0
             END AS fulltime,
             CASE
               WHEN t.BREAK_START IS NULL AND t.BREAK_END IS NULL THEN
                1
               ELSE
                0
             END AS without_break
        from (Select request_id org_id, rn
                from table(l_col_request)
               where rn between
                     (l_num_page - 1) * nvl(pi_count_req, po_all_count) + 1 and
                     (l_num_page) * nvl(pi_count_req, po_all_count)) col_rc
        join t_org_ssc_timetable t
          on t.org_id = col_rc.org_id
       where t.is_enabled = 1
       order by col_rc.rn;

    open po_employee for
      select e.org_id, e.type_employee, e.name, e.phone
        from (Select request_id org_id, rn
                from table(l_col_request)
               where rn between
                     (l_num_page - 1) * nvl(pi_count_req, po_all_count) + 1 and
                     (l_num_page) * nvl(pi_count_req, po_all_count)) col_rc
        join t_org_ssc_employee e
          on col_rc.org_id = e.org_id
       order by col_rc.rn;

    open po_services for
      select s.org_id,
             s.ss_service,
             s.ss_service_pos,
             s.segment_service,
             ss.ss_service service_name
        from (Select request_id org_id, rn
                from table(l_col_request)
               where rn between
                     (l_num_page - 1) * nvl(pi_count_req, po_all_count) + 1 and
                     (l_num_page) * nvl(pi_count_req, po_all_count)) col_rc
        join t_org_ss_service s
          on col_rc.org_id = s.org_id
        join t_dic_org_ss_service ss
          on ss.id = s.SS_SERVICE
         and ss.is_actual = 1
       order by col_rc.rn;

    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;
  --------------------------------------------------------------------------------------
  -- Групповое редактирование ЦПО
  --------------------------------------------------------------------------------------
  PROCEDURE Update_Ssc_Info_Mass(PI_ORG_ID         IN ARRAY_NUM_2,
                                 PI_BLOCK          IN NUMBER,
                                 PI_ORG_RELATION   IN NUM_TAB,
                                 PI_SS_SERVICE     IN SS_SERVICE_TAB,
                                 PI_SSC_TIMETABLE  IN ORG_TIMETABLE_TAB,
                                 PI_UNRESERVED_TMC IN array_num_2,
                                 PI_WORKER_ID      IN NUMBER,
                                 pi_cluster_id     in number,
                                 pi_ssc_employee   in org_employee_tab,
                                 pi_ssc_phone      in SSC_PHONES_tab,
                                 pi_is_pay_espp    in t_organizations.is_pay_espp%type,
                                 PO_ERR_NUM        OUT PLS_INTEGER,
                                 PO_ERR_MSG        OUT VARCHAR2) IS
    l_org_tab      num_tab;
    l_user_org_tab num_tab;
    --l_count        number;
    l_hst_id     number;
    l_empl       org_employee_tab;
    l_phones     SSC_PHONES_tab;
    l_SS_SERVICE SS_SERVICE_TAB;
  BEGIN
    -- Проверка прав на редактирование ЦПО
    if (not Security_pkg.Check_User_Right_str('EISSD.ORGS.EDIT',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      null;
    end if;
    -- Получаем организации, доступные полльзователю
    l_org_tab      := get_orgs_tab_for_multiset(pi_orgs         => pi_org_id,
                                                pi_worker_id    => pi_worker_id,
                                                pi_block        => pi_block,
                                                pi_org_relation => pi_org_relation);
    l_user_org_tab := get_user_orgs_by_prm(pi_worker_id => pi_worker_id,
                                           pi_rel_tab   => null,
                                           pi_prm_tab   => null,
                                           po_err_num   => po_err_num,
                                           po_err_msg   => po_err_msg);
    l_org_tab      := intersects(l_org_tab, l_user_org_tab);
    -- Редактируем
    for i in (select *
                from table(l_org_tab) org, t_org_ss_center osc
               where org.column_value = osc.org_id) loop

      if pi_ssc_employee is not null and pi_ssc_employee is not empty then
        select org_employee_type(type_position => nvl(e.type_employee,
                                                      t.type_position),
                                 people_name   => nvl(t.people_name, e.name),
                                 people_phone  => nvl(t.people_phone, e.phone)) bulk collect
          into l_empl
          from t_org_ssc_employee e
          full join table(pi_ssc_employee) t
            on t.type_position = e.type_employee
           and e.org_id = i.column_value
         where e.org_id = i.column_value
            or e.org_id is null;
      end if;

      if pi_ssc_phone is not null and pi_ssc_phone is not empty then
        select ssc_phones_type(PHONE_NUMBER    => nvl(t.PHONE_NUMBER,
                                                      p.phone_number),
                               SEGMENT_SERVICE => nvl(p.segment_service,
                                                      t.SEGMENT_SERVICE)) bulk collect
          into l_phones
          from t_org_ssc_phone p
          full join table(pi_ssc_phone) t
            on p.segment_service = t.segment_service
           and p.org_id = i.column_value
         where p.org_id = i.column_value
            or (p.org_id is null and
               t.SEGMENT_SERVICE =
               decode(i.SEGMENT_SERVICE,
                       3,
                       t.SEGMENT_SERVICE,
                       i.SEGMENT_SERVICE));
      end if;

      if PI_SS_SERVICE is not null and PI_SS_SERVICE is not empty then
        select ss_service_type(SERVICE_ID      => SERVICE_ID,
                               SERVICE_POS     => SERVICE_POS,
                               SEGMENT_SERVICE => SEGMENT_SERVICE) bulk collect
          into l_SS_SERVICE
          from (select t.SEGMENT_SERVICE, t.SERVICE_ID, t.SERVICE_POS
                  from table(PI_SS_SERVICE) t
                 where t.SEGMENT_SERVICE =
                       decode(i.SEGMENT_SERVICE,
                              3,
                              t.SEGMENT_SERVICE,
                              i.SEGMENT_SERVICE)
                union
                select p.segment_service, p.ss_service, p.ss_service_pos
                  from t_org_ss_service p
                 where p.org_id = i.column_value
                   and p.segment_service not in
                       (select segment_service from table(PI_SS_SERVICE)));
      end if;

      UPDATE_SSC_INFO(PI_ORG_ID               => i.column_value,
                      PI_SSC_PHONES           => l_phones,
                      PI_SSC_TIMETABLE        => PI_SSC_TIMETABLE,
                      PI_SSC_EMAIL            => i.email,
                      PI_SSC_LATITUDE         => i.LATITUDE,
                      PI_SSC_LONGITUDE        => i.LONGITUDE,
                      PI_SSC_CLOSE_DATE       => i.CLOSE_DATE,
                      PI_SSC_SEGM_SERVICE     => i.SEGMENT_SERVICE,
                      PI_SSC_URL              => i.URL,
                      pi_ss_service           => l_SS_SERVICE,
                      pi_OWNERSHIP            => i.OWNERSHIP,
                      pi_SQUARE_METER         => i.SQUARE_METER,
                      pi_WORKERS_NUMBER       => i.WORKERS_NUMBER,
                      PI_METRO                => i.METRO,
                      PI_PRIORITY             => i.PRIORITY,
                      PI_COMMENTS             => i.COMMENTS,
                      PI_WORKER_ID            => PI_WORKER_ID,
                      pi_ssc_cluster          => nvl(pi_cluster_id,
                                                     i.cluster_id),
                      pi_ssc_fact_district    => i.addr_fact_district,
                      pi_ssc_temp_close       => null,
                      pi_ssc_date_open        => i.OPEN_DATE,
                      pi_ssc_contact_phone    => null,
                      pi_ssc_CNT_CASHBOX      => i.CNT_CASHBOX,
                      pi_ssc_employee         => l_empl,
                      pi_FULL_NAME_SSC        => i.FULL_NAME_SSC,
                      pi_ssc_CNT_TERM_RTC     => i.CNT_TERM_RTC,
                      pi_ssc_CNT_TERM_AGENT   => i.CNT_TERM_AGENT,
                      pi_ssc_IS_ELECTRO_QUEUE => i.IS_ELECTRO_QUEUE,
                      pi_ssc_IS_GOLD_POOL     => i.IS_GOLD_POOL);

      if PI_UNRESERVED_TMC is not null and PI_UNRESERVED_TMC.Count != 0 and PI_UNRESERVED_TMC(1)
        .number_1 is not null then
        delete from t_org_tmc_unreserv t
         where t.org_id = i.column_value
           and t.tmc_type in
               (select number_1 from table(PI_UNRESERVED_TMC));
        insert into t_org_tmc_unreserv
          (org_id, tmc_type, cou)
          select i.column_value, number_1, number_2
            from table(PI_UNRESERVED_TMC);
        --записываем в историю
        insert into t_org_tmc_unreserv_hst
          (org_id, tmc_type, cou)
          select t.org_id, t.tmc_type, t.cou
            from t_org_tmc_unreserv t
           where t.org_id = i.column_value;
      end if;

      if pi_is_pay_espp is not null then
        update t_organizations o
           set o.is_pay_espp = pi_is_pay_espp
         where o.org_id = i.column_value;
      end if;
      -- Сохраняем историю по организации
      l_hst_id := orgs.save_org_hst(pi_worker_id, i.column_value);
    end loop;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
  end Update_Ssc_Info_Mass;
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
                        po_err_msg     out varchar2) return sys_refcursor is
    c_pr_name constant varchar2(65) := c_package || 'Get_Org_List';
    res                sys_refcursor;
    us_reg             num_tab;
    l_column_sort_asc  number;
    l_column_sort_desc number;
    l_max_num_page     number;
    l_num_page         number;
    l_org_tab          array_num_4;
    /****************************************/
    function getStrParam return varchar2 is
    begin
      return substr('pi_org_name=' || pi_org_name || ';' || 'pi_inn=' ||
                    pi_inn || ';' || 'pi_org_id=' || pi_org_id || ';' ||
                    'pi_dog_number=' || pi_dog_number || ';' ||
                    'pi_page_number=' || pi_page_number || ';' ||
                    'pi_count_rows=' || pi_count_rows || ';' || 'pi_sort=' ||
                    pi_sort || ';' || 'pi_column_sort=' || pi_column_sort || ';' ||
                    'pi_worker_id=' || pi_worker_id,
                    1,
                    2000);
    end getStrParam;
  begin
    logging_pkg.debug(getStrParam, c_pr_name);
    us_reg := orgs.Get_Regions_User(pi_worker_id => pi_worker_id);
    if pi_sort = 1 then
      l_column_sort_asc  := pi_column_sort;
      l_column_sort_desc := null;
    else
      l_column_sort_asc  := null;
      l_column_sort_desc := pi_column_sort;
    end if;
    --
    select rec_num_4(org_id, dog_id, id, rownum) bulk collect
      into l_org_tab
      from (select org_id,
                   dog_id,
                   id,
                   org_name,
                   dog_number,
                   dog_date,
                   dog_class,
                   org_name_sort
              from (select distinct t.org_id,
                                    d.dog_id,
                                    rel.id,
                                    trim(t.org_name) org_name,
                                    trim(upper(t.org_name)) org_name_sort,
                                    d.dog_number,
                                    d.dog_date,
                                    dic.name dog_class
                      from t_organizations t
                      left join mv_org_tree rel
                        on rel.org_id = t.org_id
                       and rel.org_reltype in (1004, 999)
                      left join t_dogovor d
                        on d.org_rel_id = rel.id
                      left join t_dic_dogovor_class dic
                        on dic.id = d.dog_class_id
                     where (pi_org_id is null or t.org_id = pi_org_id)
                       and t.is_enabled = 1
                       and (pi_dog_number is null or
                           replace(REPLACE(upper(d.dog_number), '  '), ' ') like
                           upper(substr(replace(REPLACE(pi_dog_number, '  '),
                                                 ' '),
                                         1,
                                         3)) || '%')
                       and (pi_inn is null or t.org_inn = pi_inn)
                       and (pi_org_name is null or
                           replace(REPLACE(upper(t.org_name), '  '), ' ') like
                           upper(substr(replace(REPLACE(pi_org_name, '  '),
                                                 ' '),
                                         1,
                                         3)) || '%'))
             order by decode(l_column_sort_asc,
                             null,
                             to_char(0),
                             1,
                             org_id,
                             2,
                             dog_number,
                             3,
                             dog_date,
                             4,
                             org_name_sort,
                             5,
                             dog_class,
                             null) asc,
                      decode(l_column_sort_desc,
                             null,
                             to_char(0),
                             1,
                             org_id,
                             2,
                             dog_number,
                             3,
                             dog_date,
                             4,
                             org_name_sort,
                             5,
                             dog_class,
                             null) desc);

    po_count_all := l_org_tab.count;

    l_max_num_page := round(po_count_all / nvl(pi_count_rows, 1));

    if (pi_page_number > l_max_num_page and l_max_num_page <> 0) then
      l_num_page := l_max_num_page + 1;
    else
      l_num_page := nvl(pi_page_number, 1);
    end if;

    open res for
      select /*distinct*/
       org.org_id,
       org.org_name,
       d.dog_number,
       d.dog_date,
       m.name_mrf mrf_name,
       r.kl_name region_name,
       (case
         when d.is_enabled = 1 or d.dog_id is null then
          0
         else
          1
       end) is_block,
       m.org_id mrf_org_id,
       dic.name dog_class
        from (Select number_1 org_id,
                     number_2 dog_id,
                     number_3 id,
                     number_4 rn
                from table(l_org_tab)
               where number_4 between (l_num_page - 1) * pi_count_rows + 1 and
                     (l_num_page) * pi_count_rows) aa
        join t_organizations org
          on org.org_id = aa.org_id
        left join t_org_relations rel
          on rel.id = aa.id
        left join t_dogovor d
          on d.dog_id = aa.dog_id
        left join t_dic_region r
          on r.reg_id = org.region_id
        left join t_dic_mrf m
          on m.id = r.mrf_id
          or m.org_id = rel.org_pid
        left join t_dic_dogovor_class dic
          on dic.id = d.dog_class_id
       order by rn;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(pi_message  => getStrParam || po_err_msg,
                        pi_location => c_pr_name);
      return null;
  end;
  --------------------------------------------------------------------------------------
  -- выгрузка справочника по протоколу ЕЛК
  --------------------------------------------------------------------------------------
  function get_org_dic(pi_worker_id in number,
                       pi_org_id    in t_organizations.org_id%type,
                       pi_hst_id    in t_organizations_hst.hst_id%type,
                       pi_kl_region in t_dic_region.kl_region%type,
                       po_err_num   out number,
                       po_err_msg   out varchar2) return sys_refcursor is
    res       sys_refcursor;
    user_orgs num_tab;
  begin
    user_orgs := security_pkg.get_user_orgs_tab_by_right_str(pi_worker_id,
                                                             'SD.SELLER_ACTIVE.DIR_VIEW',
                                                             nvl(pi_org_id,
                                                                 1));
    if user_orgs is null or user_orgs.count = 0 then
      po_err_num := 1;
      po_err_msg := 'Нет доступа';
      return null;
    end if;
  
    open res for
      select o.org_id,
             o.org_name,
             max(h.hst_id) as hst_id,
             max(h.hst_date) keep(dense_rank last order by hst_id) as hst_date,
             1 as root_id,
             r.kl_region,
             resp.person_lastname leader_lastname,
             resp.person_firstname leader_firstname,
             resp.person_middlename leader_middlename,
             resp.person_phone leader_contactphone,
             t.person_lastname manager_lastname,
             t.person_firstname manager_firstname,
             t.person_middlename manager_middlename,
             t.person_phone manager_contactphone,
             (select cast(collect(d.dog_number) as string_tab)
                from mv_org_tree tree
                join t_dogovor d
                  on d.org_rel_id = tree.root_rel_id
                 and d.is_enabled = 1
               where tree.org_id = o.org_id) dog_list
        from t_organizations o
        left join t_organizations_hst h
          on h.org_id = o.org_id
        left join t_dic_region r
          on r.reg_id = o.region_id
        left join t_person t
          on t.person_id = o.touch_id
        left join t_person resp
          on resp.person_id = o.resp_id
       where o.org_id in (select * from table(user_orgs))
         and (pi_hst_id is null or h.hst_id >= pi_hst_id)
         and (pi_kl_region is null or r.kl_region = pi_kl_region)
         and (pi_org_id is null or pi_org_id = o.org_id)
       group by o.org_id,
                o.org_name,
                r.kl_region,
                t.person_lastname,
                t.person_firstname,
                t.person_middlename,
                t.person_phone,
                resp.person_lastname,
                resp.person_firstname,
                resp.person_middlename,
                resp.person_phone;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      if res%isopen then
        close res;
      end if;
      return null;
  end get_org_dic;

  --------------------------------------------------------------------------------------
  -- выгрузка справочника по протоколу ЕЛК
  --------------------------------------------------------------------------------------
  function get_org_hst(pi_hst_id  in t_organizations_hst.hst_id%type,
                       po_err_num out number,
                       po_err_msg out varchar2) return sys_refcursor is
    res sys_refcursor;
  begin
    open res for
      select h.org_id,
             h.org_name,
             h.hst_id,
             h.hst_date,
             1 as root_id,
             r.kl_region,
             resp.person_lastname leader_lastname,
             resp.person_firstname leader_firstname,
             resp.person_middlename leader_middlename,
             resp.person_phone leader_contactphone,
             t.person_lastname manager_lastname,
             t.person_firstname manager_firstname,
             t.person_middlename manager_middlename,
             t.person_phone manager_contactphone,
             cast(collect(td.dog_number) as string_tab) dog_list
        from t_organizations_hst h
        left join t_dic_region r
          on r.reg_id = h.region_id
        left join t_person t
          on t.person_id = h.touch_id
        left join t_person resp
          on resp.person_id = h.resp_id
        left join (select distinct tree.org_id, d.dog_number
                     from mv_org_tree tree
                     join t_dogovor d
                       on d.org_rel_id = tree.root_rel_id
                      and d.is_enabled = 1) td
          on td.org_id = h.org_id
       where h.hst_id = pi_hst_id      
       group by h.org_id,
                h.org_name,
                h.hst_id,
                h.hst_date,
                r.kl_region,
                t.person_lastname,
                t.person_firstname,
                t.person_middlename,
                t.person_phone,
                resp.person_lastname,
                resp.person_firstname,
                resp.person_middlename,
                resp.person_phone;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      if res%isopen then
        close res;
      end if;
      return null;
  end get_org_hst;

  -----------------------------------------------------------------------------
  --Сохранение текущего состояния организации в историю
  -----------------------------------------------------------------------------
  function save_org_hst(pi_worker_id in number, pi_org_id in number)
    return number is
    l_hst_id number;
  begin
    for org in (select * from t_organizations o where o.org_id = pi_org_id) loop
      insert into t_organizations_hst
        (org_id,
         org_type,
         org_name,
         adr1_id,
         adr2_id,
         resp_id,
         touch_id,
         org_settl_account,
         org_con_account,
         org_kpp,
         org_bik,
         org_okpo,
         org_okonx,
         region_id,
         org_ogrn,
         org_asr_sync,
         org_comment,
         email,
         prefix,
         root_org_id,
         regdate,
         v_lice,
         na_osnovanii,
         org_inn,
         new_org_id,
         root_org_id2,
         is_enabled,
         worker_id_create,
         change_date,
         worker_id_change,
         org_full_name,
         v_lice_podkl,
         na_osnovanii_podkl,
         is_pay_espp,
         hst_date,
         hst_user,
         is_stand,
         boss_name,
         type_org,
         is_stamp,
         is_with_rekv,
         is_with_ip,
         is_with_personal_info,
         start_date,
         use_child_req,
         org_buy,
         bank_name,
         rash_inn,
         rash_bik,
         rash_settl_account,
         rash_con_account,
         rash_bank_name,
         rash_okpo,
         rash_okonx,
         rash_kpp,
         is_ss_center,
         --UNRESERVED_TMC,
         erp_r12_num)
      values
        (org.org_id,
         org.org_type,
         org.org_name,
         org.adr1_id,
         org.adr2_id,
         org.resp_id,
         org.touch_id,
         org.org_settl_account,
         org.org_con_account,
         org.org_kpp,
         org.org_bik,
         org.org_okpo,
         org.org_okonx,
         org.region_id,
         org.org_ogrn,
         org.org_asr_sync,
         org.org_comment,
         org.email,
         org.prefix,
         org.root_org_id,
         org.regdate,
         org.v_lice,
         org.na_osnovanii,
         org.org_inn,
         org.new_org_id,
         org.root_org_id2,
         org.is_enabled,
         org.worker_id_create,
         org.change_date,
         org.worker_id_change,
         org.org_full_name,
         org.v_lice_podkl,
         org.na_osnovanii_podkl,
         org.is_pay_espp,
         sysdate,
         pi_worker_id,
         org.is_stand,
         org.boss_name,
         org.type_org,
         org.is_stamp,
         org.is_with_rekv,
         org.is_with_ip,
         org.is_with_personal_info,
         org.start_date,
         org.use_child_req,
         org.org_buy,
         org.bank_name,
         org.rash_inn,
         org.rash_bik,
         org.rash_settl_account,
         org.rash_con_account,
         org.rash_bank_name,
         org.rash_okpo,
         org.rash_okonx,
         org.rash_kpp,
         org.is_ss_center,
         --org.UNRESERVED_TMC,
         org.erp_r12_num)
      returning hst_id into l_hst_id;
    end loop;

    insert into T_ORG_SS_CENTER_HST
      select null, l_hst_id, t.*
        from T_ORG_SS_CENTER t
       where t.org_id = pi_org_id;

    insert into T_ORG_SSC_EMPLOYEE_HST
      select null, l_hst_id, t.*
        from T_ORG_SSC_EMPLOYEE t
       where t.org_id = pi_org_id;

    insert into T_ORG_SSC_TEMP_CLOSE_HST
      select null, l_hst_id, t.*
        from T_ORG_SSC_TEMP_CLOSE t
       where t.org_id = pi_org_id;

    insert into T_ORG_SSC_TIMETABLE_HST
      select null, l_hst_id, t.*
        from T_ORG_SSC_TIMETABLE t
       where t.org_id = pi_org_id;

    insert into T_ORG_SS_SERVICE_HST
      select null, l_hst_id, t.*
        from T_ORG_SS_SERVICE t
       where t.org_id = pi_org_id;

    insert into T_ORG_SSC_CONTACT_PHONE_HST
      select null, l_hst_id, t.*
        from T_ORG_SSC_CONTACT_PHONE t
       where t.org_id = pi_org_id;

    insert into T_ORG_SSC_PHONE_HST
      select null, l_hst_id, t.*
        from T_ORG_SSC_PHONE t
       where t.org_id = pi_org_id;

    return l_hst_id;
  end;
  -----------------------------------------------------------------------------
  -- Сохранение текущего состояния в историю и отправка задания на экспорт изменений
  -----------------------------------------------------------------------------
  function save_org_state(pi_worker_id in number, pi_org_id in number)
    return number is
    l_hst_id         number;
    res_kernel       number;
    l_err_num        number;
    l_err_msg        varchar2(4000);
    l_org_regions    num_tab;
    l_export_regions num_tab;
  begin
    l_hst_id := save_org_hst(pi_worker_id => pi_worker_id,
                             pi_org_id    => pi_org_id);

    -- задание в конвейер на отправку изменений в ИС МРФ
    l_org_regions := Get_Reg_by_Org(pi_org_id    => pi_org_id,
                                    pi_org_pid   => null,
                                    pi_worker_id => 777);
    if l_org_regions is null or l_org_regions.count = 0 then
      logging_pkg.debug('pi_hst_id :=' || pi_org_id ||
                        ' не найдены регионы для отправки',
                        'orgs.save_org_state');
      return l_hst_id;
    end if;

    l_export_regions := regions.get_regions_with_settings('dic_organization',
                                                          l_org_regions);
    if l_export_regions is null or l_export_regions.count = 0 then
      logging_pkg.debug('pi_hst_id :=' || pi_org_id ||
                        ' не найдены регионы для отправки',
                        'orgs.save_org_state');
      return l_hst_id;
    end if;

    for reg in (select r.kl_region
                  from t_dic_region r
                 where r.reg_id in (select * from table(l_export_regions))) loop
      res_kernel := cnv_order.set_to_conveyor(pi_front_end => Constant_pkg.c_dic_frontend,
                                              pi_task      => 'com.dart.eshpd.uniproto.dictionary.upload.CnvUploadRefOrgList',
                                              pi_param     => reg.kl_region || '_' ||
                                                              l_hst_id,
                                              pi_pool_id   => constant_pkg.c_dic_pool,
                                              pi_delay     => null,
                                              po_err_num   => l_err_num,
                                              po_err_msg   => l_err_msg);

      if l_err_num <> 0 then
        logging_pkg.error(l_err_num || ' ' || l_err_msg,
                          'orgs.save_org_state');
      end if;
    end loop;

    return l_hst_id;
  exception
    when others then
      l_err_num := sqlcode;
      l_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error('pi_org_id := ' || pi_org_id||';'||l_err_num||';'||l_err_msg, 'save_org_state');
      return null;
  end;

  --смена канала продаж у организации при редактировании договора
  procedure change_org_channel_dog(pi_org_id    in number,
                                   pi_worker_id in number,
                                   po_err_num   out number,
                                   po_err_msg   out varchar2) is
    l_func_name varchar2(100) := 'orgs.change_org_channel_dog';
    l_class_dog num_tab;
    l_is_m2m    number := 0;
  begin
    select DOG_CLASS_ID bulk collect
      into l_class_dog
      from t_org_relations r
      join t_dogovor d
        on d.org_rel_id = r.id
       and d.is_enabled = 1
     where r.org_id = pi_org_id
       and d.m2m_type = 0
       and DOG_CLASS_ID is not null;

    if l_class_dog is null then
      l_class_dog := num_tab();
    end if;

    select decode(cnt, 0, 0, 1)
      into l_is_m2m
      from (select count(*) cnt
              from t_org_relations r
              join t_dogovor d
                on d.org_rel_id = r.id
               and d.is_enabled = 1
             where r.org_id = pi_org_id
               and d.m2m_type > 0);

    if l_class_dog is not null and l_class_dog.count > 0 or l_is_m2m > 0 then
      delete from t_org_channels ch
       where ch.org_id = pi_org_id
         and ch.channel_id not in
             (select s.channel_id
                from t_dic_channels_setting s
               where s.param = 'contract_class'
                 and s.value in
                     (select t.column_value from table(l_class_dog) t))
         and ch.channel_id not in
             (select s.channel_id
                from t_dic_channels_setting s
               where s.param = 'm2m_contract_type'
                 and l_is_m2m = 1);

      insert into t_org_channels_hst
        select seq_ORG_CHANNELS_HST.nextval, pi_worker_id, sysdate, ch.*
          from t_org_channels ch
         where ch.org_id = pi_org_id;

      delete from t_org_channels ch
       where ch.org_id in (select r.org_id
                             from t_org_relations r
                           connect by r.org_pid = prior r.org_id
                            start with r.org_pid = pi_org_id)
         and ch.channel_id not in
             (select c.channel_id
                from t_org_channels c
               where c.org_id = pi_org_id);

      insert into t_org_channels_hst
        select seq_ORG_CHANNELS_HST.nextval, pi_worker_id, sysdate, ch.*
          from t_org_channels ch
         where ch.org_id in
               (select r.org_id
                  from t_org_relations r
                connect by r.org_pid = prior r.org_id
                 start with r.org_pid = pi_org_id);
    end if;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return;
  end;
  ------------------------------------------------------------------------
  -- Получение списка договоров содержащих переданый договорной пермишен.
  -- Малышевский ПредыдущаяЗадача № 31262
  ------------------------------------------------------------------------
  function get_list_dog_by_perm(pi_perm_id   in num_tab,
                                pi_org_id    in number,
                                pi_worker_id in number,
                                po_err_num   out pls_integer,
                                po_err_msg   out varchar2)
    return sys_refcursor is
    res sys_refcursor;
  begin
    logging_pkg.debug('pi_org_id=' || pi_org_id || ' pi_perm_id=' ||
                      get_str_by_num_tab(pi_perm_id) || ' pi_worker_id=' ||
                      pi_worker_id,
                      'get_list_dog_by_perm');
    open res for
      select distinct d.dog_id, d.dog_number
        from (select tor.id
                from t_org_relations tor
                join t_organizations o
                  on o.org_id = tor.org_id
                 and o.is_enabled = 1
              connect by prior tor.org_pid = tor.org_id
               start with tor.org_id = pi_org_id) t
        join t_dogovor d
          on d.org_rel_id = t.id
         and d.is_enabled = 1
        join t_dogovor_prm dp
          on dp.dp_dog_id = d.dog_id
         and dp.dp_is_enabled = 1
         and dp.dp_prm_id in (select column_value from table(pi_perm_id));
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      return null;
  end;

/**
 * @param pi_right таблица прав
 * @param pi_org_id ИД организации
 * @param pi_worker_id ИД работника
 * @param po_err_num номер ошибки в случае возникновения исключения
 * @param po_err_msg сообщение ошибки в случае возникновения исключения
 * @param pi_dog_is_enabled содержит статус договора.
 *         1 - Учитывать открытые договоры с организацией
 *         0 - Учитывать как открытые так и закрытые договоры с организацией
 */
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
    return sys_refcursor is
    res         sys_refcursor;
    l_func_name varchar2(120) := 'ORGS.get_dog_org_by_rigth';
  begin
    open res for
      select distinct d.dog_id, d.dog_number
        from t_organizations o
        join mv_org_tree tree
          on tree.org_id = o.org_id
        join t_dogovor d
          on d.org_rel_id = tree.root_rel_id
         and (d.is_enabled = 1 OR d.is_enabled = pi_dog_is_enabled)
        join t_dogovor_prm dp
          on dp.dp_dog_id = d.dog_id
         and (dp.dp_is_enabled = 1 OR dp.dp_is_enabled = pi_dog_is_enabled)
        join t_perm_rights pr
          on pr.pr_prm_id = dp.dp_prm_id
        join t_rights rr
          on rr.right_id = pr.pr_right_id
       where o.org_id = pi_org_id
         and o.is_enabled = 1
         and rr.right_string_id in (select * from table(pi_rigth))
         and is_org_usi(o.org_id) = 0;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;
  ------------------------------------------------------------------------
  function Get_Regions_for_mvno(pi_worker_id in number,
                                po_err_num   out number,
                                po_err_msg   out varchar2)
    return sys_refcursor is
    res         sys_refcursor;
    l_func_name varchar2(120) := 'ORGS.Get_Regions_for_mvno';
  begin
    open res for
      select t.id          reg_mvno,
             t.name        name_mvno,
             r.kl_region   reg_id,
             t.is_priority,
             t.is_reg_available,
             t.without_search_phys,
             t.without_search_jur
        from t_dic_mvno_region t
        join t_dic_region r
          on r.reg_id = t.reg_id;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;
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
    return sys_refcursor is
    res          sys_refcursor;
    l_func_name  varchar2(120) := c_package || 'Get_Orgs_structure';
    l_org_tab    num_tab;
    l_reg_org    ARRAY_NUM_2;
    User_reg_org ARRAY_NUM_2;
    l_org_num_2  array_num_2 := array_num_2();
  begin
    if pi_org_tab is not null then
      select rec_num_2(number_1 => column_value,
                       number_2 => pi_org_child_include) bulk collect
        into l_org_num_2
        from table(pi_org_tab);
    else
      l_org_num_2 := array_num_2(rec_num_2(1, nvl(pi_org_child_include, 1)));
    end if;

    l_org_tab    := get_orgs_tab_for_multiset(pi_orgs         => l_org_num_2,
                                              Pi_worker_id    => pi_worker_id,
                                              pi_block        => pi_block,
                                              pi_org_relation => pi_org_relation);
    l_reg_org    := security_pkg.get_region_by_worker_right2(pi_worker_id => pi_worker_id,
                                                             pi_right_str => string_tab('EISSD.REPORT.DISTANCE_SALES'),
                                                             pi_org_id    => l_org_tab);
    User_reg_org := intersect_num2(l_org_tab, l_reg_org);
    open res for
      select number_1 as org_id, number_2 as region_id
        from table(User_reg_org);
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || '.' || po_err_msg, l_func_name);
      return null;
  end;
  ------------------------------------------------------------------------
  --Справочник Кластеров ЦПО
  ------------------------------------------------------------------------
  function get_dic_ssc_cluster(po_err_num out number,
                               po_err_msg out varchar2) return sys_refcursor is
    res         sys_refcursor;
    l_func_name varchar2(100) := 'ORGS.get_dic_ssc_cluster';
  begin
    open res for
      select id, name from T_DIC_ORG_SSC_CLUSTER;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg, l_func_name);
      return null;
  end;

  ------------------------------------------------------------------------
  --Справочник Должностей сотрудников организаций
  ------------------------------------------------------------------------
  function get_dic_org_employee(po_err_num out number,
                                po_err_msg out varchar2) return sys_refcursor is
    res         sys_refcursor;
    l_func_name varchar2(100) := 'ORGS.get_dic_org_employee';
  begin
    open res for
      select id, name, IS_NEED_NAME, IS_NEED_PHONE from T_DIC_ORG_employee;
    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg, l_func_name);
      return null;
  end;

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
    return sys_refcursor is
    res         sys_refcursor;
    l_func_name varchar2(100) := 'ORGS.get_org_ssc_history_change';
    User_Orgs   num_tab;
    l_tab       num_tab;
    l_check_reg number;
    l_sort_tab  request_Order_Tab;

    l_order_asc    number; -- по возрастанияю
    l_order_desc   number; -- по убыванию
    l_max_num_page number;
    l_num_page     number;
    l_org_num_2    ARRAY_NUM_2;
  begin
    -- Проверка прав на редактирование ЦПО
    if (not Security_pkg.Check_User_Right_str('EISSD.CPO.VIEW.REPORT',
                                              pi_worker_id,
                                              po_err_num,
                                              po_err_msg)) then
      null;
    end if;

    If pi_sorting = 0 then
      l_order_asc := NVL(pi_column, 1);
    else
      l_order_desc := NVL(pi_column, 1);
    end If;

    if pi_org_id is not null then
      select rec_num_2(number_1 => column_value,
                       number_2 => pi_is_child_include) bulk collect
        into l_org_num_2
        from table(pi_org_id);

      l_tab := get_orgs_tab_for_multiset(pi_orgs         => l_org_num_2,
                                         pi_worker_id    => pi_worker_id,
                                         pi_block        => 1,
                                         pi_org_relation => null);
    end if;

    -- Получаем организации, доступные полльзователю

    User_Orgs := SECURITY_PKG.Get_User_Orgs_Tab_By_Right_str(pi_worker_id,
                                                             'EISSD.CPO.VIEW.REPORT',
                                                             1);

    User_Orgs := intersects(l_tab, User_Orgs);

    if pi_regions is not null and pi_regions is not empty then
      l_check_reg := 1;
    else
      l_check_reg := 0;
    end if;

    select request_Order_Type(hst_id, rownum) bulk collect
      into l_sort_tab
      from (select h.hst_id
              from t_organizations_hst h
              join table(User_Orgs) uo
                on uo.column_value = h.org_id
              join t_organizations o
                on o.org_id = h.org_id
              join t_org_ss_center_hst ch
                on ch.hst_org_id = h.hst_id
              left join t_dic_region r
                on r.reg_id = o.region_id
              left join t_dic_mrf m
                on m.id = r.mrf_id
             where h.hst_date >= pi_date_start
               and h.hst_date < pi_date_end
               and (l_check_reg = 0 or
                   r.kl_region in (select * from table(pi_regions)))
             order by decode(l_order_asc,
                             null,
                             null,
                             1,
                             o.org_id,
                             2,
                             m.name_mrf,
                             3,
                             r.kl_name,
                             4,
                             lpad(r.kl_region, 2, '0'),
                             5,
                             lower(o.org_name),
                             6,
                             h.hst_date,
                             null) asc,
                      decode(l_order_desc,
                             null,
                             null,
                             1,
                             o.org_id,
                             2,
                             m.name_mrf,
                             3,
                             r.kl_name,
                             4,
                             lpad(r.kl_region, 2, '0'),
                             5,
                             lower(o.org_name),
                             6,
                             h.hst_date,
                             null) desc,
                      (case
                        when pi_column between 1 and 5 then
                         h.hst_date
                        else
                         null
                      end) asc);

    po_all_count := l_sort_tab.count;

    l_max_num_page := round(po_all_count / nvl(pi_count_req, 1));

    if (pi_num_page > l_max_num_page and l_max_num_page <> 0) then
      l_num_page := l_max_num_page + 1;
    else
      l_num_page := nvl(pi_num_page, 1);
    end if;

    open res for
      select h.hst_id,
             o.org_id,
             r.kl_region,
             r.mrf_id,
             o.org_name,
             h.hst_date,
             u.usr_login,
             p.person_lastname,
             p.person_firstname,
             p.person_middlename,
             a.addr_index,
             a.addr_city,
             a.addr_street,
             a.addr_building,
             a.addr_office,
             ch.addr_fact_district,
             ch.full_name_ssc,
             h.org_name hist_org_name,
             h.erp_r12_num,
             ch.metro,
             ch.open_date,
             ch.close_date,
             ch.email,
             ch.segment_service,
             ch.is_electro_queue,
             ch.is_gold_pool,
             ch.cnt_term_agent,
             ch.cnt_term_rtc,
             (select cast(collect(c.phone) as string_tab)
                from t_org_ssc_contact_phone_hst c
               where c.org_id = o.org_id
                 and c.hst_org_id = h.hst_id) contact_phone,
             (select cast(collect(cc.phone_number) as string_tab)
                from t_org_ssc_phone_hst cc
               where cc.org_id = o.org_id
                 and cc.hst_org_id = h.hst_id) phone_for_portal,
             case
               when ch.close_date < h.hst_date then
                'Закрыт'
               else
                case
                  when h.hst_date between hh.date_start and hh.date_end then
                   'Временно закрыт'
                  else
                   'Открыт'
                end
             end as status,
             ch.priority,
             ch.latitude,
             ch.longitude,
             h.is_pay_espp,
             h.IS_STAMP,
             b_p.person_email resp_email,
             b_p.person_phone resp_phone,
             b_p.person_lastname resp_lastname,
             b_p.person_firstname resp_firstname,
             b_p.person_middlename resp_middlename,
             uo.org_name user_org_name
        from (Select request_id, rn
                from table(l_sort_tab)
               where rn between (l_num_page - 1) * pi_count_req + 1 and
                     (l_num_page) * pi_count_req) s_t
        join t_organizations_hst h
          on h.hst_id = s_t.request_id
        join t_organizations o
          on o.org_id = h.org_id
        join t_org_ss_center_hst ch
          on h.hst_id = ch.hst_org_id
        left join t_org_ssc_temp_close_hst hh
          on hh.org_id = o.org_id
         and hh.hst_org_id = h.hst_id
        left join t_dic_region r
          on r.reg_id = h.region_id
        join t_users u
          on u.usr_id = h.hst_user
        join t_organizations uo
          on uo.org_id = u.org_id
        left join t_person p
          on p.person_id = u.usr_person_id
        left join t_address a
          on a.addr_id = h.adr2_id
        left join t_person b_p
          on b_p.person_id = h.resp_id
       order by rn;

    open po_timetable for
      select t.hst_org_id,
             t.org_id,
             t.day_number,
             t.work_start,
             t.work_end,
             t.break_start,
             t.break_end,
             CASE
               WHEN t.WORK_START = c_work_start AND t.WORK_END = c_work_end THEN
                1
               ELSE
                0
             END AS fulltime,
             CASE
               WHEN t.BREAK_START IS NULL AND t.BREAK_END IS NULL THEN
                1
               ELSE
                0
             END AS without_break
        from (Select request_id, rn
                from table(l_sort_tab)
               where rn between (l_num_page - 1) * pi_count_req + 1 and
                     (l_num_page) * pi_count_req) s_t
        join t_org_ssc_timetable_hst t
          on t.hst_org_id = s_t.request_id
       where t.is_enabled = 1
       order by s_t.rn;

    open po_employee for
      select e.hst_org_id,
             e.org_id,
             e.type_employee,
             e.name,
             e.phone,
             de.name type_name
        from (Select request_id, rn
                from table(l_sort_tab)
               where rn between (l_num_page - 1) * pi_count_req + 1 and
                     (l_num_page) * pi_count_req) s_t
        join t_org_ssc_employee_hst e
          on s_t.request_id = e.hst_org_id
        join t_dic_org_employee de
          on de.id = e.type_employee
       order by s_t.rn;

    open po_services for
      select s.hst_org_id,
             s.org_id,
             s.ss_service,
             s.ss_service_pos,
             s.segment_service,
             ss.ss_service ss_service_name
        from (Select request_id, rn
                from table(l_sort_tab)
               where rn between (l_num_page - 1) * pi_count_req + 1 and
                     (l_num_page) * pi_count_req) s_t
        join t_org_ss_service_hst s
          on s.hst_org_id = s_t.request_id
        join t_dic_org_ss_service ss
          on ss.id = s.SS_SERVICE
         and ss.is_actual = 1
       order by s_t.rn;

    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg, l_func_name);
      return null;
  end;

  -------------------------------------------------
  --Получение списка организаций отображения при массовом редактировании ЦПО
  --------------------------------------------------
  function get_org_list_for_ssc_update(PI_ORG_ID       IN ARRAY_NUM_2,
                                       PI_BLOCK        IN NUMBER,
                                       PI_ORG_RELATION IN NUM_TAB,
                                       PI_WORKER_ID    IN NUMBER,
                                       PO_ERR_NUM      OUT PLS_INTEGER,
                                       PO_ERR_MSG      OUT VARCHAR2)
    return sys_refcursor is
    res            sys_refcursor;
    l_func_name    varchar2(100) := 'ORGS.get_org_list_for_ssc_update';
    l_org_tab      num_tab;
    l_user_org_tab num_tab;
  begin
    l_org_tab      := get_orgs_tab_for_multiset(pi_orgs         => pi_org_id,
                                                pi_worker_id    => pi_worker_id,
                                                pi_block        => pi_block,
                                                pi_org_relation => pi_org_relation);
    l_user_org_tab := get_user_orgs_by_prm(pi_worker_id => pi_worker_id,
                                           pi_rel_tab   => null,
                                           pi_prm_tab   => null,
                                           po_err_num   => po_err_num,
                                           po_err_msg   => po_err_msg);
    l_org_tab      := intersects(l_org_tab, l_user_org_tab);

    open res for
      select o.org_id, o.org_name, c.full_name_ssc
        from table(l_org_tab) t
        join t_organizations o
          on o.org_id = t.column_value
        join t_org_ss_center c
          on c.org_id = o.org_id;

    return res;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg, l_func_name);
      return null;
  end;

  ----------------------------------------------------------------
  --Получение регионов пользователя. перенесено из users
  ----------------------------------------------------------------
  function Get_Regions_User(pi_worker_id in number) return num_tab is
    res num_tab;
  begin
    select distinct t.reg_id bulk collect
      into res
      from t_dic_region t
      join (select tor.org_id
              from t_org_relations tor
            connect by prior tor.org_id = tor.org_pid
             start with tor.org_id in
                        (select min(tor.org_id) keep(dense_rank first order by level)
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
                          start with tor.org_id in
                                     (select tuo.org_id
                                        from t_user_org tuo
                                       where tuo.usr_id = pi_worker_id))) tab
        on tab.org_id = t.org_id;
    return res;
  end;

  -------------------------------------------------------------------
  --Определение является ли организация подчиненной
  -------------------------------------------------------------------
  function is_sub_org(pi_org_id        in number,
                      pi_parent_org_id in number,
                      po_err_num       out number,
                      po_err_msg       out varchar2) return number is
    res         number;
    l_func_name varchar2(100) := 'ORGS.is_sub_org';
  begin
    select count(*)
      into res
      from t_org_relations s
     where s.org_pid = pi_parent_org_id
       and s.org_id = pi_org_id;

    if res > 0 then
      return 1;
    else
      return 0;
    end if;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg, l_func_name);
      return null;
  end;

  -----------------------------------------------------------------------
  --Определение есть ли у организации указанный класс договора
  -----------------------------------------------------------------------
 function is_org_dog_class(pi_org_id       in number,
                           pi_dog_class_id in number,
                           po_err_num      out number,
                           po_err_msg      out varchar2) return number is
   res         number;
   l_func_name varchar2(100) := 'ORGS.is_sub_org';
 begin
   if is_org_usi(pi_org_id) = 1 then
     return 0;
   end if;

   select count(*)
     into res
     from (select min(dog_class_id) keep(dense_rank first order by lvl) dog_class_id
             from (select level lvl, d.dog_class_id --min(nvl(d.dog_class_id) keep(dense_rank first order by level ) doc
                     from t_org_relations r
                     left join t_dogovor d
                       on d.org_rel_id = r.id
                    where d.dog_class_id is not null
                   connect by r.org_id = prior r.org_pid
                    start with r.org_id = pi_org_id)) tt
    where tt.dog_class_id = pi_dog_class_id;

   if res > 0 then
     return 1;
   else
     return 0;
   end if;

 exception
   when others then
     po_err_num := sqlcode;
     po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
     logging_pkg.error(po_err_num || ' ' || po_err_msg, l_func_name);
     return null;
 end;

  ------------------------------------------------------------------
  -- Проверка, доступно ли редактирование договора
  ------------------------------------------------------------------
  procedure check_dog_m2m_type(pi_dog_id       in number,
                               pi_m2m_type     in number,
                               po_err_num      out number,
                               po_err_msg      out varchar2) is
  l_addr_sync number;
  l_cnt       number;
  begin
    if pi_dog_id is null then
      po_err_num := 1;
      po_err_msg := 'Не передан идентификатор договора';
      return;
    end if;

    if pi_m2m_type not in (1, 2) then
      po_err_num := 1;
      po_err_msg := 'Не передан тип адресной синхронизации';
      return;
    end if;

  -- Тип взаимодействия по m2m с агентом (0 - нет, 2 по типу связного, 1 по типу 2gis)
    if pi_m2m_type = 1 then
      l_addr_sync := 0;
    elsif pi_m2m_type = 2 then
      l_addr_sync := 1;
    end if;

    select count(1)
      into l_cnt
      from dual
     where exists(select null
                    from t_dogovor d
                    join t_org_relations tor on tor.id = d.org_rel_id
                    join t_m2m_agent_group_org o on o.org_id = tor.org_id
                    join t_m2m_agent_group g on g.id = o.group_id
                                            and g.addr_sync <> l_addr_sync
                                            and g.is_deleted = 0
                   where d.dog_id = pi_dog_id);

    if l_cnt > 0 then
      po_err_num := 1;
      po_err_msg := 'Невозможно сменить тип адресной синхронизации пока не удалены существующие продуктовые предложения';
      return;
    end if;

    po_err_num := 0;
  exception
    when others then
      po_err_num := sqlcode;
      po_err_msg := sqlerrm || ' ' || dbms_utility.format_error_backtrace;
      logging_pkg.error(po_err_num || ' ' || po_err_msg, 'agent_m2m.check_dogovor');
  end;

  FUNCTION get_all_available_mvno_regions RETURN SYS_REFCURSOR IS
    v_result SYS_REFCURSOR;
  BEGIN
    OPEN v_result FOR
      SELECT mr.*, r.kl_region
      FROM t_dic_mvno_region mr
      JOIN t_dic_region r ON r.reg_id = mr.reg_id
      WHERE is_reg_available = 1;
    RETURN v_result;
  END;

end ORGS;
/
