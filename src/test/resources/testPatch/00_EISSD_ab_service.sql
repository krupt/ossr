-- Тип связи (адсл,...) ссылка на t_dic_values.dv_type_id=6
COMMENT ON COLUMN t_dic_ab_service.connection_type
  IS 'Тип связи (адсл,...) ссылка на t_dic_product_category';



INSERT INTO t_dic_ab_service
  (id_req, type_req, description, connection_type, client_type)
VALUES
  (258, 'Перемещение услуг между ЛС / Переоформление услуг', 'Перемещение услуг между ЛС / Переоформление услуг', 1, 'P');

INSERT INTO t_dic_ab_service
  (id_req, type_req, description, connection_type, client_type)
VALUES
  (259, 'Перемещение услуг между ЛС / Переоформление услуг', 'Перемещение услуг между ЛС / Переоформление услуг', 2, 'P');

INSERT INTO t_dic_ab_service
  (id_req, type_req, description, connection_type, client_type)
VALUES
  (260, 'Перемещение услуг между ЛС / Переоформление услуг', 'Перемещение услуг между ЛС / Переоформление услуг', 5, 'P');



DECLARE
  v_option_id NUMBER;
BEGIN
  -- Adding new option
  INSERT INTO t_dic_ab_options
    (code, action, NAME, is_actual, date_create, worker_create)
  VALUES
    ('258_transfer_1', 1, 'Перемещение услуг между ЛС', 1, SYSDATE, 91951)
  RETURNING id INTO v_option_id;

  INSERT INTO t_dic_ab_serv_opt
    (id_serv, id_option)
  VALUES
    (258, v_option_id);


  /*
   * Adding new option
   */
  INSERT INTO t_dic_ab_options
    (code, action, NAME, is_actual, date_create, worker_create)
  VALUES
    ('259_transfer_2', 1, 'Перемещение услуг между ЛС', 1, SYSDATE, 91951)
  RETURNING id INTO v_option_id;

  INSERT INTO t_dic_ab_serv_opt
    (id_serv, id_option)
  VALUES
    (259, v_option_id);


  INSERT INTO t_dic_ab_options
    (code, action, NAME, is_actual, date_create, worker_create)
  VALUES
    ('260_transfer_5', 1, 'Перемещение услуг между ЛС', 1, SYSDATE, 91951)
  RETURNING id INTO v_option_id;

  INSERT INTO t_dic_ab_serv_opt
    (id_serv, id_option)
  VALUES
    (260, v_option_id);



  INSERT INTO t_dic_ab_options
    (code, action, NAME, is_actual, date_create, worker_create)
  VALUES
    ('258_reissuance_1', 1, 'Переоформление услуг', 1, SYSDATE, 91951)
  RETURNING id INTO v_option_id;

  INSERT INTO t_dic_ab_serv_opt
    (id_serv, id_option)
  VALUES
    (258, v_option_id);


  INSERT INTO t_dic_ab_options
    (code, action, NAME, is_actual, date_create, worker_create)
  VALUES
    ('259_reissuance_2', 1, 'Переоформление услуг', 1, SYSDATE, 91951)
  RETURNING id INTO v_option_id;

  INSERT INTO t_dic_ab_serv_opt
    (id_serv, id_option)
  VALUES
    (259, v_option_id);


  INSERT INTO t_dic_ab_options
    (code, action, NAME, is_actual, date_create, worker_create)
  VALUES
    ('260_reissuance_5', 1, 'Переоформление услуг', 1, SYSDATE, 91951)
  RETURNING id INTO v_option_id;

  INSERT INTO t_dic_ab_serv_opt
    (id_serv, id_option)
  VALUES
    (260, v_option_id);
END;
/



INSERT INTO t_org_ab_rights
  (org_id, id_req)
VALUES
  (0, 258);

INSERT INTO t_org_ab_rights
  (org_id, id_req)
VALUES
  (0, 259);

INSERT INTO t_org_ab_rights
  (org_id, id_req)
VALUES
  (0, 260);


-- Adding rights;
INSERT INTO t_org_ab_rights
  (org_id, id_req)
VALUES
  (1, 258);

INSERT INTO t_org_ab_rights
  (org_id, id_req)
VALUES
  (1, 259);

INSERT INTO t_org_ab_rights
  (org_id, id_req)
VALUES
  (1, 260);



INSERT INTO t_ab_option_avail /*
                               * Adding options;
                               */
  (id_option, id_mrf, action, date_start, date_end, id_region)
  SELECT id_option, mrf_id, CASE WHEN o.code LIKE '%$_transfer$_%' ESCAPE '$' THEN 1 ELSE 0 END, '17.04.2017', '31.12.2999', reg_id
  FROM /* Option mapping */t_dic_ab_serv_opt so
  JOIN t_dic_ab_options o ON o.id = so.id_option
  CROSS JOIN t_dic_region
  WHERE id_serv IN (258, 259, 260) /*
                                    * Specific option ids;
                                    */
        AND mrf_id = 7;/* End comment; */



INSERT INTO t_ab_option_avail
  (id_option, id_mrf, action, date_start, date_end, id_region)
  SELECT id_option, mrf_id, CASE WHEN o.code LIKE '%$_transfer$_%' ESCAPE '$' THEN 1 ELSE 0 END, '17.04.2017', '31.12.2999', reg_id
  FROM /* Option mapping; */t_dic_ab_serv_opt so
  JOIN t_dic_ab_options o ON o.id = so.id_option
  CROSS JOIN t_dic_region
  WHERE id_serv IN (258, 259, 260) /*;
       Only Ural;
        ;*/ AND mrf_id = 7;

/*INSERT INTO t_ab_option_avail
  (id_option, id_mrf, action, date_start, date_end, id_region)
  SELECT id_option, mrf_id, CASE WHEN o.code LIKE '%$_transfer$_%' ESCAPE '$' THEN 1 ELSE 0 END, '17.04.2017', '31.12.2999', reg_id
  FROM t_dic_ab_serv_opt so
  JOIN t_dic_ab_options o ON o.id = so.id_option
  CROSS JOIN t_dic_region
  WHERE id_serv IN (258, 259, 260)
        AND mrf_id = 7;*/



/*INSERT INTO t_ab_option_avail
  (id_option, id_mrf, action, date_start, date_end, id_region)
  SELECT id_option, mrf_id, CASE WHEN o.code LIKE '%$_transfer$_%' ESCAPE '$' THEN 1 ELSE 0 END, '17.04.2017', '31.12.2999', reg_id
  FROM t_dic_ab_serv_opt so
  JOIN t_dic_ab_options o ON o.id = so.id_option
  CROSS JOIN t_dic_region
  WHERE id_serv IN (258, 259, 260)
        AND mrf_id = 7;
*/



/*INSERT INTO t_ab_option_avail
  (id_option, id_mrf, action, date_start, date_end, id_region)
  SELECT id_option, mrf_id, CASE WHEN o.code LIKE '%$_transfer$_%' ESCAPE '$' THEN 1 ELSE 0 END, '17.04.2017', '31.12.2999', reg_id
  FROM t_dic_ab_serv_opt so
  JOIN t_dic_ab_options o ON o.id = so.id_option
  CROSS JOIN t_dic_region
  WHERE id_serv IN (258, 259, 260)
        AND mrf_id = 7;
*/
