INSERT INTO t_dic_sim_imaging (id, description, visibility)
  VALUES (1, 'Prepaid B2C', 1);
INSERT INTO t_dic_sim_imaging (id, description, visibility)
  VALUES (2, 'Postpaid B2B', 1);
INSERT INTO t_dic_sim_imaging (id, description, visibility)
  VALUES (3, 'Postpaid B2C', 1);
INSERT INTO t_dic_sim_imaging (id, description, visibility)
  VALUES (4, 'Change', 1);
INSERT INTO t_dic_sim_imaging (id, description, visibility, mapping_id)
  VALUES (5, 'Change B2C', 0, 4);
INSERT INTO t_dic_sim_imaging (id, description, visibility, mapping_id)
  VALUES (6, 'Change B2B', 0, 4);

INSERT INTO T_DIC_SIM_EDIT_OPER_TYPE (id, description)
  VALUES (1, 'Добавление');
INSERT INTO T_DIC_SIM_EDIT_OPER_TYPE (id, description)
  VALUES (2, 'Изменение');

INSERT INTO T_DIC_SIM_DATA_SOURCE (id, description)
  VALUES (1, 'Диапазон');
INSERT INTO T_DIC_SIM_DATA_SOURCE (id, description)
  VALUES (2, 'Список');
INSERT INTO T_DIC_SIM_DATA_SOURCE (id, description)
  VALUES (3, 'Файл/архив(входной формат)');
INSERT INTO T_DIC_SIM_DATA_SOURCE (id, description)
  VALUES (4, 'Файл/архив(выходной формат)');