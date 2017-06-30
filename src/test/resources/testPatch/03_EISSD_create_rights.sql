INSERT INTO t_rights(right_id, right_name, right_maskpos, right_readonly, right_string_id)
VALUES(9917, 'Загрузка графических профилей для SIM-карт', 560, 0, 'SD.SIM.IMAGING.EDIT');

INSERT INTO t_perm(prm_id, prm_name, prm_type)
VALUES(9917, 'Загрузка графических профилей для SIM-карт', 8501);

INSERT INTO t_perm_rights(pr_prm_id, pr_right_id)
VALUES(9917, 9917);