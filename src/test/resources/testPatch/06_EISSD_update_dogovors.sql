UPDATE t_dogovor
SET is_employee_number_required = 1
WHERE org_rel_id IN (SELECT id
                     FROM t_org_relations
                     WHERE org_id IN (SELECT org_id
                                      FROM t_org_relations
                                      WHERE org_pid = 3)
                           AND org_reltype = 999);



