/*select * from t_dic_order_type
  13  Замена реквизитов физического лица
   10111  Изменение аттрибутов клиента
10110  Изменение аттрибутов клиента для основного устройства

select order_type,count(*) from t_orders o where o.order_type in (13,10111,10110)
group by order_type*/

delete t_dic_order_type t  where t.type_id=13;
delete t_dic_order_type t where t.type_id=10110;
update t_dic_order_type t set t.type_name ='Замена реквизитов физического лица' where t.type_id=10111;
update t_orders o set o.order_type =10111 where o.order_type in (13,10110);
