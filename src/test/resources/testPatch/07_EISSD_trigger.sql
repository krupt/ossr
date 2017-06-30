create or replace trigger TRG_CHANGE_DIC_SIM_COLOR_MASK
  before insert or update or delete on T_DIC_SIM_COLOR_MASK
  for each row
--триггер для логирования изменений справочника категорий цветности
begin
  if inserting then
    --добавиление
    if :new.id is null then
      :new.id := seq_DIC_SIM_COLOR_MASK.Nextval;
    end if;
    if :new.date_start is null then 
      :new.date_start:=sysdate;
    end if;  

    insert into T_SIM_COLOR_MASK_HST
      (HST_ID,
       HST_DATE,
       OS_USER,
       USER_HOST,
       ID,
       COLOR_ID,
       MASK_TEXT,
       COMMENT_TEXT,
       DATE_START,
       DATE_END)
    values
      (seq_SIM_COLOR_MASK_HST.Nextval,
       sysdate,
       upper(sys_context('USERENV', 'OS_USER')),
       upper(sys_context('USERENV', 'HOST')),
       :new.id,
       :new.COLOR_ID,
       :new.MASK_TEXT,
       :new.COMMENT_TEXT,
       :new.DATE_START,
       :new.DATE_END);
  elsif updating then
    --обновление   
    if :new.date_start is null then 
      :new.date_start:=sysdate;
    end if;
    
    if :new.id!=:old.id then 
      --если поменялся первичный ключ запишем что его удалили
      insert into T_SIM_COLOR_MASK_HST
      (HST_ID,
       HST_DATE,
       OS_USER,
       USER_HOST,
       ID,
       COLOR_ID,
       MASK_TEXT,
       COMMENT_TEXT,
       DATE_START,
       DATE_END)
    values
      (seq_SIM_COLOR_MASK_HST.Nextval,
       sysdate,
       upper(sys_context('USERENV', 'OS_USER')),
       upper(sys_context('USERENV', 'HOST')),
       :old.id,
       :old.COLOR_ID,
       null,
       null,
       null,
       null);
    end if;   
    
    insert into T_SIM_COLOR_MASK_HST
      (HST_ID,
       HST_DATE,
       OS_USER,
       USER_HOST,
       ID,
       COLOR_ID,
       MASK_TEXT,
       COMMENT_TEXT,
       DATE_START,
       DATE_END)
    values
      (seq_SIM_COLOR_MASK_HST.Nextval,
       sysdate,
       upper(sys_context('USERENV', 'OS_USER')),
       upper(sys_context('USERENV', 'HOST')),
       :new.id,
       :new.COLOR_ID,
       :new.MASK_TEXT,
       :new.COMMENT_TEXT,
       :new.DATE_START,
       :new.DATE_END);

  elsif deleting then
    --удаление
    insert into T_SIM_COLOR_MASK_HST
      (HST_ID,
       HST_DATE,
       OS_USER,
       USER_HOST,
       ID,
       COLOR_ID,
       MASK_TEXT,
       COMMENT_TEXT,
       DATE_START,
       DATE_END)
    values
      (seq_SIM_COLOR_MASK_HST.Nextval,
       sysdate,
       upper(sys_context('USERENV', 'OS_USER')),
       upper(sys_context('USERENV', 'HOST')),
       :old.id,
       :old.COLOR_ID,
       null,
       null,
       null,
       null);
  end if;
end;
/

create or replace trigger TRG_CHANGE_DIC_SIM_COLOR
  before insert or update or delete on T_DIC_SIM_COLOR
  for each row
--триггер для логирования изменений справочника категорий цветности
begin
  if inserting then
    --добавиление
    insert into T_SIM_COLOR_HST
      (HST_ID,
       HST_DATE,
       OS_USER,
       USER_HOST,
       ID,
       PS_ID,
       NAME,
       IS_ACTUAL,
       DATE_CHANGE,
       PRIOR_ID)
    values
      (seq_SIM_COLOR_HST.Nextval,
       sysdate,
       upper(sys_context('USERENV', 'OS_USER')),
       upper(sys_context('USERENV', 'HOST')),
       :new.id,
       :new.PS_ID,
       :new.NAME,
       :new.IS_ACTUAL,
       :new.DATE_CHANGE,
       :new.PRIOR_ID);
  elsif updating then
    --обновление
    :new.DATE_CHANGE := sysdate;

    insert into T_SIM_COLOR_HST
      (HST_ID,
       HST_DATE,
       OS_USER,
       USER_HOST,
       ID,
       PS_ID,
       NAME,
       IS_ACTUAL,
       DATE_CHANGE,
       PRIOR_ID)
    values
      (seq_SIM_COLOR_HST.Nextval,
       sysdate,
       upper(sys_context('USERENV', 'OS_USER')),
       upper(sys_context('USERENV', 'HOST')),
       :new.id,
       :new.PS_ID,
       :new.NAME,
       :new.IS_ACTUAL,
       :new.DATE_CHANGE,
       :new.PRIOR_ID);

  elsif deleting then
    --удаление
    insert into T_SIM_COLOR_HST
      (HST_ID,
       HST_DATE,
       OS_USER,
       USER_HOST,
       ID,
       PS_ID,
       NAME,
       IS_ACTUAL,
       DATE_CHANGE,
       PRIOR_ID)
    values
      (seq_SIM_COLOR_HST.Nextval,
       sysdate,
       upper(sys_context('USERENV', 'OS_USER')),
       upper(sys_context('USERENV', 'HOST')),
       :old.id,
       null,
       null,
       0,
       null,
       null);
  end if;
end;
                                          /                                                              