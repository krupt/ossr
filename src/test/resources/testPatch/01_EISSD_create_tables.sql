-- Create table
create table T_DIC_SIM_EDIT_OPER_TYPE
(
  id          NUMBER not null,
  description VARCHAR2(32) not null
);
-- Add comments to the table 
comment on table T_DIC_SIM_EDIT_OPER_TYPE
  is 'Справочник операций, доступных для работы с графическими профилями SIM-карт';
-- Add comments to the columns 
comment on column T_DIC_SIM_EDIT_OPER_TYPE.id
  is 'Идентификатор типа операции';
comment on column T_DIC_SIM_EDIT_OPER_TYPE.description
  is 'Тип операции';
-- Create/Recreate primary, unique and foreign key constraints 
alter table T_DIC_SIM_EDIT_OPER_TYPE
  add constraint PK_DIC_SIM_IMAGING_OP_TYPE_ID primary key (ID)
  using index;

-- Create table
create table T_DIC_SIM_IMAGING
(
  id          NUMBER not null,
  description VARCHAR2(32) not null,
  visibility  NUMBER(1) not null,
  mapping_id  NUMBER
);
-- Add comments to the table
comment on table T_DIC_SIM_IMAGING
is 'Справочник графических профилей для SIM-карт';
-- Add comments to the columns
comment on column T_DIC_SIM_IMAGING.id
is 'Идентификатор графического профиля';
comment on column T_DIC_SIM_IMAGING.description
is 'Графический профиль';
comment on column T_DIC_SIM_IMAGING.visibility
is 'Признак необходимости отображения графического профиля в выпадающих списках на вебе (1 - отображать, 0 - нет)';
comment on column T_DIC_SIM_IMAGING.mapping_id
is 'ID графического профиля для маппинга (если не NULL)';
-- Create/Recreate primary, unique and foreign key constraints
alter table T_DIC_SIM_IMAGING
  add constraint PK_DIC_SIM_IMAGING_ID primary key (ID);
alter table T_DIC_SIM_IMAGING
  add constraint FK_DIC_SIM_IMAGING_MAP_ID_ID foreign key (MAPPING_ID)
references T_DIC_SIM_IMAGING (ID);

-- Create table
create table T_DIC_SIM_DATA_SOURCE
(
  id          NUMBER not null,
  description VARCHAR2(100) not null
);
-- Add comments to the table
comment on table T_DIC_SIM_DATA_SOURCE
is 'Справочник источников данных при загрузке графических профилей для SIM-карт';
-- Add comments to the columns
comment on column T_DIC_SIM_DATA_SOURCE.id
is 'Идентификатор источника данных';
comment on column T_DIC_SIM_DATA_SOURCE.description
is 'Описание источника данных';
-- Create/Recreate primary, unique and foreign key constraints
alter table T_DIC_SIM_DATA_SOURCE
  add constraint PK_DIC_SIM_DATA_SOURCE_ID primary key (ID);

-- Create table
create table T_SIM_BY_IMAGING_TYPE
(
  id               NUMBER generated always as identity,
  imsi_range_start NUMBER(15) not null,
  imsi_range_end   NUMBER(15) not null,
  sim_imaging_type NUMBER not null
);
-- Add comments to the table 
comment on table T_SIM_BY_IMAGING_TYPE
  is 'Таблица соответствия SIM-карт и графических профилей';
-- Add comments to the columns 
comment on column T_SIM_BY_IMAGING_TYPE.id
  is 'Идентификатор диапазона';
comment on column T_SIM_BY_IMAGING_TYPE.imsi_range_start
  is 'Начальное значение диапазона';
comment on column T_SIM_BY_IMAGING_TYPE.imsi_range_end
  is 'Конечное значение диапазона';
comment on column T_SIM_BY_IMAGING_TYPE.sim_imaging_type
  is 'Графический профиль SIM-карт из диапазона';
-- Create/Recreate primary, unique and foreign key constraints 
alter table T_SIM_BY_IMAGING_TYPE
  add constraint PK_SIM_IMG_BY_TYPE_ID primary key (ID)
  using index;
alter table T_SIM_BY_IMAGING_TYPE
  add constraint UK_IMSI_RANGE_END unique (IMSI_RANGE_END)
  using index;
alter table T_SIM_BY_IMAGING_TYPE
  add constraint UK_IMSI_RANGE_START unique (IMSI_RANGE_START)
  using index;
alter table T_SIM_BY_IMAGING_TYPE
  add constraint FK_SIM_IMAGING_TYPE foreign key (SIM_IMAGING_TYPE)
  references T_DIC_SIM_IMAGING (ID);

-- Create table
create table T_SIM_BY_IMAGING_TYPE_HST
(
  id               NUMBER generated always as identity,
  imsi_range_start NUMBER(15) not null,
  imsi_range_end   NUMBER(15) not null,
  sim_imaging_type NUMBER not null,
  worker_id        NUMBER not null,
  change_date      DATE not null,
  operation_type   NUMBER not null
);
-- Add comments to the table 
comment on table T_SIM_BY_IMAGING_TYPE_HST
  is 'История добавления и изменения графических профилей по диапазонам SIM-карт';
-- Add comments to the columns 
comment on column T_SIM_BY_IMAGING_TYPE_HST.id
  is 'Идентификатор исторической записи';
comment on column T_SIM_BY_IMAGING_TYPE_HST.imsi_range_start
  is 'Начальное значение входного диапазона';
comment on column T_SIM_BY_IMAGING_TYPE_HST.imsi_range_end
  is 'Конечное значение входного диапазона';
comment on column T_SIM_BY_IMAGING_TYPE_HST.sim_imaging_type
  is 'Графический профиль входного диапазона';
comment on column T_SIM_BY_IMAGING_TYPE_HST.worker_id
  is 'Идентификатор сотрудника, внесшего изменения';
comment on column T_SIM_BY_IMAGING_TYPE_HST.change_date
  is 'Дата внесения изменения';
comment on column T_SIM_BY_IMAGING_TYPE_HST.operation_type
  is 'Тип операции (1 - создание, 2 - изменение)';
-- Create/Recreate indexes 
create index I_SIM_BY_IMG_T_HST_CH_DATE on T_SIM_BY_IMAGING_TYPE_HST (CHANGE_DATE);
-- Create/Recreate primary, unique and foreign key constraints 
alter table T_SIM_BY_IMAGING_TYPE_HST
  add constraint PK_SIM_BY_IMAGING_TYPE_HST_ID primary key (ID)
  using index;
alter table T_SIM_BY_IMAGING_TYPE_HST
  add constraint FK_SIM_BY_IMG_T_HST_IMG_TYPE foreign key (SIM_IMAGING_TYPE)
  references T_DIC_SIM_IMAGING (ID);
alter table T_SIM_BY_IMAGING_TYPE_HST
  add constraint FK_SIM_BY_IMG_T_HST_OPER_TYPE foreign key (OPERATION_TYPE)
  references T_DIC_SIM_EDIT_OPER_TYPE (ID);
alter table T_SIM_BY_IMAGING_TYPE_HST
  add constraint FK_SIM_BY_IMG_T_HST_WORKER_ID foreign key (WORKER_ID)
  references T_USERS (USR_ID);

-- Create table
create table T_SIM_BY_IMG_TYPE_HST_FULL
(
  id                   NUMBER generated always as identity,
  hst_id               NUMBER not null,
  old_id               NUMBER,
  old_imsi_range_start NUMBER(15),
  old_imsi_range_end   NUMBER(15),
  old_sim_imaging_type NUMBER,
  new_imsi_range_start NUMBER(15),
  new_imsi_range_end   NUMBER(15),
  new_sim_imaging_type NUMBER,
  change_date          DATE not null,
  worker_id            NUMBER not null,
  data_source          NUMBER not null,
  filename             VARCHAR2(500)
);
-- Add comments to the table 
comment on table T_SIM_BY_IMG_TYPE_HST_FULL
  is 'Полная история добавления и изменения графических профилей по диапазонам сим-карт';
-- Add comments to the columns 
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.id
  is 'Идентификатор расширенной исторической записи';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.hst_id
  is 'Идентификатор простой исторической записи';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.old_imsi_range_start
  is 'Начальное значение изменённого диапазона (при операции добавления = NULL)';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.old_imsi_range_end
  is 'Конечное значение изменённого диапазона (при операции добавления = NULL)';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.old_sim_imaging_type
  is 'Графический профиль сим-карт изменённого диапазона (при операции добавления = NULL)';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.new_imsi_range_start
  is 'Начальное значение входящего диапазона';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.new_imsi_range_end
  is 'Конечное значение входящего диапазона';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.new_sim_imaging_type
  is 'Графический профиль сим-карт входящего диапазона';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.change_date
  is 'Дата внесения изменения';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.worker_id
  is 'Сотрудник, внесший изменение';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.data_source
  is 'Источник загрузки данных (1 - диапазон, 2 - список, 3 - файл/архив входного формата, 4 - файл/архив выходного формата)';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.filename
  is 'Имя файла, из которого произведена загрузка (для источников 3 и 4)';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.old_id
  is 'Идентификатор изменённого диапазона в основной таблице (при операции добавления = NULL)';
-- Create/Recreate indexes 
create index I_FK_SIM_BY_IMG_T_HST_CH_DATE on T_SIM_BY_IMG_TYPE_HST_FULL (CHANGE_DATE);
-- Create/Recreate primary, unique and foreign key constraints 
alter table T_SIM_BY_IMG_TYPE_HST_FULL
  add constraint PK_SIM_BY_IMG_TYPE_HST_FULL_ID primary key (ID)
  using index;
alter table T_SIM_BY_IMG_TYPE_HST_FULL
  add constraint FK_SIM_BY_IMG_T_DATA_SC_ID foreign key (DATA_SOURCE)
  references T_DIC_SIM_DATA_SOURCE (ID);
alter table T_SIM_BY_IMG_TYPE_HST_FULL
  add constraint FK_SIM_BY_IMG_T_HST_HST_ID foreign key (HST_ID)
  references T_SIM_BY_IMAGING_TYPE_HST (ID);
alter table T_SIM_BY_IMG_TYPE_HST_FULL
  add constraint FK_SIM_BY_IMG_T_HST_NEW_IMG_T foreign key (NEW_SIM_IMAGING_TYPE)
  references T_DIC_SIM_IMAGING (ID);
alter table T_SIM_BY_IMG_TYPE_HST_FULL
  add constraint FK_SIM_BY_IMG_T_HST_OLD_IMG_T foreign key (OLD_SIM_IMAGING_TYPE)
  references T_DIC_SIM_IMAGING (ID);
alter table T_SIM_BY_IMG_TYPE_HST_FULL
  add constraint FK_SIM_BY_IMG_T_HST_WR_ID foreign key (WORKER_ID)
  references T_USERS (USR_ID);