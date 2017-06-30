-- Create table
create table T_DIC_SIM_EDIT_OPER_TYPE
(
  id          NUMBER not null,
  description VARCHAR2(32) not null
);
-- Add comments to the table 
comment on table T_DIC_SIM_EDIT_OPER_TYPE
  is '���������� ��������, ��������� ��� ������ � ������������ ��������� SIM-����';
-- Add comments to the columns 
comment on column T_DIC_SIM_EDIT_OPER_TYPE.id
  is '������������� ���� ��������';
comment on column T_DIC_SIM_EDIT_OPER_TYPE.description
  is '��� ��������';
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
is '���������� ����������� �������� ��� SIM-����';
-- Add comments to the columns
comment on column T_DIC_SIM_IMAGING.id
is '������������� ������������ �������';
comment on column T_DIC_SIM_IMAGING.description
is '����������� �������';
comment on column T_DIC_SIM_IMAGING.visibility
is '������� ������������� ����������� ������������ ������� � ���������� ������� �� ���� (1 - ����������, 0 - ���)';
comment on column T_DIC_SIM_IMAGING.mapping_id
is 'ID ������������ ������� ��� �������� (���� �� NULL)';
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
is '���������� ���������� ������ ��� �������� ����������� �������� ��� SIM-����';
-- Add comments to the columns
comment on column T_DIC_SIM_DATA_SOURCE.id
is '������������� ��������� ������';
comment on column T_DIC_SIM_DATA_SOURCE.description
is '�������� ��������� ������';
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
  is '������� ������������ SIM-���� � ����������� ��������';
-- Add comments to the columns 
comment on column T_SIM_BY_IMAGING_TYPE.id
  is '������������� ���������';
comment on column T_SIM_BY_IMAGING_TYPE.imsi_range_start
  is '��������� �������� ���������';
comment on column T_SIM_BY_IMAGING_TYPE.imsi_range_end
  is '�������� �������� ���������';
comment on column T_SIM_BY_IMAGING_TYPE.sim_imaging_type
  is '����������� ������� SIM-���� �� ���������';
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
  is '������� ���������� � ��������� ����������� �������� �� ���������� SIM-����';
-- Add comments to the columns 
comment on column T_SIM_BY_IMAGING_TYPE_HST.id
  is '������������� ������������ ������';
comment on column T_SIM_BY_IMAGING_TYPE_HST.imsi_range_start
  is '��������� �������� �������� ���������';
comment on column T_SIM_BY_IMAGING_TYPE_HST.imsi_range_end
  is '�������� �������� �������� ���������';
comment on column T_SIM_BY_IMAGING_TYPE_HST.sim_imaging_type
  is '����������� ������� �������� ���������';
comment on column T_SIM_BY_IMAGING_TYPE_HST.worker_id
  is '������������� ����������, �������� ���������';
comment on column T_SIM_BY_IMAGING_TYPE_HST.change_date
  is '���� �������� ���������';
comment on column T_SIM_BY_IMAGING_TYPE_HST.operation_type
  is '��� �������� (1 - ��������, 2 - ���������)';
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
  is '������ ������� ���������� � ��������� ����������� �������� �� ���������� ���-����';
-- Add comments to the columns 
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.id
  is '������������� ����������� ������������ ������';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.hst_id
  is '������������� ������� ������������ ������';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.old_imsi_range_start
  is '��������� �������� ���������� ��������� (��� �������� ���������� = NULL)';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.old_imsi_range_end
  is '�������� �������� ���������� ��������� (��� �������� ���������� = NULL)';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.old_sim_imaging_type
  is '����������� ������� ���-���� ���������� ��������� (��� �������� ���������� = NULL)';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.new_imsi_range_start
  is '��������� �������� ��������� ���������';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.new_imsi_range_end
  is '�������� �������� ��������� ���������';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.new_sim_imaging_type
  is '����������� ������� ���-���� ��������� ���������';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.change_date
  is '���� �������� ���������';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.worker_id
  is '���������, ������� ���������';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.data_source
  is '�������� �������� ������ (1 - ��������, 2 - ������, 3 - ����/����� �������� �������, 4 - ����/����� ��������� �������)';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.filename
  is '��� �����, �� �������� ����������� �������� (��� ���������� 3 � 4)';
comment on column T_SIM_BY_IMG_TYPE_HST_FULL.old_id
  is '������������� ���������� ��������� � �������� ������� (��� �������� ���������� = NULL)';
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