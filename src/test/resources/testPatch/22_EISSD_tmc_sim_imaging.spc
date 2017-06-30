CREATE OR REPLACE PACKAGE tmc_sim_imaging IS

  FUNCTION get_dic_sim_imaging RETURN SYS_REFCURSOR;

  FUNCTION get_dic_sim_edit_oper_type RETURN SYS_REFCURSOR;
  
  FUNCTION get_dic_sim_data_source RETURN SYS_REFCURSOR;

  PROCEDURE add_sim_imaging_tab
  (
    pi_sim_imagings  sim_imaging_tab,
    pi_worker_id     NUMBER,
    pi_data_source   NUMBER,
    pi_filename      VARCHAR2,
    po_error_code    OUT NUMBER,
    po_error_message OUT VARCHAR2
  );

  PROCEDURE change_sim_imaging_tab
  (
    pi_sim_imagings  sim_imaging_tab,
    pi_worker_id     NUMBER,
    pi_data_source   NUMBER,
    pi_filename      VARCHAR2,
    po_error_code    OUT NUMBER,
    po_error_message OUT VARCHAR2
  );

  PROCEDURE change_sim_imaging
  (
    pi_sim_imaging   sim_imaging_type,
    pi_worker_id     NUMBER,
    pi_data_source   NUMBER,
    pi_filename      VARCHAR2,
    po_error_code    OUT NUMBER,
    po_error_message OUT VARCHAR2
  );

  FUNCTION get_conflict_ranges_for_list
  (
    pi_imaging_ranges sim_imaging_tab,
    po_error_code     OUT NUMBER,
    po_error_message  OUT VARCHAR2
  ) RETURN sim_imaging_tab;

  FUNCTION get_conflict_sim_ranges
  (
    pi_start         NUMBER,
    pi_end           NUMBER
  ) RETURN sim_imaging_tab;
  
  FUNCTION range_tab_exists(pi_imaging_ranges sim_imaging_tab) RETURN NUMBER;

  FUNCTION range_exists
  (
    pi_start NUMBER,
    pi_end   NUMBER
  ) RETURN NUMBER;

  FUNCTION add_tab_history
  (
    pi_imaging_ranges sim_imaging_tab,
    pi_operation_type NUMBER,
    pi_worker_id      NUMBER,
    po_error_code     OUT NUMBER,
    po_error_message  OUT VARCHAR2
  ) RETURN num_tab;

  FUNCTION add_history
  (
    pi_sim_imaging    sim_imaging_type,
    pi_operation_type NUMBER,
    pi_worker_id      NUMBER,
    po_error_code     OUT NUMBER,
    po_error_message  OUT VARCHAR2
  ) RETURN NUMBER;

  PROCEDURE add_tab_history_full
  (
    pi_history_ids     num_tab,
    pi_new_imsi_ranges sim_imaging_tab,
    pi_worker_id       NUMBER,
    pi_data_source     NUMBER,
    pi_filename        VARCHAR2,
    po_error_code      OUT NUMBER,
    po_error_message   OUT VARCHAR2
  );

  PROCEDURE add_history_full
  (
    pi_history_id           NUMBER,
    pi_old_id               NUMBER,
    pi_old_imsi_range_start NUMBER,
    pi_old_imsi_range_end   NUMBER,
    pi_old_sim_imaging_type NUMBER,
    pi_new_imsi_range_start NUMBER,
    pi_new_imsi_range_end   NUMBER,
    pi_new_sim_imaging_type NUMBER,
    pi_worker_id            NUMBER,
    pi_data_source          NUMBER,
    pi_filename             VARCHAR2,
    po_error_code           OUT NUMBER,
    po_error_message        OUT VARCHAR2
  );

  FUNCTION search_history
  (
    pi_imsi_range_start NUMBER,
    pi_imsi_range_end   NUMBER,
    pi_start_date       DATE,
    pi_end_date         DATE,
    pi_operation_types  num_tab,
    pi_data_sources     num_tab,
    pi_imaging_types    num_tab,
    pi_page_number      NUMBER,
    pi_page_size        NUMBER
  ) RETURN SYS_REFCURSOR;
END;
/
