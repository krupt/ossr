CREATE OR REPLACE PACKAGE mvno_pkg IS

  FUNCTION create_message_pack
  (
    p_message_pack  mvno_message_pack_type,
    p_message_list  mvno_message_tab,
    o_error_code    OUT NUMBER,
    o_error_message OUT VARCHAR2
  ) RETURN NUMBER;

  FUNCTION check_message_pack_exists
  (
    p_message_pack_name VARCHAR2,
    p_message_pack_id   NUMBER := NULL
  ) RETURN NUMBER;

  PROCEDURE save_message_pack
  (
    p_message_pack  mvno_message_pack_type,
    o_error_code    OUT NUMBER,
    o_error_message OUT VARCHAR2
  );

  PROCEDURE change_message_pack_state
  (
    p_message_pack  mvno_message_pack_type,
    o_error_code    OUT NUMBER,
    o_error_message OUT VARCHAR2
  );

  FUNCTION find_message_pack
  (
    p_id          NUMBER,
    p_name        VARCHAR2,
    p_message     VARCHAR2,
    p_start_date  DATE,
    p_end_date    DATE,
    p_start_time  VARCHAR2,
    p_end_time    VARCHAR2,
    p_regions     varchar2_100_tab,
    p_subjects    num_tab,
    p_statuses    num_tab,
    p_workers     num_tab,
    p_page_number NUMBER,
    p_page_size   NUMBER
  ) RETURN SYS_REFCURSOR;

  FUNCTION get_message_pack(p_message_pack_id NUMBER) RETURN mvno_message_pack_type;

  FUNCTION get_message_pack_history(p_message_pack_id NUMBER) RETURN SYS_REFCURSOR;

  FUNCTION get_message_pack_messages(p_message_pack_id NUMBER) RETURN SYS_REFCURSOR;

  FUNCTION get_all_message_pack_states RETURN SYS_REFCURSOR;

  FUNCTION get_all_subjects RETURN SYS_REFCURSOR;

  FUNCTION get_users_by_role(p_role_id NUMBER) RETURN num_tab;

  FUNCTION get_candidates2send(p_state_id NUMBER) RETURN mvno_message_pack_tab;

  FUNCTION get_messages2send
  (
    p_pack_id       NUMBER,
    p_message_state NUMBER
  ) RETURN SYS_REFCURSOR;

  FUNCTION get_count_messages2send
  (
    p_pack_id       NUMBER,
    p_message_state NUMBER
  ) RETURN NUMBER;

  PROCEDURE save_messages
  (
    p_messages      mvno_message_tab,
    o_error_code    OUT NUMBER,
    o_error_message OUT VARCHAR2
  );

  FUNCTION get_mvno_regions_dictionary RETURN SYS_REFCURSOR;

END;
/
