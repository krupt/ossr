CREATE OR REPLACE package user_pkg is
  PKG_NAME constant varchar2(20) := $$plsql_unit;
  ----------------------------------------------------
  --���������� � ������������ ��� �������
  ----------------------------------------------------
  function get_user_profile(pi_user_id   in number,
                            pi_worker_id in number,
                            po_roles     out sys_refcursor,
                            po_param     out sys_refcursor,
                            po_err_num   out number,
                            po_err_msg   out varchar2) return sys_refcursor;

  ---------------------------------------------------------------------------------------------
  -- ������� ��� ��������� ������������� ������ �������
  ---------------------------------------------------------------------------------------------
  procedure Change_User_himself(pi_user_id      in T_USER_ORG.USR_ID%type,
                                pi_old_password in T_USERS.USR_PWD_MD5%type,
                                pi_new_password in T_USERS.USR_PWD_MD5%type,
                                pi_SALT         in t_users.salt%type,
                                pi_HASH_ALG_ID  in t_users.hash_alg_id%type,
                                pi_email        in T_PERSON.PERSON_EMAIL%type,
                                pi_person_phone in t_person.person_phone%type, -- �������
                                pi_tz_id        in t_users.tz_id%type,
                                pi_params       in REQ_PARAM_TAB,
                                pi_org_id       in number,
                                pi_ip_address   in t_users_hist.ip_address%type,
                                po_err_num      out number,
                                po_err_msg      out varchar2);

  -------------------------------------------------
  --�������������� ������ ������������ ������������
  ---------------------------------------------------
  procedure Change_User_dover(pi_user_id               in T_USER_ORG.USR_ID%type,
                              pi_FIO_DOVER             in varchar2,
                              pi_POSITION_DOVER        in varchar2,
                              pi_NA_OSNOVANII_DOVER    in varchar2,
                              pi_ADDRESS_DOVER         in varchar2,
                              pi_NA_OSNOVANII_DOC_TYPE in varchar2,
                              pi_fio_dover_nominative  in varchar2,
                              pi_ip_address            in t_users_hist.ip_address%type,
                              po_err_num               out number,
                              po_err_msg               out varchar2);

  --------------------------------------------------
  -- ��������� ������� ��������� ������������
  --------------------------------------------------
  function Get_User_hist(pi_user_id in number,
                         po_err_num out number,
                         po_err_msg out varchar2) return sys_refcursor;

  --------------------------
  --��������� ������ �����������
  ----------------------------
  function get_tree_level(pi_worker_id    in number,
                          pi_start_org_id in number,
                          po_err_num      out number,
                          po_err_msg      out varchar2,
                          pi_show_blocked NUMBER := 0) return sys_refcursor;

  --------------------------------------------------------
  --������ email ��� ������� ��������� �������� ����� � �������������� ������
  --------------------------------------------------------
  function get_email_for_restoge_passw(po_err_num out number,
                                       po_err_msg out varchar2)
    return sys_refcursor;

  --------------------------------------------------------
  --��������� ������ �� ������������� ������
  ---------------------------------------------------------
  function set_restore_passw_code(pi_login     in varchar2,
                                  pi_remote_ip in varchar2,
                                  pi_code      in varchar2,
                                  po_err_num   out number,
                                  po_err_msg   out varchar2) return number;

  ---------------------------------------------------------
  --��������� ��������� ���� ������
  ---------------------------------------------------------
  procedure save_temp_password(pi_worker_id   in number,
                               pi_hash        in varchar2,
                               pi_SALT        in t_users.salt%type,
                               pi_HASH_ALG_ID in t_users.hash_alg_id%type,
                               pi_ip_address  in varchar2,
                               po_err_num     out number,
                               po_err_msg     out varchar2);

  --------------------------------------------
  --��������� �������������� ������������ �� ������ �� ������������� ������
  --------------------------------------------
  function get_worker_by_restore_code(pi_code    in varchar2,
                                      po_err_num out number,
                                      po_err_msg out varchar2) return number;

  -----------------------------------------------
  --��������� ������� � ����������� ������������
  -----------------------------------------------
  function get_user_channels(pi_worker_id in number,
                             po_err_num   out number,
                             po_err_msg   out varchar2) return sys_refcursor;

  ------------------------------------------------
  --����������� �������� �� ������������ ������������� �2�
  ------------------------------------------------
  function is_user_m2m(pi_worker_id in number) return number;

  ------------------------------------------------
  --���������� ������ ����������� � ������ ���� ������������ � �������
  ------------------------------------------------
  function get_tree_level_by_right(pi_worker_id    in number,
                                   pi_kl_region    in varchar2,
                                   pi_start_org_id in number,
                                   pi_right_str    in varchar2,
                                   po_err_num      out number,
                                   po_err_msg      out varchar2)
    return sys_refcursor;
    
  function get_tree_level_by_right(pi_worker_id    in number,
                                   pi_kl_region    in varchar2,
                                   pi_start_org_id in number,
                                   pi_right_str    in string_tab,
                                   po_err_num      out number,
                                   po_err_msg      out varchar2)
    return sys_refcursor;    

  ------------------------------------------------------------
  --��������� ���������� � ������������(light)
  ------------------------------------------------------------
  function get_user_light(pi_user_id in number,
                          po_err_num out number,
                          po_err_msg out varchar2) return sys_refcursor;

  FUNCTION get_user_default_region(pi_worker_id NUMBER) RETURN VARCHAR2;

end user_pkg;
/
