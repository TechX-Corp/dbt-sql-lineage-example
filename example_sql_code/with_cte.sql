WITH cst_ip_x_ip as (SELECT sbj_ip_id,
    obj_ip_code
    FROM "ead_data_prod"."vpb_golden"."cst_ip_x_ip_rltnp__s2"
    WHERE dbt_valid_from <= cast('2024-04-01' AS date)
    AND nvl(dbt_valid_to,'2400-01-01') > cast('2024-04-01' AS date)
    AND src_stm_code = 'WAY4_CLIENT' ---sua dk sang bang ref_cv__mnp)
)
SELECT '2024-04-01'::date AS business_date,
convert_timezone('UTC', 'Asia/Ho_Chi_Minh', cast(CURRENT_TIMESTAMP AS timestamp) )::timestamp AS ppn_date,
card.pymtc_code:: varchar(64) AS card_nbr,
card.ar_code:: varchar(64) AS contract_nbr,
card.dao_code:: varchar(64) AS dao_code,
card_ar.sale_code:: varchar(64) AS sale_code,
card.pd_prgm_code:: varchar(64) AS prd_prg_code,
card.ap_code:: varchar(64) AS apl_code,
card.prom_code:: varchar(64) AS prom_code,
card.lmt_code:: varchar(64) AS limit_code, ---cần confirm từ BA do pending
card.pd_card_code:: varchar(64) AS prd_card_code,
card.prn_ar_code:: varchar(64) AS parent_contract_nbr,
card.mgn_br_code:: varchar(64) AS mgmt_branch_code,
card.opn_br_code:: varchar(64) AS open_branch_code,
card.rcv_ou_code:: varchar(64) AS rcv_branch_code,
card.cst_dtl_code:: varchar(64) AS cust_originl_code,
cst_ip_x_ip.obj_ip_code::varchar(64) AS cust_code,
card.lmt_amt:: numeric(23,5) AS limit_amt,
card.avl_lmt_amt:: numeric(23,5) AS avail_limit_amt,
card.card_tp:: varchar(64) AS card_type,
card.lc_st:: varchar(64) AS status,
card.lc_st_nm:: varchar(64) AS status_name,
card.phys_card_st:: varchar(128) AS card_status,
card.issu_tp:: varchar(128) AS issu_type,
card.scrd_tp:: varchar(128) AS scrd_type,
card.prim_card_f:: varchar(10) AS prim_card_flag,
--card.FEE_WV_F::varchar(10) as FEE_WAIVED_FLAG,
card.vitual_card_f:: varchar(10) AS virtual_card_flag,
card_acm.card_ar_end_dt::date AS card_contract_end_date,
card.eff_fm_dt:: date AS card_open_date,
card.issu_mth:: varchar(24) AS issu_method,
card.eff_to_dt:: date AS exp_date,
card.t24_al_id:: varchar(64) AS t24_al_id,
card.clt_code:: varchar(64) AS coll_code,
'WAY4_ACNT_CONTRACT':: varchar(64) AS src_sys_code,
'ACNT_CONTRACT':: varchar(64) AS src_sys_name,
card.is_ready:: varchar(64) AS is_ready,
card_acm.t24_issu_dt:: date AS t24_issu_date,
card.t24_ac_nbr:: varchar(64) AS t24_acct_nbr,
card.t24_rqs_br:: varchar(64) AS t24_inpt_branch,
card.t24_rqs_cnl:: varchar(64) AS t24_req_channel,
card.unq_id_in_src_stm:: varchar(64) AS uniq_id_in_src_sys,
card.card_opn_dt:: date AS value_date,
card.last_udt_dt:: date AS actual_end_date, ---check lai mapping BA do pending
card_acm.last_actvn_dt:: date AS last_actvn_date,
card.actvn_inf:: varchar(256) AS actvn_info,
card.nxt_stmt_dt:: date AS next_stmt_date,
card.last_stmt_dt:: date AS last_stmt_date,
card.src_inf:: varchar(256) AS src_info,
card.t24_issu_svc:: varchar(128) AS t24_issu_service,
card.last_udt_dt::timestamp AS last_upd_date,
card.t24_inputter::varchar(64) AS t24_inpt,
card.t24_inputter_dt::timestamp AS t24_inpt_date,
card.t24_authoriser::varchar(64) AS t24_auth,
card.t24_authoriser_dt::timestamp AS t24_auth_date,
card.t24_orig_ac_nbr:: varchar(64) AS t24_originl_acct_nbr,
card.intr_cst_code:: varchar(32) AS intro_cust_code,
card_acm.card_dsgn_code::varchar(32) AS card_design_code,
card.adl_inf:: varchar(256) AS addtl_info,
card.w4_br_code:: varchar(10) AS w4_branch_code,
card.fee_wv_yr_nbr:: varchar(32) AS fee_waived_year_nbr, --term add new 20240813
card.ref_txn_ar_code:: varchar(256) AS ref_txn_contract_code --term add new 20240813
FROM "ead_data_prod"."vpb_golden"."ast_card_inf_prfl__mnp" card
LEFT JOIN "ead_data_prod"."vpb_golden"."ast_card_inf_acm_prfl__mnp" card_acm
ON card.pymtc_id = card_acm.pymtc_id
AND card.tf_partition_date = card_acm.tf_partition_date
LEFT JOIN cst_ip_x_ip
ON cst_ip_x_ip.sbj_ip_id = card.cst_dtl_id
LEFT JOIN "ead_data_prod"."vpb_golden"."ar_card_inf_acm_prfl__mnp" card_ar
ON card.ar_id = card_ar.ar_id
AND card.tf_partition_date = card_ar.tf_partition_date
WHERE card.tf_partition_date = cast('2024-04-01' AS date)
AND EXISTS
(
SELECT 1
FROM "ead_data_prod"."vpb_golden"."ar_x_cl_pst_way4__s2" arxcl
WHERE arxcl.dbt_valid_from <= cast('2024-04-01' AS date )
AND nvl(arxcl.dbt_valid_to,'2400-01-01') > cast('2024-04-01' AS date )
AND card.ar_id = arxcl.ar_id
)