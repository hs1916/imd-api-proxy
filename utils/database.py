import os
import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

from datetime import datetime




db_endpoint = 'testdb.cd0ub2t5tipq.ap-northeast-2.rds.amazonaws.com:5432/imd'
db_user = 'postgres'
db_password = 'postgres1%21'

host_url = "http://127.0.0.1:8000/"


def db_engine_get():
    """
    database 접근을 위한 DB Engine 생성
    :return:
    """
    db_uri = f'postgresql+psycopg2://{db_user}:{db_password}@{db_endpoint}'
    db_engine = sqlalchemy.create_engine(url=db_uri)
    return db_engine



def update_row(engine: sqlalchemy.engine.base.Engine, table_name: str, row: dict):

    qry = str()

    if table_name == 'imd_ia.tbl_iia_iam_idxinfo':
        qry = get_query_idxinfo(table_name, row)
    elif table_name == 'imd_ia.tbl_iia_iam_idxdata':
        qry = get_query_idxdata(table_name, row)
    elif table_name == 'imd_ia.tbl_iia_iam_fisis':
        qry = get_query_fisis(table_name, row)


    engine.execute(qry)

    engine.execute('commit;')
    engine.dispose()



def get_query_idxinfo(table_name: str, row: dict):
    insert_on_conflict = f"""
            insert into {table_name} 
        (
            rcve_orgn,
            idx_id,
            rgsr_emnb,
            rgst_dttm,
            rgst_prgm_id,
            last_chnr_emnb,
            last_chng_dttm,
            last_chng_prgm_id,
            idx_nm,
            data_lgcf_nm,
            data_mdcf_nm,
            rgn_cls,
            idx_peri,
            unit,
            rcve_yn
        )
        values 	
        (	
            '{row['rcve_orgn']}',
            '{row['idx_id']}',
            '{row['user_id']}',
             current_timestamp,
            '{row['prgm_id']}',
            '{row['user_id']}',
             current_timestamp,
            '{row['prgm_id']}',
            '{row['idx_nm']}',
            '{row['data_lgcf_nm']}',
            '{row['data_mdcf_nm']}',
            '{row['rgn_cls']}',
            '{row['idx_peri']}',
            '{row['unit']}',
            '{row['rcve_yn']}'
        )
        on conflict on constraint  pk_tbl_iia_iam_idxinfo
        do
        update
        set
            last_chnr_emnb = EXCLUDED.last_chnr_emnb,
            last_chng_dttm = EXCLUDED.last_chng_dttm,
            last_chng_prgm_id = EXCLUDED.last_chng_prgm_id,
            idx_nm = EXCLUDED.idx_nm,
            data_lgcf_nm = EXCLUDED.data_lgcf_nm,
            data_mdcf_nm = EXCLUDED.data_mdcf_nm,
            rgn_cls = EXCLUDED.rgn_cls,
            idx_peri = EXCLUDED.idx_peri,
            unit = EXCLUDED.unit,
            rcve_yn = EXCLUDED.rcve_yn      
    """
    return insert_on_conflict



def get_query_idxdata(table_name: str, row: dict):
    insert_on_conflict = f"""
        insert into {table_name}
        (
            rcve_orgn,
            idx_id,
            idx_pont,
            detl_item,
            rgsr_emnb,
            rgst_dttm,
            rgst_prgm_id,
            last_chnr_emnb,
            last_chng_dttm,
            last_chng_prgm_id,
            idx_nm,
            idx_val,
            unit,
            rcve_dt
        )
        values 	
        (	
            '{row['rcve_orgn']}',
            '{row['idx_id']}',
            '{row['idx_pont']}',
            '{row['detl_item']}',
            '{row['user_id']}',
             current_timestamp,
            '{row['prgm_id']}',
            '{row['user_id']}',
             current_timestamp,
            '{row['prgm_id']}',
            '{row['idx_nm']}',
            '{row['idx_val']}',
            '{row['unit']}',
            '{row['rcve_dt']}'
        )
        on conflict on constraint pk_tbl_iia_iam_idxdata
        do
        update
        set
            last_chnr_emnb = EXCLUDED.last_chnr_emnb,
            last_chng_dttm = EXCLUDED.last_chng_dttm,
            last_chng_prgm_id = EXCLUDED.last_chng_prgm_id,
            idx_nm = EXCLUDED.idx_nm,
            idx_val = EXCLUDED.idx_val,
            unit = EXCLUDED.unit,
            rcve_dt = EXCLUDED.rcve_dt	      
    """
    return insert_on_conflict



def get_query_fisis(table_name: str, row: dict):
    insert_on_conflict = f"""
        insert into {table_name} 
        (
            list_no,
            list_nm,
            base_month,
            finance_cd,
            finance_nm,
            account_cd,
            account_nm,
            a,
            b,
            c
        )
        values 	
        (	
            '{row['list_no']}',
            '{row['list_nm']}',
            '{row['base_month']}',
            '{row['finance_cd']}',
            '{row['finance_nm']}',
            '{row['account_cd']}',
            '{row['account_nm']}',
            '{row['a']}',
            '{row['b']}',
            '{row['c']}'
        )
        on conflict on constraint pk_tbl_iia_iam_fisis
            do
        update
        set
            list_no = EXCLUDED.list_no,
            list_nm = EXCLUDED.list_nm,
            base_month = EXCLUDED.base_month,
            finance_cd = EXCLUDED.finance_cd,
            finance_nm = EXCLUDED.finance_nm,
            account_cd = EXCLUDED.account_cd,
            account_nm = EXCLUDED.account_nm,
            a = EXCLUDED.a,
            b = EXCLUDED.b,
            c = EXCLUDED.c;
            
    """
    return insert_on_conflict





if __name__ == "__main__":
    pass
