import psycopg2
import pandas as pd
from datetime import datetime

class PGDatabase:
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        self.cur = self.conn.cursor()

    def create_field_table(self):
        self.cur.execute("""
                    CREATE TABLE field (
                        field_id        VARCHAR,
                        field_name      VARCHAR NOT NULL,
                        unit            VARCHAR,
                        field_type      VARCHAR(10) NOT NULL CHECK (field_type IN ('OIL_PROD', 'GAS_PROD', 'OIL_PLAN', 'GAS_PLAN')),
                        conversion_factor FLOAT,
                        PRIMARY KEY (field_id, field_type)
                    );
                    """
            )
        self.conn.commit()

    def create_plan_prod_table(self):
        self.cur.execute("""
                CREATE TABLE plan_prod (
                    field_id    VARCHAR,
                    report_date DATE NOT NULL,
                    plan_type   VARCHAR(20) NOT NULL,
                    prod_ton    FLOAT,
                    prod_bbls   FLOAT,
                    prod_m3     FLOAT,
                    prod_ft3    FLOAT,
                    PRIMARY KEY (field_id, report_date, plan_type)
                );
                """
            )
        self.conn.commit()

    def create_daily_prod_table(self):
        self.cur.execute("""
                CREATE TABLE daily_prod (
                    field_id    VARCHAR,
                    report_date DATE NOT NULL,
                    prod_type   VARCHAR,
                    prod_ton    FLOAT,
                    prod_bbls   FLOAT,
                    prod_m3     FLOAT,
                    prod_ft3    FLOAT,
                    PRIMARY KEY (field_id, report_date, prod_type)
                );
                """
            )
        self.conn.commit()

    def get_all_table_names(self):
        self.cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)
        return [row[0] for row in self.cur.fetchall()]
    
    def get_table(self, table_name):
        self.cur.execute(f"SELECT * FROM {table_name};")
        return self.cur.fetchall()

    def get_table_info(self, table_name):
        self.cur.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """)
        return self.cur.fetchall()

    def delete_table(self, table_name):
        self.cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        self.conn.commit()
        print(f"Table {table_name} deleted.")

    def insert_field(self, field_id, field_name, unit, field_type, conversion_factor):
        self.cur.execute("""
            INSERT INTO field (field_id, field_name, unit, field_type, conversion_factor)
            VALUES (%s, %s, %s, %s, %s);
        """, (field_id, field_name, unit, field_type, conversion_factor))
        self.conn.commit()

    def insert_plan_prod(self, field_id, report_date, plan_type, prod_ton, prod_bbls, prod_m3, prod_ft3):
        self.cur.execute("""
            INSERT INTO plan_prod (field_id, report_date, plan_type, prod_ton, prod_bbls, prod_m3, prod_ft3)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (field_id, report_date, plan_type, prod_ton, prod_bbls, prod_m3, prod_ft3))
        self.conn.commit()

    def insert_daily_prod(self, field_id, report_date, prod_type, prod_ton, prod_bbls, prod_m3, prod_ft3):
        self.cur.execute("""
            INSERT INTO daily_prod (field_id, report_date, prod_type, prod_ton, prod_bbls, prod_m3, prod_ft3)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (field_id, report_date, prod_type, prod_ton, prod_bbls, prod_m3, prod_ft3))
        self.conn.commit()

    def get_daily_prod_by_date(self, field_id, report_date, prod_type, unit='prod_bbls'):
        self.cur.execute(f"""
            SELECT {unit} FROM daily_prod
            WHERE field_id = %s AND report_date = %s AND prod_type = %s;
        """, (field_id, report_date, prod_type))
        return self.cur.fetchone()[0]
    
    def get_conversion_factor(self, field_id, prod_type):
        # Get factor from field table by field_id and prod_type
        self.cur.execute("""
            SELECT conversion_factor FROM field
            WHERE field_id = %s AND field_type = %s;
        """, (field_id, prod_type))
        return self.cur.fetchone()[0]
    
    def get_field_name(self, field_id, field_type):
        # Get full name from field table by field_id and field_type
        self.cur.execute("""
            SELECT field_name FROM field
            WHERE field_id = %s AND field_type = %s;
        """, (field_id, field_type))
        return self.cur.fetchone()[0]

    #=======GET FOR REPORTING=========
    # Column C, D
    def get_accum_plan_year(self, field_id, year, plan_type):
        # Extract the accumulated production for a specific year by field_id
        self.cur.execute("""
            SELECT SUM(prod_ton) FROM plan_prod
            WHERE field_id = %s AND EXTRACT(YEAR FROM report_date) = %s AND plan_type = %s;
        """, (field_id, year, plan_type))
        return self.cur.fetchone()[0]
    
    # Column D
    def get_monthly_prod(self, field_id, month, prod_type, year=2025):
        # Extract the production for a specific month by field_id
        self.cur.execute("""
            SELECT SUM(prod_ton) FROM daily_prod
            WHERE field_id = %s AND EXTRACT(MONTH FROM report_date) = %s AND EXTRACT(YEAR FROM report_date) = %s AND prod_type = %s;
        """, (field_id, month, year, prod_type))
        return self.cur.fetchone()[0]

    def get_accum_daily(self, field_id, month, prod_type, year=2025):
        # Extract the accumulated production from Jan to a specific month by field_id
        if month < 1 or month > 12:
            print("Invalid month. Month must be between 1 and 12.")
            return None
        if month == 1:
            return 0
        accum_prod = 0
        for i in range(1, month):
            monthly_prod = self.get_monthly_prod(field_id, i, prod_type, year)
            if monthly_prod is not None:
                accum_prod += monthly_prod
        return accum_prod
    
    # Column H
    def get_monthly_plan_prod(self, field_id, month, plan_type, year=2025):
        # Extract the production for a specific month by field_id
        self.cur.execute("""
            SELECT SUM(prod_ton) FROM plan_prod
            WHERE field_id = %s AND EXTRACT(MONTH FROM report_date) = %s AND EXTRACT(YEAR FROM report_date) = %s AND plan_type = %s;
        """, (field_id, month, year, plan_type))
        return self.cur.fetchone()[0]
    
    # Column J
    def get_accum_monthly_prod_to_a_date(self, field_id, report_date, prod_type):
        # report_date in yyyy/mm/dd
        # Extract the accumulated production from 1/1 to the specified date by field_id
        self.cur.execute("""
            SELECT SUM(prod_ton) FROM daily_prod
            WHERE field_id = %s AND report_date BETWEEN %s AND %s AND prod_type = %s;
        """, (field_id, f'{report_date.year}-{report_date.month}-01', report_date, prod_type))
        return self.cur.fetchone()[0]
    
    # Column N
    def get_accum_daily_prod_up_to_date(self, field_id, report_date, prod_type, unit, year):
        # Extract accumulated prod up to date from 1/1/year
        self.cur.execute(f"""
            SELECT SUM({unit}) FROM daily_prod
            WHERE field_id = %s AND report_date BETWEEN '{year}-01-01' AND %s AND prod_type = %s;
        """, (field_id, report_date, prod_type))
        return self.cur.fetchone()[0]
    


def generate_report(query_date, 
                    POSTGRES_DB, 
                    POSTGRES_USER, 
                    POSTGRES_PASSWORD,
                    HOST,
                    PORT):
    PGDB = PGDatabase(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=HOST,
        port=PORT
    )

    query_date = datetime.strptime(query_date, "%Y/%m/%d")
    month = query_date.month
    year = query_date.year
    day = query_date.day

    # Column B
    field_names = [
        "Bạch Hổ & Rồng& 50%NR-ĐM",
        "NR-ĐM (Zarubezhneft)",
        "Cond. Dinh Cố & GPP Ca Mau",
        "Đại Hùng",
        "PM3-CAA",
        "46 CN",
        "Rạng Đông+Phương Đông",
        "Ruby+Pearl+Topaz+ Diamond",
        "STĐ+STV+STT+STN",
        "Cá Ngừ Vàng",
        "Tê  Giác Trắng",
        "Chim Sáo+ Dừa",
        "Lan Tây + Lan Đỏ",
        "Rồng Đôi+Rồng Đôi Tây",
        "Hải Sư Trắng +Hải Sư Đen",
        "Thăng Long + Đông Đô",
        "Hải Thạch + Mộc Tinh",
        "Cá Tầm",
        "Thiên Ưng",
        "Sao Vàng -Đại Nguyệt",
        "Nhenhesky (49%VN)",
        "Algeria"
    ]
    fields =['BHR', 'DM', 'DC', 'DH', 'PM3CA', '46CN', 'RDPD', 'RPT', 'STD-STV-STT-STN', 'CNV', 'TGT', 'CS', 'LTLD', 'RD-RDT', 'HST-HSD', 'TLDD', 'HT-MT', 'CT', 'ThienUng', 'SVDN', 'Nhenhexky', 'Algeria']
    # Column C
    column_c = [PGDB.get_accum_plan_year(field_id=field, year=year, plan_type='KHSLCPGiaoOil') for field in fields]
    # Column D
    column_d = [PGDB.get_accum_plan_year(field_id=field, year=year, plan_type='KHQTOIL') for field in fields]
    # Column E
    sub_field_ids = [('BH', 'R', 'GT', 'ThT', 'NR'), 
                    'DM', 'DC-GPP', 'DH', 'PM3CAA', '46CN',
                    ('RangDong', 'PhuongDong'),
                    ('Ruby', 'Pearl', 'Topaz', 'Diamond'),
                    ('STD', 'STV', 'STD-DB', 'STT', 'STN'), 
                    'CNV', 'TGT', 'CS', 'LT', 'RD-RDT',
                    ('HST', 'HSD'), 'TLDD', 'HT-MT', 'CT', 
                    'ThienUng', 'SV', 'Nhenhexky', 'Algeria'
                    ]
    column_e = []
    for _field in sub_field_ids:
        if isinstance(_field, tuple):
            _sub_field_prod = []
            for sub_field in _field:
                _sub_field_prod.append(PGDB.get_accum_daily(field_id=sub_field, month=month, prod_type='OIL_PROD')/1000)
            column_e.append(sum(_sub_field_prod))
        else:
            column_e.append(PGDB.get_accum_daily(field_id=_field, month=month, prod_type='OIL_PROD')/1000)
    # Column F
    column_f = [e * 100 / (1000 * c) if c != 0 else 0 for e, c in zip(column_e, column_c)]
    # Column G
    column_g = [e * 100 / (1000 * d) if d != 0 else 0 for e, d in zip(column_e, column_d)]
    # Column H
    column_h = [PGDB.get_monthly_plan_prod(field_id=field, month=month, plan_type='KHSLCPGiaoOil')*1000 for field in fields]
    # Column I
    column_i = [PGDB.get_monthly_plan_prod(field_id=field, month=month, plan_type='KHQTOIL')*1000 for field in fields]
    # Column J
    column_j = []
    for _field in sub_field_ids:
        if isinstance(_field, tuple):
            _sub_field_prod = []
            for sub_field in _field:
                try:
                    _sub_field_prod.append(PGDB.get_accum_monthly_prod_to_a_date(field_id=sub_field, report_date=query_date, prod_type='OIL_PROD')/1000)
                except Exception as e:
                    _sub_field_prod.append(0)
            column_j.append(sum(_sub_field_prod))
        else:
            column_j.append(PGDB.get_accum_monthly_prod_to_a_date(field_id=_field, report_date=query_date, prod_type='OIL_PROD')/1000)
    # Column K
    column_k = [j * 100 / h if h != 0 else 0 for j, h in zip(column_j, column_h)]

    # Column L
    column_l = [j * 100 / i if i != 0 else 0 for j, i in zip(column_j, column_i)]

    # Column M
    column_m = [e + j for e, j in zip(column_e, column_j)]

    # Column N
    column_n = []
    for _field in sub_field_ids:
        if isinstance(_field, tuple):
            _unused_fields = ['Pearl', 'Topaz', 'Diamond', 'HSD']
            _v = 0
            for sub_field in _field:
                if sub_field in _unused_fields:
                    _v += 0
                else:
                    _v += PGDB.get_accum_daily_prod_up_to_date(field_id=sub_field, report_date=query_date, prod_type='OIL_PROD', unit='prod_bbls', year=year)
            column_n.append(_v)
        else:
            column_n.append(PGDB.get_accum_daily_prod_up_to_date(field_id=_field, report_date=query_date, prod_type='OIL_PROD', unit='prod_bbls', year=year))

    # Column O
    column_o = [m/(1000*c) if c != 0 else 0 for m, c in zip(column_m, column_c)]

    # Column P
    column_p = [m/(1000*d) if d != 0 else 0 for m, d in zip(column_m, column_d)]

    # Column Q
    column_q = []
    for _field in sub_field_ids:
        if isinstance(_field, tuple):
            _unused_fields = ['Pearl', 'Topaz', 'Diamond', 'HSD']
            _v = 0
            for sub_field in _field:
                if sub_field in _unused_fields:
                    _v +=0
                else:
                    _v += PGDB.get_daily_prod_by_date(field_id=sub_field, report_date=query_date, prod_type='OIL_PROD', unit='prod_ton')
            column_q.append(_v)
        else:
            column_q.append(PGDB.get_daily_prod_by_date(field_id=_field, report_date=query_date, prod_type='OIL_PROD', unit='prod_ton'))

    # Column R
    column_r = []
    for _field in sub_field_ids:
        if isinstance(_field, tuple):
            _unused_fields = ['Pearl', 'Topaz', 'Diamond', 'HSD']
            _v = 0
            for sub_field in _field:
                if sub_field in _unused_fields:
                    _v +=0
                else:
                    _v += PGDB.get_daily_prod_by_date(field_id=sub_field, report_date=query_date, prod_type='OIL_PROD', unit='prod_bbls')
            column_r.append(_v)
        else:
            column_r.append(PGDB.get_daily_prod_by_date(field_id=_field, report_date=query_date, prod_type='OIL_PROD', unit='prod_bbls'))
    data = {
        "Mỏ": field_names,
        "KHCP  (tr.tấn)": column_c,
        "KHQT  (tr.tấn)": column_d,
        "Tháng trước - Cộng dồn (ng.tấn)": column_e,
        "Tháng trước - %KHCP": column_f,
        "Tháng trước - %KHQT": column_g,
        "Tháng này - KHCP (ng.tấn)": column_h,
        "Tháng này - KHQT (ng.tấn)": column_i,
        "Tháng này - Thực hiện (ng.tấn)": column_j,
        "Tháng này - %KHCP": column_k,
        "Tháng này - %KHQT": column_l,
        "SL hiện tại - Cộng dồn (ng.tấn)": column_m,
        "SL hiện tại - Cộng dồn (thùng)": column_n,
        "SL hiện tại - %KHCP": column_o,
        "SL hiện tại - %KHQT": column_p,
        "SL ngày (tấn)": column_q,
        "SL ngày (thùng)": column_r
    }
    return pd.DataFrame(data)


