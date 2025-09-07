import psycopg2
import pandas as pd
from datetime import datetime

class PGOilQuery:
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

    def get_latest_date_by_field(self, field_id, prod_type, query_date = '2025/07/01'): #"%Y/%m/%d"
        # Check latest date of data before the query_date, if no data, choose the closest data had data
        query_date = datetime.strptime(query_date, "%Y/%m/%d").date()
        found = False
        excepted_dates = []
        self.cur.execute("""
                SELECT report_date
                FROM daily_prod
                WHERE field_id = %s AND prod_type = %s AND report_date <= %s
                ORDER BY report_date DESC
                LIMIT 1;
            """, (field_id, prod_type, query_date))
        try:
            _last_date = self.cur.fetchone()[0]
        except:
            _last_date = None

        while not found:
            # Check the data by that date
            if _last_date is not None:
                self.cur.execute("""
                    SELECT * FROM daily_prod
                    WHERE field_id = %s AND report_date = %s AND prod_type = %s;
                """, (field_id, _last_date, prod_type))
                data = self.cur.fetchone()
                _prod_ton = data[3] if data is not None else None
                _prod_bbls = data[4] if data is not None else None
                _prod_m3 = data[5] if data is not None else None
                _prod_ft3 = data[6] if data is not None else None
                if prod_type=='OIL_PROD' and (_prod_ton is not None and _prod_bbls is not None):
                    found = True
                    return _last_date#.strftime("%Y/%m/%d")
                elif prod_type=='GAS_PROD' and (_prod_m3 is not None and _prod_ft3 is not None):
                    found = True
                    # Return data as string #"%Y/%m/%d"
                    return _last_date#.strftime("%Y/%m/%d")
                else:
                    excepted_dates.append(_last_date)
                    print(f"Daily data on {_last_date} is incomplete for {field_id} - {prod_type}. Trying previous date...")
                    # Temporarily remove that date and check again
                    self.cur.execute("""
                        SELECT report_date
                        FROM daily_prod
                        WHERE field_id = %s AND prod_type = %s AND report_date <= %s
                        AND report_date NOT IN %s
                        ORDER BY report_date DESC
                        LIMIT 1;
                    """, (field_id, prod_type, query_date, tuple(excepted_dates)))
                    try:
                        _last_date = self.cur.fetchone()[0]
                    except:
                        _last_date = None
            else:
                print(f"No data found for {field_id} - {prod_type}.")
                return None

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
        return self.cur.fetchone()
    
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
    


def generate_oil_report(query_date, 
                    POSTGRES_DB, 
                    POSTGRES_USER, 
                    POSTGRES_PASSWORD,
                    HOST,
                    PORT):
    PGDB = PGOilQuery(
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
                _prod = PGDB.get_accum_daily(field_id=sub_field, month=month, prod_type='OIL_PROD')/1000
                if _prod is not None:
                    _sub_field_prod.append(_prod)
                else:
                    _sub_field_prod.append(0)
            column_e.append(sum(_sub_field_prod))
        else:
            _prod = PGDB.get_accum_daily(field_id=_field, month=month, prod_type='OIL_PROD')
            column_e.append(_prod/1000 if _prod is not None else 0)
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
                    _prod = PGDB.get_accum_monthly_prod_to_a_date(field_id=sub_field, report_date=query_date, prod_type='OIL_PROD')
                    _sub_field_prod.append(_prod/1000)
                except Exception as e:
                    _sub_field_prod.append(0)
            column_j.append(sum(_sub_field_prod))
        else:
            _prod = PGDB.get_accum_monthly_prod_to_a_date(field_id=_field, report_date=query_date, prod_type='OIL_PROD')
            column_j.append(_prod/1000 if _prod is not None else 0)
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
                    _prod = PGDB.get_accum_daily_prod_up_to_date(field_id=sub_field, report_date=query_date, prod_type='OIL_PROD', unit='prod_bbls', year=year)
                    if _prod is not None:
                        _v += _prod
            column_n.append(_v)
        else:
            _prod = PGDB.get_accum_daily_prod_up_to_date(field_id=_field, report_date=query_date, prod_type='OIL_PROD', unit='prod_bbls', year=year)
            if _prod is not None:
                column_n.append(_prod)
            else:
                column_n.append(0)

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
                    _daily_prod = PGDB.get_daily_prod_by_date(field_id=sub_field, report_date=query_date, prod_type='OIL_PROD', unit='prod_ton')
                    if _daily_prod is not None:
                        _v += _daily_prod[0]
            column_q.append(_v)
        else:
            _daily_prod = PGDB.get_daily_prod_by_date(field_id=_field, report_date=query_date, prod_type='OIL_PROD', unit='prod_ton')
            if _daily_prod is not None:
                column_q.append(_daily_prod[0])
            else:
                column_q.append(0)

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
                    _daily_prod = PGDB.get_daily_prod_by_date(field_id=sub_field, report_date=query_date, prod_type='OIL_PROD', unit='prod_bbls')
                    if _daily_prod is not None:
                        _v += _daily_prod[0]
            column_r.append(_v)
        else:
            _daily_prod = PGDB.get_daily_prod_by_date(field_id=_field, report_date=query_date, prod_type='OIL_PROD', unit='prod_bbls')
            if _daily_prod is not None:
                column_r.append(_daily_prod[0])
            else:
                column_r.append(0)
    data = {
        "Mỏ": field_names,
        "KHCP  (tr.tấn)": [ '%.2f' % elem for elem in column_c ],
        "KHQT  (tr.tấn)": [ '%.2f' % elem for elem in column_d ],
        "Tháng trước - Cộng dồn (ng.tấn)": [ '%.2f' % elem for elem in column_e ],
        "Tháng trước - %KHCP": [ '%.2f' % elem for elem in column_f ],
        "Tháng trước - %KHQT": [ '%.2f' % elem for elem in column_g ],
        "Tháng này - KHCP (ng.tấn)": [ '%.2f' % elem for elem in column_h ],
        "Tháng này - KHQT (ng.tấn)": [ '%.2f' % elem for elem in column_i ],
        "Tháng này - Thực hiện (ng.tấn)": [ '%.2f' % elem for elem in column_j ],
        "Tháng này - %KHCP": [ '%.2f' % elem for elem in column_k ],
        "Tháng này - %KHQT": [ '%.2f' % elem for elem in column_l ],
        "SL hiện tại - Cộng dồn (ng.tấn)": [ '%.2f' % elem for elem in column_m ],
        "SL hiện tại - Cộng dồn (thùng)": [ '%.2f' % elem for elem in column_n ],
        "SL hiện tại - %KHCP": [ '%.2f' % elem for elem in column_o ],
        "SL hiện tại - %KHQT": [ '%.2f' % elem for elem in column_p ],
        "SL ngày (tấn)": [ '%.2f' % elem for elem in column_q ],
        "SL ngày (thùng)": [ '%.2f' % elem for elem in column_r ]
    }
    return pd.DataFrame(data)
 
def generate_oil_report_w_latest_data(query_date, 
                    POSTGRES_DB, 
                    POSTGRES_USER, 
                    POSTGRES_PASSWORD,
                    HOST,
                    PORT):
    PGDB = PGOilQuery(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=HOST,
        port=PORT
    )

    sub_field_ids = [('BH', 'R', 'GT', 'ThT', 'NR'), 
                        'DM', 'DC-GPP', 'DH', 'PM3CAA', '46CN',
                        ('RangDong', 'PhuongDong'),
                        ('Ruby', 'Pearl', 'Topaz', 'Diamond'),
                        ('STD', 'STV', 'STD-DB', 'STT', 'STN'), 
                        'CNV', 'TGT', 'CS', 'LT', 'RD-RDT',
                        ('HST', 'HSD'), 'TLDD', 'HT-MT', 'CT', 
                        'ThienUng', 'SV', 'Nhenhexky', 'Algeria'
                    ]
    _latest_dates_by_field = {}
    for sub_field_id in sub_field_ids:
        if isinstance(sub_field_id, tuple):
            dates = []
            for field_id in sub_field_id:
                _date = PGDB.get_latest_date_by_field(field_id, 'OIL_PROD', query_date)
                dates.append(_date)
            valid_dates = [d for d in dates if d is not None]
            _date = max(valid_dates) if valid_dates else None
            _latest_dates_by_field[sub_field_id] = _date
        else:
            _date = PGDB.get_latest_date_by_field(sub_field_id, 'OIL_PROD', query_date)
            _latest_dates_by_field[sub_field_id] = _date

    report = generate_oil_report(query_date,
                                POSTGRES_DB, 
                                POSTGRES_USER, 
                                POSTGRES_PASSWORD,
                                HOST,
                                PORT)
    _reformat_date = datetime.strptime(query_date, "%Y/%m/%d")
    report['Dữ liệu cập nhật đến ngày'] = _reformat_date.strftime("%d/%m/%Y")
    for i, (k, v) in enumerate(_latest_dates_by_field.items()):
        if v is not None and v != datetime.strptime(query_date, "%Y/%m/%d").date():
            _query_date = v.strftime("%Y/%m/%d")
            print(f"Field {k} has latest data on {_query_date}, not {query_date}")
            _new_query_report = generate_oil_report(_query_date,
                                                    POSTGRES_DB, 
                                                    POSTGRES_USER, 
                                                    POSTGRES_PASSWORD,
                                                    HOST,
                                                    PORT)
            _reformat_date = datetime.strptime(_query_date, "%Y/%m/%d")
            _new_query_report['Dữ liệu cập nhật đến ngày'] = _reformat_date.strftime("%d/%m/%Y")
            report.at[i, 'Dữ liệu cập nhật đến ngày'] = v.strftime("%d/%m/%Y")
            report.loc[i, :] = _new_query_report.loc[i, :].values
        else:
            print(f"Field {k} has latest data on same date")
    return report

# ================= GAS REPORT ================================== GAS REPORT ===========================================
class PGGasQuery:
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        self.cur = self.conn.cursor()
    
    def get_all_table_names(self):
        self.cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)
        return [row[0] for row in self.cur.fetchall()]

    def get_latest_date_by_field(self, field_id, prod_type='GAS_PROD', query_date = '2025/07/01'): #"%Y/%m/%d"
        # Check latest date of data before the query_date, if no data, choose the closest data had data
        query_date = datetime.strptime(query_date, "%Y/%m/%d").date()
        found = False
        excepted_dates = []
        self.cur.execute("""
                SELECT report_date
                FROM daily_prod
                WHERE field_id = %s AND prod_type = %s AND report_date <= %s
                ORDER BY report_date DESC
                LIMIT 1;
            """, (field_id, prod_type, query_date))
        try:
            _last_date = self.cur.fetchone()[0]
        except:
            _last_date = None

        while not found:
            # Check the data by that date
            if _last_date is not None:
                self.cur.execute("""
                    SELECT * FROM daily_prod
                    WHERE field_id = %s AND report_date = %s AND prod_type = %s;
                """, (field_id, _last_date, prod_type))
                data = self.cur.fetchone()
                _prod_ton = data[3] if data is not None else None
                _prod_bbls = data[4] if data is not None else None
                _prod_m3 = data[5] if data is not None else None
                _prod_ft3 = data[6] if data is not None else None
                if prod_type=='OIL_PROD' and (_prod_ton is not None and _prod_bbls is not None):
                    found = True
                    return _last_date#.strftime("%Y/%m/%d")
                elif prod_type=='GAS_PROD' and (_prod_m3 is not None and _prod_ft3 is not None):
                    found = True
                    # Return data as string #"%Y/%m/%d"
                    return _last_date#.strftime("%Y/%m/%d")
                else:
                    excepted_dates.append(_last_date)
                    print(f"Daily data on {_last_date} is incomplete for {field_id} - {prod_type}. Trying previous date...")
                    # Temporarily remove that date and check again
                    self.cur.execute("""
                        SELECT report_date
                        FROM daily_prod
                        WHERE field_id = %s AND prod_type = %s AND report_date <= %s
                        AND report_date NOT IN %s
                        ORDER BY report_date DESC
                        LIMIT 1;
                    """, (field_id, prod_type, query_date, tuple(excepted_dates)))
                    try:
                        _last_date = self.cur.fetchone()[0]
                    except:
                        _last_date = None
            else:
                print(f"No data found for {field_id} - {prod_type}.")
                return None
    
    #=======GET FOR REPORTING=========
    # Column C, D
    def get_accum_plan_year(self, field_id, year, plan_type):
        # Extract the accumulated production for a specific year by field_id
        self.cur.execute("""
            SELECT SUM(prod_m3) FROM plan_prod
            WHERE field_id = %s AND EXTRACT(YEAR FROM report_date) = %s AND plan_type = %s;
        """, (field_id, year, plan_type))
        return self.cur.fetchone()[0]
    
    def get_data_by_field(self, field_id):
        """Get datetime and prod_m3 by field_id and prod_type='GAS_PROD'"""
        self.cur.execute("""
            SELECT report_date, prod_m3 FROM daily_prod
            WHERE field_id = %s AND prod_type = 'GAS_PROD';
        """, (field_id,))
        return self.cur.fetchall()

    def get_all_production_data(self):
        """Get all production data for all fields and prod_type='GAS_PROD'"""
        self.cur.execute("""
            SELECT field_id, report_date, prod_m3 FROM daily_prod
            WHERE prod_type = 'GAS_PROD';
        """)
        return self.cur.fetchall()
    
    # Column D
    def get_monthly_prod(self, field_id, month, prod_type, year=2025):
        # Extract the production for a specific month by field_id
        self.cur.execute("""
            SELECT SUM(prod_m3) FROM daily_prod
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
            SELECT SUM(prod_m3) FROM plan_prod
            WHERE field_id = %s AND EXTRACT(MONTH FROM report_date) = %s AND EXTRACT(YEAR FROM report_date) = %s AND plan_type = %s;
        """, (field_id, month, year, plan_type))
        return self.cur.fetchone()[0]
    
    # Column J
    def get_accum_monthly_prod_to_a_date(self, field_id, report_date, prod_type):
        # report_date in yyyy/mm/dd
        # Extract the accumulated production from 1/1 to the specified date by field_id
        self.cur.execute("""
            SELECT SUM(prod_m3) FROM daily_prod
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
    
    # Column Q, R
    def get_daily_prod_by_date(self, field_id, report_date, prod_type, unit='prod_bbls'):
        self.cur.execute(f"""
            SELECT {unit} FROM daily_prod
            WHERE field_id = %s AND report_date = %s AND prod_type = %s;
        """, (field_id, report_date, prod_type))
        return self.cur.fetchone()
    
def generate_gas_report(query_date, 
                    POSTGRES_DB, 
                    POSTGRES_USER, 
                    POSTGRES_PASSWORD,
                    HOST,
                    PORT):
    PGDB = PGGasQuery(
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
        'Bạch Hổ+ Rồng',
        'Tê Giác Trắng',
        'Rạng Đông+Phương Đông',
        'Chim Sáo+  Dừa',
        'STĐ+STV+STT+STN',
        'Cá Ngừ Vàng',
        'Lan Tây+Lan Đỏ',
        'Rồng Đôi+Rồng Đôi Tây',
        'Lô PM3-CAA ( tổng khí về bờ)',
        'Hải Sư Trắng +Hải Sư Đen',
        'Hải Thạch + Mộc Tinh',
        'Thái Bình',
        'Thiên Ưng',
        'Sao Vàng -Đại Nguyet',
        'Đại Hùng',
        'Cá Tầm',
    ]
    KHQT_fields = ['BH', 'TGT', 'RangDong', 'CS', 'STD-STV-STT', 'CNV', 'LTLD', 'RD-RDT', 'PM3CA-46CN', 'HST-HSD', 'HT-MT', 'TB', 'ThienUng', 'SVDN', 'DH', 'CT']
    KHCP_fields = ['BH', 'TGT', 'RDPD', 'CS-D', 'STD-STV-STT-STN', 'CNV', 'LTLD', 'RD-RDT', 'PM3CA-46CN', 'HST-HSD', 'HT-MT', 'TB', 'ThienUng', 'SVDN', 'DH', 'CT']

    # Column C
    column_c = [PGDB.get_accum_plan_year(field_id=field, year=year, plan_type='KHSLCPGiaoGas') for field in KHCP_fields]
    # Column D
    column_d = [PGDB.get_accum_plan_year(field_id=field, year=year, plan_type='KHQTGAS') for field in KHQT_fields]

    # Column E
    sub_field_ids = [('BH', 'R'), 
                    'TGT',
                    ('RangDong', 'PhuongDong'),
                    'CS',
                    ('STD', 'STV', 'STD-DB', 'STT', 'STN'), 
                    'CNV', 'LT', 'RD-RDT', 
                    'PM3-46CN',
                    'HST-HSD', 'HT', 'ThaiBinh', 'ThienUng', 'SV', 'DH', 'CT'
                    ]
    column_e = []
    for _field in sub_field_ids:
        if isinstance(_field, tuple):
            _sub_field_prod = []
            for sub_field in _field:
                _accum_prod = PGDB.get_accum_daily(field_id=sub_field, month=month, prod_type='GAS_PROD')
                if _accum_prod is not None:
                    _sub_field_prod.append(_accum_prod)
                else:
                    _sub_field_prod.append(0)
            column_e.append(sum(_sub_field_prod))
        else:
            _accum_prod = PGDB.get_accum_daily(field_id=_field, month=month, prod_type='GAS_PROD')
            if _accum_prod is not None:
                column_e.append(_accum_prod)
            else:
                column_e.append(0)

    # Column F
    column_f = [e * 100 / (c) if c != 0 else 0 for e, c in zip(column_e, column_c)]
    # Column G
    column_g = [e * 100 / (d) if d != 0 else 0 for e, d in zip(column_e, column_d)]
    # Column H
    column_h = [PGDB.get_monthly_plan_prod(field_id=field, month=month, plan_type='KHSLCPGiaoGas') for field in KHCP_fields]
    # Column I
    column_i = [PGDB.get_monthly_plan_prod(field_id=field, month=month, plan_type='KHQTGAS') for field in KHQT_fields]
    # Column J
    column_j = []
    for _field in sub_field_ids:
        if isinstance(_field, tuple):
            _sub_field_prod = []
            for sub_field in _field:
                try:
                    _prod = PGDB.get_accum_monthly_prod_to_a_date(field_id=sub_field, report_date=query_date, prod_type='GAS_PROD')
                    if _prod is None:
                        _prod = 0
                    _sub_field_prod.append(_prod)
                except Exception as e:
                    _sub_field_prod.append(0)
            column_j.append(sum(_sub_field_prod))
        else:
            _prod = PGDB.get_accum_monthly_prod_to_a_date(field_id=_field, report_date=query_date, prod_type='GAS_PROD')
            if _prod is not None:
                column_j.append(_prod)
            else:
                column_j.append(0)
    # # Column K
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
                    _prod = PGDB.get_accum_daily_prod_up_to_date(field_id=sub_field, report_date=query_date, prod_type='GAS_PROD', unit='prod_ft3', year=year)
                    if _prod is None:
                        _prod = 0
                    _v += _prod
            column_n.append(_v)
        else:
            _prod = PGDB.get_accum_daily_prod_up_to_date(field_id=_field, report_date=query_date, prod_type='GAS_PROD', unit='prod_ft3', year=year)
            if _prod is not None:
                column_n.append(_prod)
            else:
                column_n.append(0)

    # Column O
    column_o = [100*m/c if c != 0 else 0 for m, c in zip(column_m, column_c)]

    # Column P
    column_p = [100*m/d if d != 0 else 0 for m, d in zip(column_m, column_d)]

    # Column Q
    column_q = []
    for _field in sub_field_ids:
        if isinstance(_field, tuple):
            _v = 0
            for sub_field in _field:
                _prod = PGDB.get_daily_prod_by_date(field_id=sub_field, report_date=query_date, prod_type='GAS_PROD', unit='prod_m3')
                if _prod is None:
                    _prod = 0
                    _v += _prod
                else:
                    _v += _prod[0]
            column_q.append(_v)
        else:
            _prod = PGDB.get_daily_prod_by_date(field_id=_field, report_date=query_date, prod_type='GAS_PROD', unit='prod_m3')
            if _prod is not None:
                column_q.append(_prod[0])
            else:
                column_q.append(0)

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
                    _prod = PGDB.get_daily_prod_by_date(field_id=sub_field, report_date=query_date, prod_type='GAS_PROD', unit='prod_ft3')
                    if _prod is None:
                        _prod = 0
                        _v += _prod
                    else:
                        _v += _prod[0]
            column_r.append(_v)
        else:
            _prod = PGDB.get_daily_prod_by_date(field_id=_field, report_date=query_date, prod_type='GAS_PROD', unit='prod_ft3')
            if _prod is not None:
                column_r.append(_prod[0])
            else:
                column_r.append(0)

    data = {
            "Mỏ": field_names,
            "KHCP  (tr.m3)": ["%.2f" % elem for elem in column_c],
            "KHQT  (tr.m3)": ["%.2f" % elem for elem in column_d],
            "Tháng trước - Cộng dồn (tr.m3)": ["%.2f" % elem for elem in column_e],
            "Tháng trước - %KHCP": ["%.2f" % elem for elem in column_f],
            "Tháng trước - %KHQT": ["%.2f" % elem for elem in column_g],
            "Tháng này - KHCP (tr.m3)": ["%.2f" % elem for elem in column_h],
            "Tháng này - KHQT (tr.m3)": ["%.2f" % elem for elem in column_i],
            "Tháng này - Thực hiện (tr.m3)": ["%.2f" % elem for elem in column_j],
            "Tháng này - %KHCP": ["%.2f" % elem for elem in column_k],
            "Tháng này - %KHQT": ["%.2f" % elem for elem in column_l],
            "SL hiện tại - Cộng dồn (tr.m3)": ["%.2f" % elem for elem in column_m],
            "SL hiện tại - Cộng dồn (tr.ft3)": ["%.2f" % elem for elem in column_n],
            "SL hiện tại - %KHCP": ["%.2f" % elem for elem in column_o],
            "SL hiện tại - %KHQT": ["%.2f" % elem for elem in column_p],
            "SL ngày (tr.m3)": ["%.2f" % elem for elem in column_q],
            "SL ngày (tr.ft3)": ["%.2f" % elem for elem in column_r]
        }
    return pd.DataFrame(data)

def generate_gas_report_w_latest_data(query_date, 
                    POSTGRES_DB, 
                    POSTGRES_USER, 
                    POSTGRES_PASSWORD,
                    HOST,
                    PORT):
    PGDB = PGGasQuery(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=HOST,
        port=PORT
    )
    sub_field_ids = [('BH', 'R'), 
                    'TGT',
                    ('RangDong', 'PhuongDong'),
                    'CS',
                    ('STD', 'STV', 'STD-DB', 'STT', 'STN'), 
                    'CNV', 'LT', 'RD-RDT', 
                    'PM3-46CN',
                    'HST-HSD', 'HT', 'ThaiBinh', 'ThienUng', 'SV', 'DH', 'CT'
                    ]
    _latest_dates_by_field = {}
    for sub_field_id in sub_field_ids:
        if isinstance(sub_field_id, tuple):
            dates = []
            for field_id in sub_field_id:
                _date = PGDB.get_latest_date_by_field(field_id, 'GAS_PROD', query_date)
                dates.append(_date)
            valid_dates = [d for d in dates if d is not None]
            _date = max(valid_dates) if valid_dates else None
            _latest_dates_by_field[sub_field_id] = _date
        else:
            _date = PGDB.get_latest_date_by_field(sub_field_id, 'GAS_PROD', query_date)
            _latest_dates_by_field[sub_field_id] = _date

    report = generate_gas_report(query_date,
                                POSTGRES_DB, 
                                POSTGRES_USER, 
                                POSTGRES_PASSWORD,
                                HOST,
                                PORT)
    _reformat_date = datetime.strptime(query_date, "%Y/%m/%d")
    report['Dữ liệu cập nhật đến ngày'] = _reformat_date.strftime("%d/%m/%Y")
    for i, (k, v) in enumerate(_latest_dates_by_field.items()):
        if v is not None and v != datetime.strptime(query_date, "%Y/%m/%d").date():
            _query_date = v.strftime("%Y/%m/%d")
            print(f"Field {k} has latest data on {_query_date}, not {query_date}")
            _new_query_report = generate_gas_report(_query_date,
                                                    POSTGRES_DB, 
                                                    POSTGRES_USER, 
                                                    POSTGRES_PASSWORD,
                                                    HOST,
                                                    PORT)
            _reformat_date = datetime.strptime(_query_date, "%Y/%m/%d")
            _new_query_report['Dữ liệu cập nhật đến ngày'] = _reformat_date.strftime("%d/%m/%Y")
            report.at[i, 'Dữ liệu cập nhật đến ngày'] = v.strftime("%d/%m/%Y")
            report.loc[i, :] = _new_query_report.loc[i, :].values
        else:
            print(f"Field {k} has latest data on same date")
    return report