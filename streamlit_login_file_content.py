# -*- coding: utf-8 -*-

import streamlit as st
import pymysql
from datetime import datetime
import pandas as pd
from pandas.tseries.offsets import MonthEnd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import io, os
from streamlit_option_menu import option_menu

@st.cache_data
def load_location_data():    
    #file_path = "file_content.txt"
    file_path = "file_content.txt"
    
    if not os.path.exists(file_path):
        st.error(f"'{file_path}' 파일을 찾을 수 없습니다.")
        return {}

    file_content = ""
    # 1. 인코딩 시도 (cp949는 윈도우 메모장 기본 한글 인코딩인 경우가 많음)
    try:
        with open(file_path, "r", encoding="cp949") as f:
            file_content = f.read()
    except UnicodeDecodeError:
        # 2. cp949 실패 시 utf-8로 다시 시도
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                file_content = f.read()
        except Exception as e:
            st.error(f"파일 인코딩 오류: {e}")
            return {}
    
    data = {}
    lines = file_content.strip().split('\n')
    for line in lines[1:]:
        parts = line.split('\t')
        if len(parts) < 3 or parts[2].strip() != '존재':
            continue        
        lawd_cd = parts[0].strip()[:5] # 시군구 코드 (앞 5자리)
        full_address = parts[1].strip()
        address_parts = full_address.split()
        
        if len(address_parts) < 2:
            continue
        sido = address_parts[0]
        
        # 시군구명 추출 로직 (원본 코드 복잡성 유지)
        sigungu = ""
        dong = ""        
        big_city = ['성남시','수원시','고양시','부천시','안양시','안산시','용인시','창원시','천안시','포항시','청주시','전주시']
        
        if len(address_parts) == 2 and address_parts[1] in big_city:
            continue
        elif len(address_parts) >= 2 and address_parts[0] =='세종특별자치시':
            sigungu = "세종시"
            dong = ' '.join(address_parts[1:])         
        elif len(address_parts) > 2 and address_parts[1] in big_city:
            sigungu = " ".join(address_parts[1:3])
            if len(address_parts) > 3:
                dong = ' '.join(address_parts[3:])
        else:
            sigungu = address_parts[1]
            if len(address_parts) > 2:
                dong = ' '.join(address_parts[2:])
        # 1. 시도 계층 구조 생성
        if sido not in data:
            data[sido] = {}
        if sigungu and sigungu not in data[sido]:
            data[sido][sigungu] = []
        if dong and dong not in data[sido][sigungu]:
            data[sido][sigungu].append(dong)           
        

    # 정렬
    for sido_val in data:
        for sigungu_val in data[sido_val]:
            data[sido_val][sigungu_val].sort()
        
        if sido not in data: data[sido] = {}
        if sigungu not in data[sido]: data[sido][sigungu] = []
        if dong and dong not in data[sido][sigungu]: data[sido][sigungu].append(dong)
    
    return data

#sido_data = load_location_data()

st.set_page_config(page_title="실거래가조회", layout="wide")

# 1. DB 연결 함수
load_dotenv()

def get_engine():
    # 로컬 .env 또는 서버 환경 변수에서 가져옴
    db_user = os.getenv("DB_USER")
    db_pw = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    
    # SQLAlchemy 엔진 생성
    db_url = f"mysql+pymysql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}"
    return create_engine(db_url)


def get_connection():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        # 포트 번호는 정수(int)형이어야 하므로 형변환이 필요합니다.
        port=int(os.getenv("DB_PORT", 3309)), 
        charset='utf8',
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor
    )
# 2. 로그인 처리 로직
def login_handler(id_input, pass_input):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        # ID와 PW를 동시에 체크
        sql = "SELECT user FROM rp_user WHERE user = %s AND password = %s;"
        cur.execute(sql, (id_input, pass_input))
        row = cur.fetchone()
        return True if row else False
    except pymysql.Error as e:
        st.error(f"DB 오류: {e}")
        return False
    finally:
        if conn: conn.close()

def signup_handler(new_id, new_pass):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        # 아이디 중복 체크
        cur.execute("SELECT * FROM rp_user WHERE user = %s;", (new_id,))
        if cur.fetchone():
            return False, "이미 존재하는 아이디입니다."
        
        # 정보 저장
        cur.execute("INSERT INTO rp_user (user, password) VALUES (%s, %s);", (new_id, new_pass))
        return True, "회원가입이 완료되었습니다!"
    except pymysql.Error as e:
        return False, f"DB 오류: {e}"
    finally:
        if conn: conn.close()

def delete_user_handler(user_id):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM rp_user WHERE user = %s;", (user_id,))
        return True
    except pymysql.Error as e:
        st.error(f"탈퇴 처리 중 오류 발생: {e}")
        return False
    finally:
        if conn: conn.close()

def get_total_user_count():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        # rp_user 테이블의 전체 행 개수 조회
        cur.execute("SELECT COUNT(*) as count FROM rp_user;")
        row = cur.fetchone()
        return row['count'] if row else 0
    except pymysql.Error as e:
        st.error(f"회원 수 조회 중 오류: {e}")
        return 0
    finally:
        if conn: conn.close()


# --- 3. 메인 로직 ---
def main():
    sido_data = load_location_data() #streamlit은 rerun을 고려해서 main함수내에서 선언되어야 한다.
    
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
        
    if 'result_df' not in st.session_state:
        st.session_state.result_df = None

    if not st.session_state.logged_in:        
        auth_menu = option_menu(
            menu_title=None,
            options=["로그인", "회원가입"],
            icons=["person-check", "person-plus"],
            orientation="horizontal",
        )

        if auth_menu == "로그인":
            left, mid, right = st.columns([1, 2, 1])
            with mid:
                st.title("로그인")
                id_input = st.text_input("ID")
                pass_input = st.text_input("PW", type="password")
                if st.button("로그인"):
                    if login_handler(id_input, pass_input):
                        st.session_state.logged_in = True
                        st.session_state.user_id = id_input
                        st.rerun()
                    else:
                        st.error("아이디 또는 비밀번호가 일치하지 않습니다.")

        elif auth_menu == "회원가입":
            left, mid, right = st.columns([1, 2, 1])
            with mid:
                st.title("회원가입")
                new_id = st.text_input("ID")
                new_pass = st.text_input("PW", type="password")
                confirm_pass = st.text_input("PW확인", type="password")
                if st.button("가입하기"):
                    if not new_id or not new_pass:
                        st.warning("아이디와 비밀번호를 모두 입력해주세요.")
                    elif new_pass != confirm_pass:
                        st.error("비밀번호가 일치하지 않습니다.")
                    else:
                        success, msg = signup_handler(new_id, new_pass)
                        if success: st.success(msg)
                        else: st.error(msg)
    else:
        # --- 앱 메인 화면 (로그인 후) ---
        with st.sidebar:            
            total_users = get_total_user_count()
            st.markdown(f"""
        <div style="margin-bottom: 10px;">
            <p style="font-size: 16px">전체 회원수: {total_users}명</p>            
        </div>""", unsafe_allow_html=True)
            #st.metric(label="전체 회원 수", value=f"{total_users}명")
            
            st.info(f"👤 {st.session_state.user_id}님 접속 중")
            if st.button("로그아웃"):
                st.session_state.update({"logged_in": False, "result_df": None, "user_id": None})
                st.rerun()
            
            st.divider()
            with st.expander("회원탈퇴"):
                st.warning("탈퇴 시 데이터가 삭제됩니다.")
                confirm_delete = st.checkbox("정말 탈퇴하시겠습니까?")
                if st.button("회원탈퇴 실행"):
                    if confirm_delete and delete_user_handler(st.session_state.user_id):
                        st.session_state.update({"logged_in": False, "result_df": None, "user_id": None})
                        st.rerun()

        st.markdown('<h3 style="font-size: 18px;">실거래데이터 조회</h3>', unsafe_allow_html=True)
        # (생략된 sido_data 정의가 필요함)
        # 예시: sido_data = {"서울특별시": {"강남구": ["역삼동", "삼성동"]}} 
        
        URL_KEYS = ["분양권", "아파트 매매", "아파트 전월세", "오피스텔 매매", "오피스텔 전월세", "연립/다세대 매매", "연립/다세대 전월세"]
        selected_type = st.radio("🔍 검색 항목 선택", URL_KEYS, horizontal=True, index=1)

        with st.container():
            col1, col2, col3, col4, col5 = st.columns([1.2, 1.2, 1.2, 1.5, 1.5])
            
            sido_list = sorted(list(sido_data.keys()))
            with col1:
                sido = st.selectbox("시도", options=sido_list, index=8)            
            # 시군구 선택 (sido가 결정된 후 안전하게 가져옴)
            with col2:
                # 더 이상 locals()를 체크할 필요 없이 직접 참조
                sigungu_opts = sorted(list(sido_data[sido].keys())) if sido in sido_data else []
                sigungu = st.selectbox("시군구", options=sigungu_opts, index=0)
                
            # 읍면동 선택
            with col3:
                if sido in sido_data and sigungu in sido_data[sido]:
                    dong_opts = ["전체"] + sorted(sido_data[sido][sigungu])
                else:
                    dong_opts = ["전체"]
                dong = st.selectbox("읍면동", options=dong_opts, index=1)
                
            with col4:
                sub1, sub2 = st.columns(2)
                ex_min = sub1.selectbox("전용(min)", [10, 20, 30, 40, 59, 84], index=4)
                ex_max = sub2.selectbox("전용(max)", [60, 75, 85, 100, 120, 150], index=2)
            with col5:
                deal_ymd = st.date_input("기준월(월말)>=", datetime.today() + MonthEnd(-2))

        btn_col, space, excel_col, _ = st.columns([1, 1, 1, 7])
        if btn_col.button("🚀 조회", use_container_width=True):
            try:
                engine = get_engine()

                # 지역 그룹 정의
                sma = ['서울특별시', '인천광역시', '경기도']
                big6 = ['부산광역시', '대구광역시', '대전광역시', '광주광역시', '울산광역시', '세종특별자치시']
                dodo = ['강원특별자치도', '충청북도', '충청남도', '전라특별자치도', '전라남도', '경상북도', '경상남도', '제주특별자치도']
                
                table_map = {
                    "분양권": "bunyang", "아파트 매매": "sale_sma", "아파트 전월세": "rent_sma",
                    "오피스텔 매매": "ot_sale", "오피스텔 전월세": "ot_rent",
                    "연립/다세대 매매": "villa_sale", "연립/다세대 전월세": "villa_rent"
                }
        
                # 테이블 분기 로직
                if selected_type == '아파트 매매':
                    if sido in big6: table_name = 'sale_big6'
                    elif sido in dodo: table_name = 'sale_dodo'
                    else: table_name = 'sale_sma'
                elif selected_type == '아파트 전월세' and sido not in sma:
                    table_name = 'rent_notsma'
                else:
                    table_name = table_map.get(selected_type, "sale_sma")
        
                # 쿼리 및 파라미터 구성 (딕셔너리 바인딩 방식)
                query = f"SELECT * FROM {table_name} WHERE 광역시도 = :sido AND 시자치구 = :sigungu AND 기준월 >= :deal_ymd"
                params = {
                    "sido": sido, "sigungu": sigungu, 
                    "deal_ymd": deal_ymd.strftime('%Y-%m-%d'),
                    "ex_min": ex_min, "ex_max": ex_max
                }
                
                if dong != "전체":
                    query += " AND 법정동 = :dong"
                    params["dong"] = dong
                query += " AND 전용면적 >= :ex_min AND 전용면적 <= :ex_max LIMIT 5000"
        
                with st.spinner('테이블 조회 중...'):
                    with engine.connect() as conn:
                        df = pd.read_sql(text(query), conn, params=params)
                
                # 데이터 정제 및 세션 저장
                if not df.empty:
                    df.drop('id', axis=1, inplace=True)                
                    st.session_state.result_df = df.reset_index(drop=True)
                else:
                    st.session_state.result_df = pd.DataFrame() # 빈 결과 저장        
                engine.dispose()
        
            except Exception as e:
                st.error(f"조회 중 오류 발생: {e}")

        # 결과 출력
        if st.session_state.result_df is not None:
            res_df = st.session_state.result_df
            if not res_df.empty:
                res_df = res_df[res_df.columns] 
                st.dataframe(res_df, use_container_width=True, height=600, hide_index=True)
                
                st.markdown(f"""
                    <div class="status-bar">
                        <span style='font-size: 16px; font-weight: bold;'>📊 검색 결과: </span>
                        <span style='font-size: 26px; color: white; font-weight: bold;'>{len(res_df):,}건</span>
                    </div>
                """, unsafe_allow_html=True)
                # 엑셀 다운로드
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    res_df.to_excel(writer, index=False)
                excel_col.download_button("엑셀다운", data=buffer.getvalue(), file_name=f"data_{datetime.now().strftime('%m%d_%H%M')}.xlsx")
            else:
                st.warning("데이터가 없습니다.")

if __name__ == "__main__":

    main()







