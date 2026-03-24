from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
import pandas as pd
from sqlalchemy import create_engine

def add_test_player(data):
    print("ป้อนข้อมูลนักเตะใหม่สำหรับทดสอบ (หรือกด Enter เพื่อข้าม):")
    name = input("ชื่อนักเตะ: ").strip()
    if not name:
        return data
    
    team_id = int(input("Team ID (1-20): "))
    goals = int(input("จำนวนประตู: "))
    minutes = int(input("นาทีที่ลงเล่น: "))
    rating = float(input("Average Rating: "))
    assists = int(input("จำนวนแอสซิสต์: "))
    dreamteam = int(input("Player of the Week count: "))
    position_id = int(input("Position ID (1=GK, 2=DEF, 3=MID, 4=FWD): "))
    birth_year = int(input("ปีเกิด: "))
    salary_cost = float(input("Salary cost (FPL units): "))
    
    new_player = {
        'web_name': name,
        'team': team_id,
        'goals_scored': goals,
        'minutes': minutes,
        'points_per_game': rating,
        'assists': assists,
        'dreamteam_count': dreamteam,
        'element_type': position_id,
        'birth_date': f"{birth_year}-01-01",
        'now_cost': salary_cost
    }
    data['elements'].append(new_player)
    print(f"เพิ่มนักเตะ {name} สำเร็จ!")
    return data

def run_pipeline():
    data = extract_data()
    data = add_test_player(data)
    data = transform_data(data)
    load_data(data)

def show_result():
    engine = create_engine("postgresql://airflow:airflow@localhost:5432/airflow")
    df = pd.read_sql("SELECT * FROM top_scorers", engine)
    df.to_csv("top_scorers.csv", index=False)
    print(df.to_string(index=False))

if __name__ == "__main__":
    run_pipeline()
    show_result()