#담당 : 이승석
#PVI 계산 
import pandas as pd
import numpy as np
import os
import pvi_preprocessing  #전처리 모듈 가져옴.

def calculate(df): 
    # 칼럼 통일
    df_calc = df.copy()
    df_calc['SGI(노출)'] = df['SGI'] * -1
    df_calc['생활용수(민감)'] = df['생활용수']
    df_calc['농경지(민감)'] = df['농경지']
    df_calc['미보급률(적응)'] = 100 - df['보급률']
    df_calc['누수율(적응)'] = 100 - df['유수율']
    
    # 계산에 쓸 수치 데이터만 분리
    indicators = df_calc[['SGI(노출)', '생활용수(민감)', '농경지(민감)', '미보급률(적응)', '누수율(적응)']]

    # 정규화
    df_norm = (indicators - indicators.min()) / (indicators.max() - indicators.min() + 1e-9)

    # 엔트로피 가중치 계산
    m = len(df)
    k = 1 / np.log(m)
    p_ij = df_norm / df_norm.sum(axis=0)
    
    # p_ij가 0일 때 log(0) 에러 방지
    entropy_matrix = p_ij * np.log(p_ij + 1e-9)
    entropy = -k * entropy_matrix.sum(axis=0)
    
    d_j = 1 - entropy
    weights = d_j / d_j.sum()

    print("\n[엔트로피 가중치]")
    print(weights)

    # PVI 점수 산출
    df_result = df[['도시']].copy()
    df_result['PVI_Score'] = df_norm.dot(weights)
    
    # PVI 점수 0~1 정규화
    df_result['PVI_Final'] = (df_result['PVI_Score'] - df_result['PVI_Score'].min()) / \
                             (df_result['PVI_Score'].max() - df_result['PVI_Score'].min())

    # 저장
    if not os.path.exists('results'):
        os.makedirs('results')
        
    save_path = 'data/pvi_result_final.csv'
    df_result.to_csv(save_path, index=False, encoding='utf-8-sig')
    
    print(f"[완료]")
    return df_result

if __name__ == "__main__":
    print("실행")
    df_input = pvi_preprocessing.load_and_preprocess()
    if df_input is not None:
        result = calculate(df_input)

    else:
        print("\n[ERROR]")