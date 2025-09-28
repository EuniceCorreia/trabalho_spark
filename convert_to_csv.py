import pandas as pd
import os

# Caminhos
xlsx_path = "data/raw/ips_brasil.xlsx"
csv_path = "data/raw/ips_brasil.csv"

if os.path.exists(xlsx_path):
    try:
        # Abrir o arquivo Excel e listar abas disponíveis
        excel_file = pd.ExcelFile(xlsx_path)
        print("Abas disponíveis no XLSX:", excel_file.sheet_names)
        
        # Ler a primeira aba (ajuste se necessário baseado no print acima)
        sheet_name = excel_file.sheet_names[0]  # Ex.: 'Sheet1' ou 'Municipios'
        df = pd.read_excel(xlsx_path, sheet_name=sheet_name, engine='openpyxl')
        
        # Limpar dados vazios (opcional, remove linhas totalmente vazias)
        df = df.dropna(how='all')
        
        # Salvar como CSV (separador vírgula, encoding UTF-8 para acentos em nomes BR)
        df.to_csv(csv_path, index=False, sep=',', encoding='utf-8')
        
        print(f"Conversão OK! CSV salvo em {csv_path}")
        print(f"Aba usada: {sheet_name}")
        print(f"Shape do DataFrame: {df.shape} (linhas x colunas)")
        print("Colunas principais:", df.columns.tolist()[:8])  # Mostra até 8 colunas
        print("\nPrimeiras 3 linhas:\n", df.head(3).to_string(index=False))
        
    except Exception as e:
        print(f"Erro na conversão: {e}")
        print("Dica: Edite o script e mude sheet_name para uma aba específica (ex.: 'Municipios').")
else:
    print(f"Erro: {xlsx_path} não encontrado. Verifique se a cópia funcionou.")
