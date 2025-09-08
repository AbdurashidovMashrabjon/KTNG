import pandas as pd
from tkinter import Tk, filedialog

def main():
    Tk().withdraw()
    file_path = filedialog.askopenfilename(
        title="Выберите Excel файл",
        filetypes=[("Excel files", "*.xlsx *.xls")]
    )
    if not file_path:
        print("❌ Файл не выбран")
        return

    # Читаем Excel без заголовков
    df_raw = pd.read_excel(file_path, header=None, dtype=str)

    # Ищем строку с настоящими заголовками (где есть слово "Пользователь" или "User")
    header_row = None
    for i, row in df_raw.iterrows():
        row_values = [str(x) for x in row.tolist()]
        if any("ользов" in val or "User" in val for val in row_values):
            header_row = i
            break

    if header_row is None:
        print("❌ Не удалось найти строку с заголовками")
        return

    # Читаем заново с правильными заголовками
    df = pd.read_excel(file_path, header=header_row, dtype=str)
    df.columns = df.columns.str.strip()

    print("✅ Заголовки колонок:", df.columns.tolist())

    # Определяем колонку Пользователь
    user_col = None
    for col in df.columns:
        if "ользов" in col or "User" in col:
            user_col = col
            break

    if not user_col:
        print("❌ Колонка Пользователь не найдена!")
        return

    # Добавляем Mobile если нет
    if "Mobile" not in df.columns:
        df["Mobile"] = ""

    merged_rows = []
    skip_next = False

    for i in range(len(df)):
        if skip_next:
            skip_next = False
            continue

        row = df.iloc[i].copy()

        # Проверяем следующую строку: если там только цифры → это телефон
        if i + 1 < len(df):
            next_val = str(df.iloc[i + 1][user_col]).strip()
            if next_val.isdigit():
                row["Mobile"] = next_val
                skip_next = True

        merged_rows.append(row)

    new_df = pd.DataFrame(merged_rows)

    # Сохраняем
    output_file = file_path.replace(".xlsx", "_August2.xlsx")
    new_df.to_excel(output_file, index=False)

    print(f"\n✅ Готово! Файл объединён и сохранён как: {output_file}")

if __name__ == "__main__":
    main()
