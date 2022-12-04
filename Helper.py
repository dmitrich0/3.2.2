import os


class Helper:
    @staticmethod
    def parse_year_from_date_slice(date: str) -> int:
        return int(date[:4])

    @staticmethod
    def get_filenames_from_dir(dir_name: str) -> list[str]:
        return list(map(lambda x: f"./{dir_name}/" + x, list(os.walk(f".//{dir_name}"))[0][2]))
