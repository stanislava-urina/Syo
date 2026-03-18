def read(filename):
    file = open(filename, "r", encoding='utf-8')
    content=file.read()
    file.close()
    return content

def write(filename, id, phone):
    with open(filename, 'a') as file:
        file.write(f'\n{id} {phone}')
    file.close()


def read_id(filename):
    file = open(filename, "r", encoding='utf-8')
    lines = file.readlines()
    file.close()

    s = {}
    for i in lines:
        i = i.strip()
        if i:
            line = i.split()
            if len(line) > 1:
                s[line[0]] = line[-1]
    return s

def read_simple(filename):
    file = open(filename, "r", encoding='utf-8')
    lines = file.readlines()
    file.close()
    return lines

def write_id(filename, id):
    with open(filename, 'a') as file:
        file.write(f'\n{id}')
    file.close()


def remove_from_file(filename, user_id):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        with open(filename, 'w', encoding='utf-8') as f:
            for line in lines:
                if line.strip() != str(user_id):
                    f.write(line)
        return True
    except FileNotFoundError:
        return False
    except Exception as e:
        print(f"Ошибка при удалении из файла: {e}")
        return False
