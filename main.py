from locale import setlocale
import telebot

def rfile(namecom):
    file = open(f'txt/{namecom}.txt', 'r', encoding='utf-8')
    content = file.read()
    file.close()
    return content



token = ('8567842690:AAHoV1jUvx87BRoM1vFhYIyNx1u4kwMRz0I')
bot = telebot.TeleBot(token)

@bot.message_handler(commands=['start'])
def start_message(message):
    bot.send_message(message.chat.id, rfile('start'))

@bot.message_handler(commands=['help'])
def start_message(message):
    bot.send_message(message.chat.id, rfile('help'))


bot.polling(none_stop=True, interval=0)