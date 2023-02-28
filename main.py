import requests
from bs4 import BeautifulSoup

def get_html(url):
    result = requests.get(url)
    return result.text

def get_data(html):
    soup = BeautifulSoup(html, 'lxml')
    posts_count = soup.find_all('article', {'class': 'tm-articles-list__item'}).__len__()

    for i in range(posts_count):
        posts_of_page = soup.find_all('article', {'class': 'tm-articles-list__item'}).__getitem__(i)
        h2 = posts_of_page.find('h2')
        a = h2.find('a')

        post_header = a.text
        post_time = posts_of_page.find('time').get_attribute_list('title')
        post_link = 'https://habr.com' + a.get_attribute_list('href')[0]

        print('-'*100)
        print(i)
        print(post_header)
        print(post_time)
        print(post_link)

def main():
    html = get_html('https://habr.com/ru/flows/develop/top/weekly/')
    get_data(html)

if __name__ == '__main__':
    main()

