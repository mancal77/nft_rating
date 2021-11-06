from time import sleep
from bs4 import BeautifulSoup as bs

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os

options = Options()
options.headless = True
options.add_argument("--window-size=1920,1200")


DRIVER_PATH = './chromedriver'
driver = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)
driver.get("https://rarity.tools/upcoming")
sleep(5)
# print(driver.page_source)
# text = driver.page_source.split('\n')
soup = bs(driver.page_source, 'html.parser')

projects_full_list = []
projects = soup.find_all('tr', class_="text-left text-gray-800")
for project in projects:
    project_name = project.find('div', 'text-lg font-bold text-pink-700 dark:text-gray-300')
    project_description = project.find('div', 'max-width: 600px;')
    # discord =
    # twitter =
    # project_url =
    projects_full_list.append((project_name, project_description))
print(projects_full_list)
# print(soup)
driver.quit()
