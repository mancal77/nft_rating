from time import sleep
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

options = Options()
options.headless = True
options.add_argument("--window-size=1920,1200")


DRIVER_PATH = './chromedriver'
driver = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)
driver.get("https://rarity.tools/upcoming")

# Sleep needed to wait till page loaded
sleep(5)
soup = bs(driver.page_source, 'html.parser')

projects_full_list = []
projects = soup.find_all('tr', class_="text-left text-gray-800")
for project in projects:
    project_name = str(project.find('div', 'text-lg font-bold text-pink-700 dark:text-gray-300')).split('600px;">\n\t\t\t')[1].split('<!--')[0]
    project_description = str(project.find_all('div', attrs={'style':'max-width: 600px;'})[1]).split('600px;">\n\t\t\t\t')[1].split('\n\t\t\t</div')[0]
    for idx, a in enumerate(project.find_all('a', href=True)):
        if idx == 0:
            discord = a['href']
        elif idx == 1:
            twitter = a['href']
        elif idx == 2:
            project_url = a['href']
    projects_full_list.append((project_name, project_description, discord, twitter, project_url))
print(projects_full_list)
driver.quit()
