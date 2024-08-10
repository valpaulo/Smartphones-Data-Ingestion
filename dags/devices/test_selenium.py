from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time

service = Service(executable_path="../chromedriver")
driver = webdriver.Chrome(service=service)

driver.get('https://www.google.com/')

input_element = driver.find_element(By.CLASS_NAME, 'gLFyf')
input_element.send_keys('selenium' + Keys.ENTER)

time.sleep(1000)

driver.quit()