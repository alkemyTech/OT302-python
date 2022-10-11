from bs4 import BeautifulSoup 

# Reading the data inside the xml file to a variable under the name  data
with open('posts.xml', 'r', encoding='utf-8') as f:
    data = f.read() 

# Passing the stored data inside the beautifulsoup parser 
soup = BeautifulSoup(data) 
# print(soup)

rows = soup.get_attribute_list('Id')
# values = rows.get('AcceptedAnswerId')
print(rows)
# print(values)