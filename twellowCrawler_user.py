import logging
import threading
import time
import requests
from bs4 import BeautifulSoup
import string
import traceback
import sys

# set up logging
logging.basicConfig(filename="twellowCrawler.log", format='%(asctime)s %(threadName)s [%(thread)d] %(levelname)s : %(message)s ', level=logging.DEBUG)
group_mapping = {}
job_list = []
job_list_lock = threading.Lock()
user_list = {}
user_lock_list = {}
user_lock_list_lock = threading.Lock()

def extractCategories():
    """ Extracts all possible categories in twellow. Takes as input the file 'Twellow.html' which has all the listings for the categories. Using BeautifulSoup, it then extracts all the categories and their urls and puts then in a text file. It also creates a dictionary that tells us the relationship between categories and sub-categories."""
    
    logging.info("Extracting all possible categories in twellow...")
    global group_mapping
    f = open('category-url.txt','w')
    soup = BeautifulSoup(open('Twellow.html'))
    data = soup.find(id="listings")
    for i in range(0,len(data.contents)):
        try:
            item = data.contents[i]
            all_a = item.find_all('a')
            f.write(string.join([all_a[0].string.encode('ascii', 'ignore'), ';;;;', all_a[0]['href'], '\n']))
            super_category = all_a[0].string.encode('ascii', 'ignore')
            group_mapping[super_category] = super_category
            for idx in range(1,len(all_a)):
                f.write(string.join([all_a[idx].string.encode('ascii', 'ignore'), ';;;;', all_a[idx]['href'], '\n']))
                group_mapping[all_a[idx].string.encode('ascii', 'ignore')] = super_category

        except:
            logging.error("Error at item: %s : %s", item, traceback.format_exc())
    logging.info('Finished processing all categories...')
    fw = open('category_mapping.py','w')
    fw.write(group_mapping.__str__())
    logging.info('Sub-group and super-group mapping saved to file...')

def extractFollowersThread():
    global job_list
    global job_list_lock
    global group_mapping
    global user_list
    global user_lock_list
    global user_lock_list_lock

    while(True):
        # Sleep for a few minutes to prevent too much load on the server.
        time.sleep(random.randint(150,300))
        # Pop a job out of the job list. Each job is a (category,url) pair.
        job_list_lock.acquire()
        if len(job_list) == 0:
            job_list_lock.release()
            return
        category, url = job_list.pop()
        job_list_lock.release()
        logging.info('grilling category-url pair: %s %s', category, url)

        base_url = url
        # Now go through all the users of the category and Store for each user their twitter handle and the groups they belong to, including the super-group.
        try:
            while(True):
                # Get the web page
                time.sleep(random.randint(0,50))
                r = requests.get(url, timeout=20)
                logging.info("Url request successful for url: %s", url)
                # Extract information from webpage. All the users are in the div with class 'search-cat-user', and the first <a> tag in the div has the twitter user handle as the href field. Add tags and super-tags accordingly (super-tags for each category are stored in the global dictionary).
                soup = BeautifulSoup(r.text)                                                            
                all_users = soup.find_all(attrs={"class":"search-cat-user"})                            
                for user in all_users:                                                                  
                    try:                                                                                
                        user_id = user.find('a')['href'][1:]                                            
                        if user_id == '':                                                               
                            continue                                                                    
                        logging.info('Operating on user: %s', user_id)
                        if user_id not in user_lock_list:
                            user_lock_list_lock.acquire()
                            # Test again if it hasn't already been added by another thread
                            if user_id not in user_lock_list:
                                user_lock_list[user_id] = threading.Lock()
                                logging.info("Creating lock for user: %s", user_id)
                            user_lock_list_lock.release()
                        user_lock_list[user_id].acquire()
                        if user_id not in user_list:
                            logging.info("This user not in dict: %s", user_id)
                            user_list[user_id] = {'name': user.find('a')['title']}                                          
                            user_list[user_id]['super_tags'] = []                                                           
                            user_list[user_id]['tags'] = []                                                                 
                        user_list[user_id]['tags'].append(category)                                     
                        if group_mapping[category] not in user_list[user_id]['super_tags']:             
                            user_list[user_id]['super_tags'].append(group_mapping[category])            
                        user_lock_list[user_id].release()
                        logging.info("Updated user info: %s", user_list[user_id].__str__())
                    except:                              
                        logging.error("Error in: %s", user.find('a'))                                              
                        logging.error(traceback.format_exc())
                        # Release any unreleased locks: if already released, would through an exception - hence the try-catch blocks
                        try:
                            user_lock_list_lock.release()
                        except:
                            pass
                        try:
                            user_lock_list[user_id].release()
                        except:
                            pass
        
                # When all the users of a page are done, go to the next page and next till you reach the last page, which is verfied from the information at the bottom of the page. 
                pagination = soup.find(attrs={"class":"pagination"})
                last_page = int(pagination.find(attrs={'title':'Last Page'})['href'].split('/')[-1])
                current_page = int(pagination.find(attrs={'class':'current'})['href'].split('/')[-1])
                logging.info("Finished operating on url: %s", url)
                if current_page == last_page:
                    logging.info("Last page operation finished for CATEGORY: %s", category)
                    break
                url = base_url+ '/page/' + str(current_page+1)
                logging.info("Starting on the next page url: %s", url)
        except:
            logging.error(traceback.format_exc())
    
    

if __name__ == '__main__':
    
    extractCategories()
    
    # Create a threaded routine that calls extract followers multiple times to get all the followers in all the categories

    # First, input jobs into the job_list
    logging.info('Preparing the job list...')
    for line in open('category-url.txt'):
        category, url = line.split(';;;;')
        job_list.append([category.strip(), url.strip()])
    logging.info("Job list prepared...")

    # Spawn a few threads here
    logging.info("Dispatching threads...")
    threads = []
    for i in range(0,12):
        threads.append(threading.Thread(target=extractFollowersThread))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    logging.info('FINAL LENGTH OF USER-LIST: %d', len(user_list))
    f = open('user_list.dat', 'w')
    f.write(user_list.__str__())
    logging.info('User list saved to file!')
