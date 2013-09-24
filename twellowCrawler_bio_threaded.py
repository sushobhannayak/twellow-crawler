import threading
import requests
from bs4 import BeautifulSoup
import re
import time, random, logging, traceback, string, sys

# set up logging
logging.basicConfig(filename="newTwellowCrawlerBio.log", format='%(asctime)s %(threadName)s %(levelname)s : %(message)s ', level=logging.INFO)

job_list = []
job_list_lock = threading.Lock()
user_bio = {}
user_lock_list = {}
user_lock_list_lock = threading.Lock()

def extractBios():
    global job_list
    global user_bio
    global job_list_lock
    global user_lock_list
    global user_lock_list_lock

    while True:
        time.sleep(random.randint(100,300))
        
        job_list_lock.acquire()
        if len(job_list) == 0:
            job_list_lock.release()
            return
        category, url = job_list.pop()
        job_list_lock.release()
        logging.info('grilling category-url pair: %s %s', category, url)

        base_url = url
        # Now go through all the users of the category and Store for each user their twitter handle and bio
        try:
            while(True):
                time.sleep(random.randint(0,50))
                # Get the web page
                # headers = {'User-agent':'Mozilla/5.0'}
                r = requests.get(url, timeout=20)
                logging.debug("Url request successful for url: %s", url)
                # Extract information from webpage. All the users are in
                # the div with class 'search-cat-user', and the first <a>
                # tag in the div has the twitter user handle as the href
                # field. Add tags and super-tags accordingly (super-tags
                # for each category are stored in the global dictionary).
                print r.status_code
                soup = BeautifulSoup(r.text)  
                all_users = soup.find_all(attrs={"class":"search-cat-user"})                            
                for user in all_users:                                                                  
                    try:                                                                                
                        user_id = user.find('a')['href'][1:]                                            
                        if (user_id == '') or (user_id in user_bio):                                                               
                            continue                                                                    
                        logging.debug('Operating on user: %s', user_id)
                        # Get user bio now
                        user_soup = BeautifulSoup(user.__str__())
                        [s.extract() for s in user_soup.find_all(attrs={'class':'search-cat-user-name'})]
                        found = user_soup.find(attrs={'class':'search-cat-user-bio'})
                        found = BeautifulSoup(found.__str__())
                        bio = re.compile('\n').sub('', found.string)

                        if user_id not in user_lock_list:
                            user_lock_list_lock.acquire()
                            # Test again if it hasn't already been added by another thread
                            if user_id not in user_lock_list:
                                user_lock_list[user_id] = threading.Lock()
                                logging.debug("Creating lock for user: %s", user_id)
                            user_lock_list_lock.release()

                        user_lock_list[user_id].acquire()
                        user_bio[user_id] = bio.strip().encode('ascii', 'ignore')
                        logging.info('user_id:%s bio:%s', user_id, bio.strip().encode('ascii', 'ignore'))
                        print 'user_id:', user_id, 'bio', bio.strip().encode('ascii', 'ignore')
                        user_lock_list[user_id].release()

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
                logging.debug("Finished operating on url: %s", url)
                if current_page == last_page:
                    logging.info("Last page operation finished for CATEGORY: %s", category)
                    break
                url = base_url+ '/page/' + str(current_page+1)
                logging.debug("Starting on the next page url: %s", url)
        except:
            logging.error(traceback.format_exc())



if __name__ == '__main__':
    
    
    # First, input jobs into the job_list
    logging.debug('Preparing the job list...')
    for line in open('category-url-mini.txt'):
        category, url = line.split(';;;;')
        job_list.append([category.strip(), url.strip()])
    logging.info("Job list prepared...")

    # Spawn a few threads here
    logging.debug("Dispatching threads...")
    threads = []
    for i in range(0,8):
        threads.append(threading.Thread(target=extractBios))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # extractBios()

    logging.info('FINAL LENGTH OF USER-BIO: %d', len(user_bio))
    # f = open('user_bio.dat', 'w')
    # f.write(user_bio.__str__())
    # logging.info('User bios saved to file!')
