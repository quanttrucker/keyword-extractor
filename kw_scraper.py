import requests
import pandas as pd
import os
from tqdm import tqdm
#import tldextract
import requests
from datetime import datetime as dt

def get_headers(cookie):
    headers = {
        "Accept": "application/json, text/plain, */*",
        'content-type': 'application/json; charset=utf-8',
        'Cookie': cookie}
    return headers


#scrape alexa.com for keywords
def get_site_keywords(url_domain, headers, sort_param='competition_paid', per_page=200):
    r =  requests.get(f'https://www.alexa.com/pro/keywords/site/table/{url_domain}?order=descending&sort_by={sort_param}&per_page={per_page}',
                      headers = headers)

    if r.status_code == 200:
        site_data = r.json()
        site_keywords = pd.DataFrame(site_data['result'])
        site_keywords['pixel_domain'] = url_domain
        return site_keywords
    else:
        print(f'Unable to parse url = {url_domain}')
        return []

def save_parsed_data_to_s3(out_path, data):
    file_name = "scraped_{}".format(dt.now()).replace(':', '.')
    data.to_pickle(out_path+file_name+'.pkl')

def scrape_domains(domains_to_process, out_path, headers):
    result_data = {}
    result_data['site_keywords'] = {}
    try:
        for domain in tqdm(domains_to_process):
            result_data[domain] = {}
            site_keywords = get_site_keywords(domain, headers)
            if len(site_keywords):
                result_data['site_keywords'][domain] = site_keywords
            site_keywords_all = pd.concat(result_data['site_keywords'].values())
            save_parsed_data_to_s3(out_path, site_keywords_all)
            print("Success")
            return True
    except:
        print("Failed to scrape the domains!")
        return False


if __name__ == '__main__':
    num_domains_per_time = 4
    unparsed_path=os.getcwd()+"\\unscraped\\pickled_cats_cleaned.pkl"
    scraped_path=os.getcwd()+"\\scraped\\"
    cookie = 'attr_first={"source":"(direct)","medium":"(none)","campaign":"(not set)","term":"(not provided)","content":"(not set)","lp":"try.alexa.com/keyword-research","date":"2020-04-24","timestamp":1587732330601}; _ga=GA1.2.1190425803.1587732331; __auc=ca52b5ac171ac38b940cdaaeb3c; em_cdn_uid=t=1622025577838&u=e86077bc342347568dd9dcedeffdeb8d; AgencyBarSeen=true; G_ENABLED_IDPS=google; _compw={"rank":"6m","engagement":"6m"}; session_www_alexa_com=705911ad-7aae-4d28-9f33-ff3a1c5829de; _gid=GA1.2.556359508.1626695059; rpt=!; session_www_alexa_com_daily=1626871948; attr_last={"source":"(direct)","medium":"(none)","campaign":"(not set)","term":"(not provided)","content":"(not set)","lp":"www.alexa.com/pro/dashboard","date":"2021-07-21","timestamp":1626873798206}; __asc=c3e23acd17ac93c1e413f044db4; _gat_UA-2146411-1=1; _gat_UA-2146411-12=1; alexa_user_lifecycles={"undefined":"prospect","ML6ZrwCX0IT-GwoNu4STP8xudvwl9v_VyFX1wXpK6WQ":"paying","4RqnVXXF5UcNs5hct4uS7oNWnnk8_lX6dYgjazKugHg":"trial","YiOF9nNAtC8PqDSOYb1xidlfrFHM0UrPIo9bm1JC1zw":"prospect"}; mp_23564df485f0237ed31a0187a9aa3aad_mixpanel={"distinct_id": "es2MokZM7WJ7cIUKDU9JeaJn0_B7stkCJRr6F4gzeks","$device_id": "17a3d874ccd532-0bc7085b6817c8-6373267-1fa400-17a3d874cce4a3","$initial_referrer": "https://www.alexa.com/pro/dashboard","$initial_referring_domain": "www.alexa.com","$user_id": "es2MokZM7WJ7cIUKDU9JeaJn0_B7stkCJRr6F4gzeks","logged_in": true,"lifecycle_stage": "prospect","highest_subscription": "Advanced","agency": false,"source_first": "(direct)","medium_first": "(none)","campaign_first": "(not set)","content_first": "(not set)","lp_first": "try.alexa.com/keyword-research","date_first": "2020-04-24","source_last": "(direct)","medium_last": "(none)","campaign_last": "(not set)","content_last": "(not set)","lp_last": "www.alexa.com/pro/dashboard/subscription-offer","date_last": "2021-06-25"}; session_key=EOYrf2mPKqokzTjYsUVCXCqqaxTJRpO/EKn+TzZF5Z6QgjC6nQOKuN4IZ6mhA69lnEsw+cdflWkcUbBGK7cT+wsTswKRtVrH1vzaBxgCSei8wWZuxTHYAjnQ/B6v2rOKux9wwrWTJ6jC8Z2+bh9DzMivCu1UUmqkvL4XS+ewDcAOPrHKYVDwjqCjPV2cunbRcNXPOtUIQI+750a20ffhmtQwdHrP+PB8rcsWDHoC/P300SYz9kegXeC1NbpG+de9znDgDmgNM4zp1w2aDhPdXw==; lv=1626873807; jwtScribr=eJyrViopVrIyNDMyszA3NzGw1DMxMNJRys0BihkY6CgVZ6YApZUsDE1NzQz1ElPKEvOSU1OUQBIlqSAZoCo9IDcZxleqBQBovhXV.f8heLbzfYmZuJu6x403JQxyGCJ12UcP5+jXZRYm+S5A'
    #cookie='_ga=GA1.2.1677986153.1627230993; _gid=GA1.2.28075695.1627230993; attr_first={"source":"(direct)","medium":"(none)","campaign":"(not set)","term":"(not provided)","content":"(not set)","lp":"www.alexa.com/","date":"2021-07-25","timestamp":1627230994567}; attr_last={"source":"(direct)","medium":"(none)","campaign":"(not set)","term":"(not provided)","content":"(not set)","lp":"www.alexa.com/","date":"2021-07-25","timestamp":1627230994567}; __asc=a31ccad017ade868512b609d452; __auc=a31ccad017ade868512b609d452; _alx_ss={"status":0}; session_www_alexa_com_daily=1627231487; captcha=; session_www_alexa_com=c660a25b-d799-4fb9-abf2-8cf6710c2c11; AgencyBarSeen=true; AgencyBarNotYetExpired=true; session_key=IRL8wQFcumpz7G5u+5HVK1MbAk1N/iiegHda7zEviqUioR2z9GOl2Y3dEXbOjp+ML2pxlEAjHkqumdLSm/9cj4K6BLQRrpj/9VuVUCHc9uY2iZiIcCPg3uuoygqpEkfp5hTnuLo+ItReS48zc2xhMo7A041HzV7B3wuYLtQfmx8zlnZ10O+CCt1gNANWRg1UCdZwrKTl/7/sGkLsbpMU/QpQjwYXikAZi+whPdZfF/6BN3vVjumzMzvulwwN1kDMsMyoq22Saw3SVAf8krcdPg==; alexa_user_lifecycles={"undefined":"prospect","qUkNDYDqHCuw51goDjmIAtN3qxyUJsQy5s5oFhqwzwo":"prospect","YiOF9nNAtC8PqDSOYb1xidlfrFHM0UrPIo9bm1JC1zw":"prospect"}; lv=1627231651; jwtScribr=eJyrViopVrIyNDMyNzI2NTY00TMzMtFRys0BihkY6CgVZ6YApZUsDE1NzQz1ElPKEvOSU1OUQBIlqSAZoCo9IDcZxleqBQBl3xXL.kEOweIkC_UjAjl2xysYImmYx+cgJldUil1b2eqB1Ios; mp_23564df485f0237ed31a0187a9aa3aad_mixpanel={"distinct_id": "es2MokZM7WJ7cIUKDU9JeaJn0_B7stkCJRr6F4gzeks","$device_id": "17ade8e27caa-0a56326005e679-2343360-e1000-17ade8e27cb291","$initial_referrer": "https://www.alexa.com/pro/dashboard/trial-offer","$initial_referring_domain": "www.alexa.com","$user_id": "es2MokZM7WJ7cIUKDU9JeaJn0_B7stkCJRr6F4gzeks","logged_in": true,"lifecycle_stage": "prospect","highest_subscription": "Advanced","agency": false}'
    headers = get_headers(cookie)

    unscraped_domains = list(pd.read_pickle(unparsed_path).index)
    domains_to_process = unscraped_domains[:num_domains_per_time]
    result = scrape_domains(domains_to_process, scraped_path, headers)
    if result:
        net_unscraped = list(set(unscraped_domains)-set(domains_to_process))
        df_net_unscraped = pd.DataFrame(net_unscraped).rename(columns={0:'url_domain'})
        df_net_unscraped.set_index('url_domain').to_pickle(unparsed_path)