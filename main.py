import csv
from itertools import izip

from concurrent.futures import ThreadPoolExecutor, as_completed
from lxml import html
import requests

BASE_URL = "https://www.bowlingball.com"



def get_detail_page_urls():
    res = requests.get(BASE_URL + '/shop/all/bowling-balls/', params={'limit': 999999, 'ft1': 'i', 'fv1': 1})
    res.raise_for_status()
    tree = html.fromstring(res.content)
    links = []
    for anchor in tree.xpath('//div[@class="product_info_block"]/a'):
        links.append(BASE_URL + anchor.attrib['href'].strip())
    return links

def get_specs(url):
    print 'getting page %s' % url
    res = requests.get(url)
    res.raise_for_status()

    tree = html.fromstring(res.content)
    specs = {}
    specs['name'] = tree.xpath('//h1[@class="ProductNameText"]')[0].text_content().strip()
    specs['url'] = url
    for row in tree.xpath('//table[@class="specs_table"]/tr'):
        for key_element, val_element in pairwise(row.getchildren()):
            val = val_element.text_content().strip().encode('utf8')
            if val:
                specs[key_element.text_content().strip()] = val
    return specs

def pairwise(iterable):
    """s -> (s0, s1), (s2, s3), (s4, s5), ...

    copied from https://stackoverflow.com/a/5389547/309616
    """
    a = iter(iterable)
    return izip(a, a)


def main():
    detail_page_urls = get_detail_page_urls()
    all_keys = set()
    all_specs = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_detail_pages = [executor.submit(get_specs, url) for url in detail_page_urls]
        for future in as_completed(future_detail_pages):
            try:
                specs = future.result()
            except Exception as e:
                print '%s generated an exception: %s' % (url, e)
            else:
                print 'done processing %s' % specs['name']
                all_keys = all_keys.union(set(specs))
                all_specs.append(specs)

    with open('specs.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=list(all_keys))
        writer.writeheader()
        for specs in all_specs:
            try:
                writer.writerow(specs)
            except Exception as e:
                print '%s generated an exception: %s' % (specs[e], e)


if __name__ == "__main__":
    main()

