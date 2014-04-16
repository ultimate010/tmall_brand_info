#!/usr/bin/env python
#-*-coding:utf-8-*-
import time
import datetime
from fetcher import fetcher
import sys
import lxml.html as H
import re
from pyhelper import pyhelper_add_helper, pyhelper_main

threadsNum = 4

LEIMU_XPATH = '//div[@class="catFilter"]/dl/dd/a/@href'
BRAND_LOGO_XPATH = '//div[@class="brandItem"]/div[@class="brandItem-info"]/div/p/a/img/@data-ks-lazyload'
BRAND_URL_XPATH = '//div[@class="brandItem"]/div[@class="brandItem-info"]/div/p/a/@href'
BRAND_TITLE_XPATH = '//div[@class="brandItem"]/div[@class="brandItem-info"]/div/p/strong/text()'
BRAND_DESC_XPATH = '//div[@class="brandItem"]/div[@class="brandItem-info"]/div/dl/dd/text()'
PATTERN_BRANDID = 'brandId=([0-9]+)'
PATTERN_YE = u'共([0-9]+)页'
PATTERN_INDID = "\?industryId=([0-9]+)&"
PATTERN_CATID = "&categoryId=([0-9]+)&"
PATTERN_ETID = "\&etgId=([0-9]+)"

def genLevelFirst(fet,urls):
    try:
        result = []
        taskCount = 0
        for url in urls:
            taskCount += 1
            fet.push(url)
        while taskCount > 0:
            taskCount -= 1
            url , content = fet.pop()
            fet.taskleft() #watch ans
            dom_lei = H.fromstring(content)
            classUrls = dom_lei.xpath(LEIMU_XPATH)
            if ( len(classUrls) == 0):
                sys.stderr.write("%s:Can not paser %s\n" % (datetime.datetime.now(),url))
            for u in classUrls:
                #raw_input()
                #print u
                result.append(u)
        return result
    except Exception as err:
        sys.stderr.write("%s:%s\n" % (datetime.datetime.now(),err))




def main():
    if len(sys.argv) != 2:
        print "Usage: python %s firstLevelurl  1> outfile 2>logFile" % sys.argv[0]
        exit(1)
    f = fetcher(threads=threadsNum)
    with open(sys.argv[1],'r') as fi:
        urls = fi.readlines()
        urls = map (lambda x: x.strip("\r\n") , urls)
        level1Urls = genLevelFirst(f,urls)
    for url in level1Urls:
        print url

@pyhelper_add_helper
def genLevelSecond(secondLevelurlsfile,outfile,thirdfile):
    f = fetcher(threads=threadsNum)
    with open(secondLevelurlsfile , 'r') as fi:
        urls = fi.readlines()
        urls = map (lambda x: x.strip("\r\n") , urls)
        ans = _genLevelSecond(f,urls,thirdfile)
    fout = open(outfile , 'w')
    for  url in ans:
        fout.write("%s\n" % url.encode('utf-8' , 'ignore'))
def _genLevelSecond(fet , urls,thirdfile):
    try:
        fthree = open(thirdfile , 'w')
        pye = re.compile(PATTERN_YE , re.M)
        pindustryid = re.compile(PATTERN_INDID , re.M)
        pcategoryid = re.compile(PATTERN_CATID , re.M)
        petgid = re.compile(PATTERN_ETID , re.M)
        pbrandid = re.compile(PATTERN_BRANDID , re.M)
        taskCount = 0
        for url in urls:
            taskCount += 1
            fet.push(url)
        while taskCount > 0:
            taskCount -= 1
            url , content = fet.pop()
            content = content.decode('gbk','ignore')
            did = pindustryid.findall(url)
            cid = pcategoryid.findall(url)
            eid = petgid.findall(url)
            #print content
            ans = pye.findall(content)
    #        print did[0]
    #        print cid[0]
    #        print eid[0]
            if(len(ans) > 0):
                fthree.write("%s\t%s\n" % (url , ans[0]))
            else:
                fthree.write("%s\tERRO\n" % (url))

            fet.taskleft() #watch ans
#            print content
            dom_lei = H.fromstring(content)

            logourls = dom_lei.xpath(BRAND_LOGO_XPATH)
            brandurls = dom_lei.xpath(BRAND_URL_XPATH)
            titles = dom_lei.xpath(BRAND_TITLE_XPATH)
            dess = dom_lei.xpath(BRAND_DESC_XPATH)
#            print brandurls[0]
#            print titles[0]
#            print dess[0]
#            print logourls[0]
#            raw_input()
            if (len(brandurls) == 0):
                sys.stderr.write("%s:Can not paser %s\n" % (datetime.datetime.now(),url))
            str = ''
            try:
                for i , _url  in enumerate(brandurls):
                    bid = pbrandid.findall(_url)
                    str = titles[i] + "\t" + dess[i] + "\t" + bid[0] + "\t" + cid[0] + "\t" + _url + "\t" + logourls[i]
#                   raw_input()
                    yield str
            except Exception as err:
                fthree.write("%s\tPASERERR\n" % (url))
                sys.stderr.write("%s:%s\t%s\n" % (datetime.datetime.now(),url , err))

    except Exception as err:
        sys.stderr.write("%s:%s\n" % (datetime.datetime.now(),err))



if __name__ == '__main__':
    pyhelper_main()
    #main()
