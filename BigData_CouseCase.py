
# 大数据分析案例  <基金数据分析>
# 版权所有 余力 buaayuli@ruc.edu.cn
# 在源程序文件夹下有data文件夹，存放各基金价格Excel文件 #

if "import模块 --------------------------------------------------------------------":
    import numpy as np
    import pandas as pd
    from random import shuffle
    import matplotlib.pyplot as plt
    from pandas.plotting import register_matplotlib_converters
    register_matplotlib_converters()
    from tkinter import ttk
    from tkinter import *
    import tkinter as tk
    import os, re, requests,logging
    from bs4 import BeautifulSoup

if "抓取_全部基金信息总表 --------------------------------------------------":
    # 知识点：数据抓取、函数、数据类型、语句结构
    def crawl_whole_info_table():
        if "0、网上基金信息样例-------":
            pass
            #  一个item_list，也就是一个基金的所有信息
            #  ['000227', '华安年年红债券A', 'HANNHZQA', '2020-02-14', '1.0680', '1.4730', '', '1.6175', '2.8865', '4.2661', '6.3655',
            #   '9.3036', '17.2048', '20.9574', '2.9838', '55.7880', '2013-11-14', '6', '', '0.60%', '0.06%', '1', '0.06%', '1','30.5295']

        if "1、网上抓取--------------------":
            try:
                session = requests.session()
                session.headers[
                    "Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
                session.headers["Accept-Encoding"] = "gzip, deflate, br"
                session.headers["Accept-Language"] = "zh-CN,zh;q=0.9"
                session.headers["Connection"] = "keep-alive"
                session.headers["Upgrade-Insecure-Requests"] = "1"
                session.headers[
                    "User-Agent"] = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"

                session.headers["Host"] = "fund.eastmoney.com"
                session.headers["Referer"] = 'http://fund.eastmoney.com/data/fundranking.html'

                fund_list = []
                for fund_type in ['hh', 'gp', 'zq', 'zs']:
                    url = "http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&ft=%s&rs=&gs=0&qdii=&tabSubtype=,,,,,&pn=10000" % (
                        fund_type)
                    response = session.get(url, verify=False, timeout=60)
                    response.encode = 'utf8'
                    response = response.text
                    response = response[response.find('var rankData = {datas:['): response.find(']')]
                    response = response.replace("var rankData = {datas:[", "")

                    for i in response.split('"'):
                        if len(i.split(',')) < 10:
                            continue
                        order = i.split(',')
                        for i in range(5, 16):
                            if "." in order[i]:
                                order[i] = float(order[i])
                            else:
                                order[i] = 0
                        if len(order[0]) < 6:
                            order[0] = "0" * (6 - len(order[0])) + order[0]
                        fund_list.append(order)

            except Exception as ex:
                logging.exception(str(ex))

        if "2、创建pandas数据表-----------------":
            title = ["代码", "名称", "英文", "日期", "单位净值", "累积净值", "日增长", "近1周", "近1月", "近3月", "近6月", "近1年", "近2年", "近3年",
                     "今年来",
                     "成立来", "成立时间", "未知", "成立来2", "折前手续费", "手续费", "折数", "手续费2", "折数2", "未知2"]
            fund_df = pd.DataFrame(fund_list, columns=title)
            fund_df = fund_df.iloc[:,:17]
            fund_df.to_csv("All_fund_info.csv", index=False, encoding='gbk')
        return fund_df
    if "test" and 1:
        print("Testing FUNCTION crawl_whole_info_table() ...")
        df = crawl_whole_info_table()
        print("The whole fund info table is in the following.")
        print(df)

if "抓取_单个基金历史价格 --------------------------------------------------":
    #知识点：数据抓取、Pandas、文件保存
    def crawl_one_fund_price(code, per=48, sdate='', edate=''):
        if "1、获取总页数 ---------------":
            url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx'
            params = {'type': 'lsjz', 'code': code, 'page': 1, 'per': per, 'sdate': sdate, 'edate': edate}
            rsp = requests.get(url, params)
            rsp.raise_for_status()
            html = rsp.text
            pattern = re.compile(r'pages:(.*),')
            pages = int( re.search(pattern, html).group(1) )

            heads = []
            soup = BeautifulSoup(html, 'html.parser')
            for head in soup.findAll("th"):
                heads.append(head.contents[0])

        if "2、开始抓取 -----------------":
            records = []
            page = 1
            while page <= pages:
                params = {'type': 'lsjz', 'code': code, 'page': page, 'per': per, 'sdate': sdate, 'edate': edate}
                rsp = requests.get(url, params=params)
                rsp.raise_for_status()
                html = rsp.text
                soup = BeautifulSoup(html, 'html.parser')
                for row in soup.findAll("tbody")[0].findAll("tr"):
                    row_records = []
                    for record in row.findAll('td'):
                        val = record.contents
                        # 处理空值
                        if val == []:
                            row_records.append(np.nan)
                        else:
                            row_records.append(val[0])
                    records.append(row_records)
                page = page + 1

            if len(records) == 0:
                records = [[np.nan, "0", "0", "0", "0", 0, 0, 0]]

        if "3、得到数据表 ---------------":
            np_records = np.array(records)
            data = pd.DataFrame()
            for col, col_name in enumerate(heads):
                data[col_name] = np_records[:, col]

            data['单位净值'] = data['单位净值'].astype(float)
            data['累计净值'] = data['累计净值'].astype(float)
            data['日增长率'] = data['日增长率'].str.strip('%').astype(float)
            data = data.sort_values(by='净值日期', axis=0, ascending=True).reset_index(drop=True)

        if "4、保存到文件 ":
            file = u'data/{}.xlsx'.format(code)
            data.to_excel(file, index=False, encoding='gbk')
            return  data
    if "test" and 0:
        print("Testing FUNCTION crawl_one_fund_price() ...")
        price_df=crawl_one_fund_price("002939")
        print("The fund history price is in the following.")
        print(price_df)

if "文件_读取data文件夹下基金号-----------------------------------------":
    # 知识点：函数、序列、文件夹（不作要求）、循环
    def read_filenames_from_folder(folder):
        filename_list = []
        for root, dirs, files in os.walk(folder):
            for f in files:
                portion = os.path.splitext(f)
                if portion[1] == ".xlsx" and len(portion[0]) == 6:
                    filename_list.append(portion[0])
        # print(filename_list)
        return filename_list
    if "test" and 0:
        all_codes = read_filenames_from_folder("data")  # 这条语句可以读取并返回data文件夹下的所有基金代码
        print("基金个数", len(all_codes))
        print(all_codes)

if "文件_获取所有基金号------------------------------------------------------":
    # 知识点：文件读取
    def get_all_fund_codes():
        if not os.path.exists("All_fund_info.csv"):
            crawl_whole_info_table()
        fund_pd=pd.read_csv("All_fund_info.csv", encoding='gbk')
        codes=fund_pd["代码"].tolist()
        return codes
    if "test" and 0:
        print("Testing FUNCTION get_all_fund_codes() ...")
        all_codes =get_all_fund_codes()
        print("总基金个数", len(all_codes))
        print(all_codes)

if "可视化_数据表的窗口------------------------------------------------------":
    #知识点：循环、序列、对象、类、Pandas、
    def treeview_dataframe_general(df, tableinfo=""):
        if "1、窗口基本属性 -------------":
            min_num = 29 if len(df) > 29 else len(df)
            win_width = len(df.columns) * 110 + 100
            win_width = 1300 if win_width > 1300 else win_width
            win_high = len(df) * 20 + 100
            win_high = 600 if win_high > 600 else win_high

            win = tk.Tk()
            # win = tk.Toplevel()
            win.geometry(str(win_width) + "x" + str(win_high))
            win.title(tableinfo)
            win.resizable(width=True, height=True)

            tk.Label(win, text=tableinfo + " 共" + str(len(df)) + "行", font=('微软雅黑', 14), width=60,
                     height=2).pack()

        if "2、创建表格窗体 -------------":
            tree = ttk.Treeview(win, height=min_num, show="tree headings")

            # "增加滚动条":
            vsb = ttk.Scrollbar(win, orient="vertical", command=tree.yview)
            vsb.pack(side='right', fill='y')
            tree.configure(yscrollcommand=vsb.set)

        if "3、表格行列添加 -------------":
            # 列设置
            tree["columns"] = tuple(["index"] + list(df.columns))
            tree.column("#0", width=0, anchor="center")
            tree.column("index", width=40, anchor="center")

            column_width = int((win_width - 50) / len(df.columns))
            for col in df.columns:  # 增加列
                tree.column(col, width=column_width, anchor="center")
                tree.heading(col, text=col, anchor="center")

            # 行设置
            for x in range(len(df)):  # 增加行
                item = [list(df.index)[x]]
                for col in df.columns:
                    item.append(df.iloc[x][col])
                tree.insert("", x, text=str(x + 1), values=item)

            tree.pack()
            win.mainloop()
    if "test" and 0:
        print("Testing FUNCTION crawl_whole_info_table() ...")
        Dataset = crawl_whole_info_table()
        treeview_dataframe_general(Dataset)

if "可视化_基金价格曲线------------------------------------------------------":
    # 知识点：循环、文件读取、函数、pandas、matplotlib
    def figure_fund_price_history(codes, start_day="1000-01-01", end_day="3000-01-01"):  # 重要
        if "读取基金价格--------------------------------":
            codes_dict = {}
            for code in codes:

                df = pd.read_excel(u"data/" + code + '.xlsx')
                df = df.reindex(columns=["净值日期", "单位净值", "日增长率", "累计净值"])

                if df.iloc[0, 0] > start_day:
                    start_day = df.iloc[0, 0]
                if df.iloc[len(df) - 1, 0] < end_day:
                    end_day = df.iloc[len(df) - 1, 0]

                df = (df[np.array(df['净值日期'] >= start_day) & np.array(df['净值日期'] <= end_day)]).copy()
                df['净值日期'] = pd.to_datetime(df['净值日期'], format='%Y-%m-%d')
                codes_dict[code] = df

        if "画图---------------------------------------":

            plt.figure(figsize=(40, 20), dpi=120)
            plt.rcParams['font.family'] = 'Microsoft YaHei'
            plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']

            plt.title('基金价格历史曲线')
            plt.xlabel('时间')
            plt.ylabel('价格')
            plt.grid(True)

            color = ["m", "g", "b", "k", "c", "y", "r"]
            style = ["-", "--", ":", "-."]
            line_style = []
            for s in style:
                for c in color:
                    line_style.append(c + s)
            shuffle(line_style)  # 随机打乱

            i = 0
            for code in codes:
                plt.plot_date(codes_dict[code]["净值日期"], codes_dict[code]["单位净值"], line_style[i], label=code )
                i = i + 1

            plt.legend(numpoints=1, fontsize=14)
            plt.legend(loc='upper left')
            plt.show()
    if "test" and 0:
        print("Testing FUNCTION figure_fund_price_history() ...")
        all_codes = read_filenames_from_folder("data")
        codes = all_codes[2:5]
        figure_fund_price_history(codes, start_day="1000-01-01", end_day="3000-01-01")  # 重要

if "分析_计算基金涨跌日比率-----------------------------------------------":
    #知识点： pandas、布尔索引、获取列值、文件读取
    def fund_rise_days_ratio(code):
        file = u"data/" + code + '.xlsx'  # 由crawl_fund_price.py 抓取
        if not os.path.exists(file):
            return "not exist"

        df = pd.read_excel(file)
        df = df[df["单位净值"] != ""]  # 删除没有值的行
        all_days = df.shape[0]
        df = df[df["日增长率"] > 0]  # 删除没有值的行
        rise_days = df.shape[0]
        rise_ratio = rise_days / all_days
        # treeview_dataframe_general(df)
        return rise_ratio
    if "test" and 0:
        print("Testing FUNCTION fund_rise_daysnum() ...")
        code = "002939"
        rise_ratio = fund_rise_days_ratio(code)
        print("The rise_ratio of fund %s is %s" % (code, rise_ratio))

if "过滤_近一个月来情况价格------------------------------------------------":
    #知识点： pandas、布尔索引、获取列值、文件读取
    def recent_price(code):
        file = u"data/" + code + '.xlsx'  # 由crawl_fund_price.py 抓取
        if not os.path.exists(file):
            return "not exist"

        df = pd.read_excel(file)
        print(df)
        df = df[ df["单位净值"] != ""  ]  # 删除没有值的行
        df = df[df["净值日期"] >"2021-11-01"]  # 删除没有值的行
        return df
    if "test" and 0:
        df = recent_price("002939")
        print(df)
        print(df.shape, df.shape[0],df.shape[1])
        treeview_dataframe_general(df)
