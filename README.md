本项目来自百度音乐数据部门@evan，不提供相关日志，转载声明出处！！！
# Vip潜在客户模型
## 解析日志
选取订单日志，播放日志，曲库信息，用户浏览日志
1.PayOrderParse为解析订单日志
输出格式
以king榜为例

new Text(baiduid),new Text(product,baiduid,start_time.deliver_time,pay_time,money,source,"king",prepay_type,is_succ,event_terminal_type,
list_money,good_type.pay_type,pay_status,deliver_status,refund_time)
```
deliver_time 发货完成时间
refund_time 退款时间
source 订单来源
pay_type 付款类型

```
2.UserPlayCollection解析播放日志，并添加用户身份信息
首先解析四端的播放日志
输出格式为

new Text(device_type,baiduid,song_id),new Text(logDate,singer_id,p100ms,p60s,pend,time,favor,ref,rate,event_userid)
```
time为播放时间
p100ms,p60s,pend为播放行为
rate为码率
favor为收藏

```
将上面的订单输出，作为缓存文件，与播放日志进行整合
```
job.addCacheFile(new URI("/xxxxx/PayOrderParse/part-r-00000#tip.orders"));
```
输出格式为
new Text(device_type,baiduid,song_id),new Text(播放信息的json，订单信息)
```
播放信息的json: play_json
{logDate:logdate+singer_id,p100ms,p60s,pend,time,favor,ref,rate,event_userid}
订单信息:order_info
如果播放信息中的id与订单里面匹配
new Text(product,baiduid,start_time.deliver_time,pay_time,money,source,"king",prepay_type,is_succ,event_terminal_type,
list_money,good_type.pay_type,pay_status,deliver_status,refund_time)
如果没有匹配到id
noorder+|id
```
3.UserAddSongInfo播放记录关联曲库信息
首先取播放量前1000首歌的song_id 输出文件为popularity.song
将popularity.song中的歌曲id与曲库信息做匹配，得到这播放量前1000首歌的歌曲信息，输出文件名为songs.info
格式如下
```
new Text(song_id),new Text(songinfo)
songinfo为
(authorid,style,language,publish_time,category,si_has_filmtv)
```
将songs.info和播放记录和曲库信息在进行关联
输出格式

```
map输出
1.曲库信息
new Text(song_id),new Text(quku$+authorid+style+language+publish_time+category+si_has_filmtv)
2.播放日志和播放量前1000首歌的歌曲信息匹配
如果匹配上
new Text(#Done+订单和播放输出product后面的信息+#Done+songinfo),new Text()
例：
new Text(#Done+baiduid+song_id+play_json+order_info+#Done+songinfo),new Text()

reduce输出
new Text(baiduid+song_id+play_json+order_info),new Text(songinfo)

```
4.UserBrowseRecord解析用户的浏览日志
输出value格式 scanjson
```
new Text(baiduid),new Text(date-hour+device_type+product+refer_domain)
例：
FFFF2D24911467517D74E1EF8AC76590
{"20160813-15\tpcweb\tmusic\twww.baidu.com":1}
```
5.UserPCAddBrowse  浏览日志和上面的播放相关联
```
new Text(baiduid+song_id+play_json+order_info+songinfo),new Text(scanjson)
```
未完待续...









