## 时间与时间戳的相互转换

对于时间数据，如`2016-05-05 20:28:54`，有时需要与[时间戳](https://so.csdn.net/so/search?q=时间戳)进行相互的运算，此时就需要对两种形式进行转换，在Python中，转换时需要用到`time`模块，具体的操作有如下的几种：

- 将时间转换为时间戳
- 重新格式化时间
- 时间戳转换为时间
- 获取当前时间及将其转换成时间戳

### 1、将时间转换成时间戳
将如上的时间2016-05-05 20:28:54转换成时间戳，具体的操作过程为：

利用strptime()函数将时间转换成时间数组
利用mktime()函数将时间数组转换成时间戳

```python
import time

dt = "2016-05-05 20:28:54"

timeArray=time.strptime(dt,"%Y-%m-%d %H:%M:%S")

timestamp=time.mktime(timeArray)
print(timestamp)
```

### 2、重新格式化时间

重新格式化时间需要以下的两个步骤：

- 利用`strptime()`函数将时间转换成时间数组
- 利用`strftime()`函数重新格式化时间

```python
import time

dt = "2016-05-05 20:28:54"

#转换成时间数组
timeArray = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
#转换成新的时间格式(20160505-20:28:54)
dt_new = time.strftime("%Y年%m月%d日-%H时%M分%S秒",timeArray)
print(dt_new)
```

### 3、将时间戳转换成时间

在时间戳转换成时间中，首先需要将时间戳转换成localtime，再转换成时间的具体格式：

- 利用`localtime()`函数将时间戳转化成localtime的格式
- 利用`strftime()`函数重新格式化时间

```python
import time

timestamp = 1462451334

#转换成localtime
time_local = time.localtime(timestamp)
#转换成新的时间格式(2016-05-05 20:28:54)
dt = time.strftime("%Y-%m-%d %H:%M:%S",time_local)

print(dt)
```

### 4、按指定的格式获取当前时间

利用`time()`获取当前时间，再利用`localtime()`函数转换为localtime，最后利用`strftime()`函数重新格式化时间。

```python
#coding:UTF-8
import time

#获取当前时间
time_now = int(time.time())
#转换成localtime
time_local = time.localtime(time_now)
#转换成新的时间格式(2016-05-09 18:59:20)
dt = time.strftime("%Y-%m-%d %H:%M:%S",time_local)

print(dt)
```

## 如何将毫米级时间戳格式化为毫米级可读格式

```python
import time

# 毫米级时间戳是13位
time_in_ms = 1611208640592
# 转换为东八区时间
timelocal = time.localtime(time_in_ms / 1000.0)
# 将毫米级时间戳格式化位可读格式
formatted_time = time.strftime('%Y-%m-%d %H:%M:%S:{}'.format(time_in_ms % 1000), timelocal)

print(formatted_time)
```