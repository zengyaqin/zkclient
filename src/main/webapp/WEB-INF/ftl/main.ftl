<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>zookeeper 登录</title>
    <link href="../css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <h1>Hello, world!</h1>
    <script src="../jquery.min.js"></script>
    <script src="../js/bootstrap.min.js"></script>


    <div class="container">

        <form class="form-signin" role="form">
            <h2 class="form-signin-heading">登录</h2>
            <input type="remote" class="form-control" placeholder="地址" required autofocus>
            <input type="port" class="form-control" placeholder="端口" required>
            <div class="checkbox">
                <label>
                    <input type="checkbox" value="remember-me"> 记住
                </label>
            </div>
            <button class="btn btn-lg btn-primary btn-block" type="submit">登录</button>
        </form>

    </div> <!-- /container -->
</body>
</html>