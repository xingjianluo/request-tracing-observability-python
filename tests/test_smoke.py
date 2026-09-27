"""冒烟测试：骨架可导入、可构造应用。"""


def test_app_importable():
    from main import app

    assert app is not None
