import json
import copy
from collections.abc import MutableMapping
from importlib import import_module
from pprint import pformat

from scrapy.settings import default_settings


SETTINGS_PRIORITIES = {
    # 配置的优先级
    'default': 0,
    'command': 10,
    'project': 20,
    'spider': 30,
    'cmdline': 40,
}


def get_settings_priority(priority):
    """
    Small helper function that looks up a given string priority in the
    :attr:`~scrapy.settings.SETTINGS_PRIORITIES` dictionary and returns its
    numerical value, or directly returns a given numerical priority.
    """
    # 返回`SETTINGS_PRIORITIES`中的数字或者本身
    if isinstance(priority, str):
        return SETTINGS_PRIORITIES[priority]
    else:
        return priority


class SettingsAttribute:

    """Class for storing data related to settings attributes.
    用于存储与设置属性相关的数据的类。

    This class is intended for internal usage, you should try Settings class
    for settings configuration, not this one.
    该类仅供内部使用，应尝试使用Settings类进行设置配置，而不是此类。
    """

    def __init__(self, value, priority):
        self.value = value
        if isinstance(self.value, BaseSettings):
            self.priority = max(self.value.maxpriority(), priority)
        else:
            self.priority = priority

    def set(self, value, priority):
        """Sets value if priority is higher or equal than current priority."""
        # 如果优先级高于或等于当前优先级，则设置值。

        if priority >= self.priority:
            if isinstance(self.value, BaseSettings):
                value = BaseSettings(value, priority=priority)
            self.value = value
            self.priority = priority

    def __str__(self):
        # !r 表示用本体展示，不转成字符串
        return f"<SettingsAttribute value={self.value!r} priority={self.priority}>"

    __repr__ = __str__


class BaseSettings(MutableMapping):
    """
    Instances of this class behave like dictionaries, but store priorities
    along with their ``(key, value)`` pairs, and can be frozen (i.e. marked
    immutable).
    此类的实例的行为类似于字典，但是将优先级及其（键，值）对存储在一起，并且可以被冻结（即标记为不可变的）。

    Key-value entries can be passed on initialization with the ``values``
    argument, and they would take the ``priority`` level (unless ``values`` is
    already an instance of :class:`~scrapy.settings.BaseSettings`, in which
    case the existing priority levels will be kept).  If the ``priority``
    argument is a string, the priority name will be looked up in
    :attr:`~scrapy.settings.SETTINGS_PRIORITIES`. Otherwise, a specific integer
    should be provided.
    可以在初始化时使用``values''参数传递键值条目，
    键值条目将采用``priority''级别（除非``values''已经是：class：`〜scrapy.settings的实例）。
    BaseSettings`，在这种情况下，将保留现有优先级）。
    如果``priority''参数是一个字符串，则优先级名称将在：attr：`〜scrapy.settings.SETTINGS_PRIORITIES`中查找。否则，应提供一个特定的整数。

    Once the object is created, new settings can be loaded or updated with the
    :meth:`~scrapy.settings.BaseSettings.set` method, and can be accessed with
    the square bracket notation of dictionaries, or with the
    :meth:`~scrapy.settings.BaseSettings.get` method of the instance and its
    value conversion variants. When requesting a stored key, the value with the
    highest priority will be retrieved.
    创建对象后，可以使用：meth：`〜scrapy.settings.BaseSettings.set`方法加载或更新新设置，
    并可以使用方括号符号或字典来访问新设置。实例的scrapy.settings.BaseSettings.get`方法及其值转换变体。
    请求存储的密钥时，将检索优先级最高的值。
    """

    def __init__(self, values=None, priority='project'):
        # 用于set的时候，不可变的判断
        self.frozen = False
        self.attributes = {}
        if values:
            self.update(values, priority)

    def __getitem__(self, opt_name):
        if opt_name not in self:
            return None
        return self.attributes[opt_name].value

    def __contains__(self, name):
        return name in self.attributes

    def get(self, name, default=None):
        """
        Get a setting value without affecting its original type.
        在不影响其原始类型的情况下获取设置值。

        :param name: the setting name
        :type name: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        return self[name] if self[name] is not None else default

    def getbool(self, name, default=False):
        """
        Get a setting value as a boolean.

        ``1``, ``'1'``, `True`` and ``'True'`` return ``True``,
        while ``0``, ``'0'``, ``False``, ``'False'`` and ``None`` return ``False``.

        For example, settings populated through environment variables set to
        ``'0'`` will return ``False`` when using this method.

        :param name: the setting name
        :type name: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        got = self.get(name, default)
        try:
            return bool(int(got))
        except ValueError:
            if got in ("True", "true"):
                return True
            if got in ("False", "false"):
                return False
            raise ValueError("Supported values for boolean settings "
                             "are 0/1, True/False, '0'/'1', "
                             "'True'/'False' and 'true'/'false'")

    def getint(self, name, default=0):
        """
        Get a setting value as an int.

        :param name: the setting name
        :type name: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        return int(self.get(name, default))

    def getfloat(self, name, default=0.0):
        """
        Get a setting value as a float.

        :param name: the setting name
        :type name: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        return float(self.get(name, default))

    def getlist(self, name, default=None):
        """
        Get a setting value as a list. If the setting original type is a list, a
        copy of it will be returned. If it's a string it will be split by ",".

        For example, settings populated through environment variables set to
        ``'one,two'`` will return a list ['one', 'two'] when using this method.

        :param name: the setting name
        :type name: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        value = self.get(name, default or [])
        if isinstance(value, str):
            value = value.split(',')
        return list(value)

    def getdict(self, name, default=None):
        """
        Get a setting value as a dictionary. If the setting original type is a
        dictionary, a copy of it will be returned. If it is a string it will be
        evaluated as a JSON dictionary. In the case that it is a
        :class:`~scrapy.settings.BaseSettings` instance itself, it will be
        converted to a dictionary, containing all its current settings values
        as they would be returned by :meth:`~scrapy.settings.BaseSettings.get`,
        and losing all information about priority and mutability.

        :param name: the setting name
        :type name: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        value = self.get(name, default or {})
        if isinstance(value, str):
            value = json.loads(value)
        return dict(value)

    def getwithbase(self, name):
        """Get a composition of a dictionary-like setting and its `_BASE`
        counterpart.
        获取类似字典的设置及其对应的_BASE的组成。

        :param name: name of the dictionary-like setting
        :type name: str
        """
        compbs = BaseSettings()
        compbs.update(self[name + '_BASE'])
        compbs.update(self[name])
        return compbs

    def getpriority(self, name):
        """
        Return the current numerical priority value of a setting, or ``None`` if
        the given ``name`` does not exist.

        :param name: the setting name
        :type name: str
        """
        if name not in self:
            return None
        return self.attributes[name].priority

    def maxpriority(self):
        """
        Return the numerical value of the highest priority present throughout
        all settings, or the numerical value for ``default`` from
        :attr:`~scrapy.settings.SETTINGS_PRIORITIES` if there are no settings
        stored.
        """
        if len(self) > 0:
            return max(self.getpriority(name) for name in self)
        else:
            return get_settings_priority('default')

    def __setitem__(self, name, value):
        self.set(name, value)

    def set(self, name, value, priority='project'):
        """
        Store a key/value attribute with a given priority.

        Settings should be populated *before* configuring the Crawler object
        (through the :meth:`~scrapy.crawler.Crawler.configure` method),
        otherwise they won't have any effect.

        :param name: the setting name
        :type name: str

        :param value: the value to associate with the setting
        :type value: object

        :param priority: the priority of the setting. Should be a key of
            :attr:`~scrapy.settings.SETTINGS_PRIORITIES` or an integer
        :type priority: str or int
        """
        # 判断是否可变
        self._assert_mutability()
        # 获取优先级数字
        priority = get_settings_priority(priority)
        if name not in self:
            if isinstance(value, SettingsAttribute):
                self.attributes[name] = value
            else:
                self.attributes[name] = SettingsAttribute(value, priority)
        else:
            self.attributes[name].set(value, priority)

    def setdict(self, values, priority='project'):
        self.update(values, priority)

    def setmodule(self, module, priority='project'):
        """
        Store settings from a module with a given priority.
        以给定的优先级存储模块中的设置。

        This is a helper function that calls
        :meth:`~scrapy.settings.BaseSettings.set` for every globally declared
        uppercase variable of ``module`` with the provided ``priority``.

        :param module: the module or the path of the module
        :type module: types.ModuleType or str

        :param priority: the priority of the settings. Should be a key of
            :attr:`~scrapy.settings.SETTINGS_PRIORITIES` or an integer
        :type priority: str or int
        """
        self._assert_mutability()
        if isinstance(module, str):
            module = import_module(module)
        for key in dir(module):
            if key.isupper():
                self.set(key, getattr(module, key), priority)

    def update(self, values, priority='project'):
        """
        Store key/value pairs with a given priority.

        This is a helper function that calls
        :meth:`~scrapy.settings.BaseSettings.set` for every item of ``values``
        with the provided ``priority``.

        If ``values`` is a string, it is assumed to be JSON-encoded and parsed
        into a dict with ``json.loads()`` first. If it is a
        :class:`~scrapy.settings.BaseSettings` instance, the per-key priorities
        will be used and the ``priority`` parameter ignored. This allows
        inserting/updating settings with different priorities with a single
        command.

        :param values: the settings names and values
        :type values: dict or string or :class:`~scrapy.settings.BaseSettings`

        :param priority: the priority of the settings. Should be a key of
            :attr:`~scrapy.settings.SETTINGS_PRIORITIES` or an integer
        :type priority: str or int
        """
        self._assert_mutability()
        if isinstance(values, str):
            values = json.loads(values)
        if values is not None:
            if isinstance(values, BaseSettings):
                for name, value in values.items():
                    self.set(name, value, values.getpriority(name))
            else:
                for name, value in values.items():
                    self.set(name, value, priority)

    def delete(self, name, priority='project'):
        self._assert_mutability()
        priority = get_settings_priority(priority)
        if priority >= self.getpriority(name):
            del self.attributes[name]

    def __delitem__(self, name):
        self._assert_mutability()
        del self.attributes[name]

    def _assert_mutability(self):
        if self.frozen:
            raise TypeError("Trying to modify an immutable Settings object")

    def copy(self):
        """
        Make a deep copy of current settings.

        This method returns a new instance of the :class:`Settings` class,
        populated with the same values and their priorities.

        Modifications to the new object won't be reflected on the original
        settings.
        """
        return copy.deepcopy(self)

    def freeze(self):
        """
        Disable further changes to the current settings.

        After calling this method, the present state of the settings will become
        immutable. Trying to change values through the :meth:`~set` method and
        its variants won't be possible and will be alerted.
        """
        self.frozen = True

    def frozencopy(self):
        """
        Return an immutable copy of the current settings.

        Alias for a :meth:`~freeze` call in the object returned by :meth:`copy`.
        """
        copy = self.copy()
        copy.freeze()
        return copy

    def __iter__(self):
        return iter(self.attributes)

    def __len__(self):
        return len(self.attributes)

    def _to_dict(self):
        return {k: (v._to_dict() if isinstance(v, BaseSettings) else v)
                for k, v in self.items()}

    def copy_to_dict(self):
        """
        Make a copy of current settings and convert to a dict.

        This method returns a new dict populated with the same values
        and their priorities as the current settings.

        Modifications to the returned dict won't be reflected on the original
        settings.

        This method can be useful for example for printing settings
        in Scrapy shell.
        """
        settings = self.copy()
        return settings._to_dict()

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text(repr(self))
        else:
            p.text(pformat(self.copy_to_dict()))


class _DictProxy(MutableMapping):

    def __init__(self, settings, priority):
        self.o = {}
        self.settings = settings
        self.priority = priority

    def __len__(self):
        return len(self.o)

    def __getitem__(self, k):
        return self.o[k]

    def __setitem__(self, k, v):
        self.settings.set(k, v, priority=self.priority)
        self.o[k] = v

    def __delitem__(self, k):
        del self.o[k]

    def __iter__(self, k, v):
        return iter(self.o)


class Settings(BaseSettings):
    """
    This object stores Scrapy settings for the configuration of internal
    components, and can be used for any further customization.
    该对象存储用于内部组件配置的Scrapy设置，并可用于任何进一步的自定义。

    It is a direct subclass and supports all methods of
    :class:`~scrapy.settings.BaseSettings`. Additionally, after instantiation
    of this class, the new object will have the global default settings
    described on :ref:`topics-settings-ref` already populated.
    它是直接的子类，并支持：class：`〜scrapy.settings.BaseSettings`的所有方法。
    另外，在实例化此类之后，新对象将具有已经填充在：ref：`topics-settings-ref`上描述的全局默认设置。
    """

    def __init__(self, values=None, priority='project'):
        # Do not pass kwarg values here. We don't want to promote user-defined
        # dicts, and we want to update, not replace, default dicts with the
        # values given by the user
        super().__init__()
        self.setmodule(default_settings, 'default')
        # Promote default dictionaries to BaseSettings instances for per-key
        # priorities
        for name, val in self.items():
            if isinstance(val, dict):
                self.set(name, BaseSettings(val, 'default'), 'default')
        self.update(values, priority)


def iter_default_settings():
    """Return the default settings as an iterator of (name, value) tuples"""
    # 返回默认设置，作为（名称，值）元组的迭代器
    # <下面这个函数用到>
    for name in dir(default_settings):
        if name.isupper():  # 前提必须是大写的，因为前面使用dir
            yield name, getattr(default_settings, name)


def overridden_settings(settings):
    """Return a dict of the settings that have been overridden"""
    # 返回已被覆盖的设置的字典
    # <只是在最开始的时候，日志打印一下要用>
    for name, defvalue in iter_default_settings():
        # 比较一下，现在使用的setting和default_setting之间的区别
        value = settings[name]
        if not isinstance(defvalue, dict) and value != defvalue:
            yield name, value
