=======================
Starbelly Python Client
=======================


***********
Development
***********


Pylint
======
To prevent Pylint from raising errors for protobuf generated types, the plugin `pylint-protobuf <https://github.com/nelfin/pylint-protobuf>`_ is included as a
development dependency.

Run pylint with pylint-protobuf plugin:

.. code-block:: bash

    $ pylint --load-plugins=pylint_protobuf client.py

To use the plugin with VSCode, edit `settings.json <https://code.visualstudio.com/docs/getstarted/settings#_settings-file-locations>`_:

.. code-block:: json

   {
       "python.pythonPath": "venv/bin/python",
       "python.linkting.pylintEnabled": true,
       "python.linting.pylintArgs": ["--load-plugins", "pylint_protobuf", "--disable", "C0330" ],
       "python.testing.unittestArgs": [
           "-v",
           "-s",
           "./tests",
           "-p",
           "test_*.py"
       ],
       "python.testing.pytestEnabled": true,
       "python.testing.nosetestsEnabled": false,
       "python.testing.unittestEnabled": false,
       "python.testing.pytestArgs": [
           "tests"
       ],
       "restructuredtext.confPath": "${workspaceFolder}/venv/lib/python3.7/site-packages/importlib_metadata/docs"
   }