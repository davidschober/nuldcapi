from setuptools import setup
# This is the skeleton replace "NAME" with the name of the project
setup(name='nuproxy helpers',
        version='0.1',
        description='put some text here',
        url='put the url of git',
        author='David Schober',
        author_email='davidschob@gmail.com',
        license='MIT',
        packages=['nuproxy'],
        #Require packages if you need them
        install_requires=[
            '',
            ],
        #These are used instead of bin packages
        entry_points = {
            'console_scripts': ['collection_csv=scripts.collection_csv:main'],
            },
        include_package_data=True,
        zip_safe=False)
