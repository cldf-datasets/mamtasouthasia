from setuptools import setup


setup(
    name='cldfbench_mamtasouthasia',
    py_modules=['cldfbench_mamtasouthasia'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'mamta2=cldfbench_mamtasouthasia:Dataset',
        ]
    },
    install_requires=[
        'cldfbench[glottolog,excel]',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
