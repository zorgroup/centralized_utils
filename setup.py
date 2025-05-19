from setuptools import setup, find_packages

setup(
    name="centralized_utils",
    version="0.1.0",
    description="Centralized utility package: logging, S3 upload, sanitization, etc.",
    author="Your Name",
    author_email="you@example.com",
    packages=find_packages(),
    install_requires=[
        # e.g. 'boto3>=1.0.0'
    ],
    python_requires='>=3.7',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
