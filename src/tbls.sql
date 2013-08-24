create table tweets (
    inserted timestamp default now(),
    tweet_id bigint not null,
    account_id bigint not null,
    tweet_text VarChar(200) not null,
    created_at timestamp not null,
    retweet_count int not null,
    source Varchar(300),
    original_tweet_id bigint,

    primary key(tweet_id),
    foreign key (account_id) references accounts(account_id),
    index(tweet_id),
    index (tweet_id, account_id)
)

create table stemmed_tweets (
    inserted timestamp default now(),
    tweet_id bigint not null,
    stemmed_text VarChar(200) not null,

    primary key(tweet_id),
    foreign key (tweet_id) references tweets(tweet_id),
    index(tweet_id)
)

create table stemmed_spell_checked_tweets (
    inserted timestamp default now(),
    tweet_id bigint not null,
    stemmed_text VarChar(200) not null,

    primary key(tweet_id),
    foreign key (tweet_id) references tweets(tweet_id),
    index(tweet_id)
)

create table accounts (
    inserted timestamp default now(),
    account_id bigint not null,
    account_name Varchar(300) not null,
    name Varchar(300),
    location VarChar(300),
    followers int not null,
    friends int not null,
    time_zone VarChar(300) not null,
    
    primary key(account_id),
    index (account_id)
)

create table industries (
    industry_id smallint not null auto_increment,
    industry varchar(200) not null,

    unique key (industry_id),
    primary key (industry)
)

create table anchors (
    account_id bigint not null,
    vote Char not null,
    industry_id smallint not null,

    primary key(account_id),
    foreign key(account_id) references accounts(account_id),
    foreign key(industry_id) references industries(industry_id)
)

create table hashtags (
    inserted timestamp default now(),
    tweet_id bigint not null,
    hashtag VarChar(100) not null,

    primary key (tweet_id, hashtag),
    foreign key (tweet_id) references tweets(tweet_id)
)

create table candidate_following (
    inserted timestamp default now(),
    account_id bigint not null,
    following_obama boolean not null,
    following_biden boolean not null,
    following_romney boolean not null,
    following_ryan boolean not null,

    primary key (account_id),
    foreign key (account_id) references accounts(account_id)
)
