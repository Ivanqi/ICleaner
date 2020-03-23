<?php
return [
    19 => [
        't_log_god_body_op' => [
            'comment' => 'god_body_op',
            'fields' => [
                'pid' => [
                    'type' => 'BIGINT',
                    'notes' => 'pid'
                ],
                'agent_id' => [
                    'type' => 'INT',
                    'notes' => '代理ID'
                ],
                'server_id' => [
                    'type' => 'INT',
                    'notes' => '区服ID'
                ],
                'account_name' => [
                    'type' => 'STRING',
                    'notes' => '账号名'
                ],
                'role_id' => [
                    'type' => 'BIGINT',
                    'notes' => '角色ID'
                ],
                'role_level' => [
                    'type' => 'BIGINT',
                    'notes' => '角色登记'
                ],
                'is_internal' => [
                    'type' => 'INT',
                    'notes' => '是否内部号'
                ],
                'platform' => [
                    'type' => 'INT',
                    'notes' => '平台'
                ],
                'via' => [
                    'type' => 'INT',
                    'notes' => 'VIA'
                ],
                'mtime' => [
                    'type' => 'INT',
                    'notes' => '时间戳'
                ],
            ]
        ],
        't_log_barter' => [
            'comment' => '战斗表',
            'fields' => [
                'pid' => [
                    'type' => 'BIGINT',
                    'notes' => '代理ID'
                ],
                'agent_id' => [
                    'type' => 'INT',
                    'notes' => '代理ID'
                ],
                'server_id' => [
                    'type' => 'INT',
                    'notes' => '区服ID'
                ],
                'account_name' => [
                    'type' => 'STRING',
                    'notes' => '账号名'
                ],
                'role_id' => [
                    'type' => 'BIGINT',
                    'notes' => '角色ID'
                ],
                'role_level' => [
                    'type' => 'INT',
                    'notes' => '角色等级'
                ],
                'is_internal' => [
                    'type' => 'INT',
                    'notes' => '是否内部号'
                ],
                'platform' => [
                    'type' => 'INT',
                    'notes' => '平台'
                ],
                'via' => [
                    'type' => 'INT',
                    'notes' => 'VIA' 
                ],
                'mtime' => [
                    'type' => 'INT',
                    'notes' => '时间戳'
                ],
                'op_type' => [
                    'type' => 'INT',
                    'notes' => '操作类型'
                ],
                'barter_id' => [
                    'type' => 'INT',
                    'notes' => '战斗ID'
                ],
                'barter_goods_id' => [
                    'type' => 'INT',
                    'notes' => '战斗商品ID'
                ],
                'request_id' => [
                    'type' => 'INT',
                    'notes' => '请求ID'
                ]
            ]
        ]
    ]
];